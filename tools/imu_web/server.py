#!/usr/bin/env python3
"""IMU recorder: stdlib HTTP/WebSocket API and SQLite WAL storage, behind Nginx.

Two ingestion paths share one storage layer:

* legacy HTTP ``POST /api/ingest`` (one or two hands, batched) - unchanged.
* per-frame WSS messages on ``/api/stream`` - one hand, one frame per message,
  acknowledged only after its database transaction commits.

Pairing is done on the server from the device clock fields
(``session`` + ``scheduled_us`` + ``aligned_us`` + ``sync_valid``), never from
the WebSocket arrival time. The export turns those slots back into the
reference dataset layout (77 columns).
"""
import base64
import csv
from contextlib import contextmanager
import datetime as dt
import hashlib
import hmac
import io
import json
import math
import os
from pathlib import Path
import queue
import re
import sqlite3
import struct
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

DB = os.environ.get('IMU_DB', '/var/lib/glove-imu/imu.sqlite3')
TOKEN = os.environ.get('IMU_TOKEN', '')
ANNOTATION_TOKEN = os.environ.get('IMU_ANNOTATION_TOKEN', '')
AXES = ['ax', 'ay', 'az', 'gx', 'gy', 'gz']
VALUES = [f'ch{ch}_{axis}' for ch in range(11)
          for axis in AXES + (['mx', 'my', 'mz'] if ch == 10 else [])]
META = ['id', 'received_at', 'received_unix_ms', 'device', 'session', 'hand',
        'sequence', 'timestamp_ms', 'source_timestamp_ms', 'device_unix_ms',
        'valid_mask', 'mag_mask']
IDENTIFIER = re.compile(r'^[A-Za-z0-9_-]{1,80}$')
# Devices identify themselves by MAC; allow ':' and '.' so the firmware MAC can
# be used verbatim. Values are always bound as SQL parameters.
DEVICE = re.compile(r'^[A-Za-z0-9_.:-]{1,80}$')
CAL_VALUES = [f'cal_ch{ch}_{axis}' for ch in range(3) for axis in AXES]
SYNC_VALUES = ['source_us', 'aligned_us', 'scheduled_us', 'clock_offset_us',
               'scan_us', 'sync_rtt_us', 'sync_age_ms', 'sync_valid', 'paired', 'pair_delta_us']
PAIR_COLUMNS = ['pair_id', 'pair_state']
DATASET_AXES = ['acc_x', 'acc_y', 'acc_z', 'gyro_x', 'gyro_y', 'gyro_z']
DATASET_VALUES = [f'f_{ch // 2}_s{ch % 2 + 1}_{axis}' for ch in range(10) for axis in DATASET_AXES] + ['dorsal_' + axis for axis in DATASET_AXES + ['mag_x', 'mag_y', 'mag_z']]
DATASET_COLUMNS = ['worker', 'v', 'label', 'active_hand', 'sample_id', 'timestamp/ms', 'frame_id', 'hand_type'] + DATASET_VALUES

# --- pairing configuration (all overridable from the environment) -----------
SLOT_US = int(os.environ.get('IMU_SLOT_US', '20000'))
PAIR_TOLERANCE_US = int(os.environ.get('IMU_PAIR_TOLERANCE_US', '5000'))
PAIR_WAIT_MS = int(os.environ.get('IMU_PAIR_WAIT_MS', '200'))
LATE_PAIR_MS = int(os.environ.get('IMU_LATE_PAIR_MS', '2000'))
WINDOW_FRAMES = int(os.environ.get('IMU_WINDOW_FRAMES', '30'))
INFER_QUEUE_SIZE = int(os.environ.get('IMU_INFER_QUEUE', '64'))

PAIRED_STATES = ('paired', 'late_paired')

# Diagnostics shared with /api/pairing/stats and the tests.
STATS = {'frames_accepted': 0, 'frames_inserted': 0, 'duplicates': 0,
         'pairs_formed': 0, 'pairs_missing': 0, 'pairs_out_of_tolerance': 0,
         'infer_enqueued': 0, 'infer_dropped': 0, 'infer_runs': 0}
STATS_LOCK = threading.Lock()
STOP = threading.Event()


def bump(name, amount=1):
    with STATS_LOCK:
        STATS[name] = STATS.get(name, 0) + amount


@contextmanager
def connect():
    con = sqlite3.connect(DB, timeout=20)
    con.row_factory = sqlite3.Row
    try:
        with con:
            yield con
    finally:
        con.close()


def initialize():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    with connect() as con:
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('CREATE TABLE IF NOT EXISTS frames ('
                    'id INTEGER PRIMARY KEY, received_at TEXT NOT NULL, '
                    'received_unix_ms INTEGER NOT NULL, device TEXT NOT NULL, '
                    'session TEXT NOT NULL, hand TEXT NOT NULL, sequence INTEGER NOT NULL, '
                    'timestamp_ms INTEGER NOT NULL, source_timestamp_ms INTEGER, '
                    'device_unix_ms INTEGER, '
                    'valid_mask INTEGER NOT NULL, mag_mask INTEGER NOT NULL, '
                    + ', '.join(f'{name} INTEGER' for name in VALUES)
                    + ', UNIQUE(device, session, sequence))')
        con.execute('CREATE INDEX IF NOT EXISTS frames_time ON frames(received_unix_ms)')
        con.execute('CREATE INDEX IF NOT EXISTS frames_device ON frames(device,id)')
        con.execute('CREATE INDEX IF NOT EXISTS frames_session ON frames(session,id)')
        con.execute('CREATE TABLE IF NOT EXISTS presence (device TEXT PRIMARY KEY, hand TEXT NOT NULL, received_unix_ms INTEGER NOT NULL, recording INTEGER NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS annotations (session TEXT PRIMARY KEY, worker TEXT NOT NULL, label TEXT NOT NULL, v TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS deleted_sessions (session TEXT PRIMARY KEY, deleted_at TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS samples (sample_id INTEGER PRIMARY KEY AUTOINCREMENT, session TEXT NOT NULL UNIQUE)')
        # Assign legacy recordings in chronological order once; never renumber on deletion.
        missing = con.execute('SELECT session FROM frames WHERE session NOT IN (SELECT session FROM samples) '
                              'GROUP BY session ORDER BY min(received_unix_ms),min(id)').fetchall()
        con.executemany('INSERT INTO samples (session) VALUES (?)', [(r['session'],) for r in missing])
        if 'active_hand' not in {r[1] for r in con.execute('PRAGMA table_info(annotations)')}:
            con.execute("ALTER TABLE annotations ADD COLUMN active_hand TEXT NOT NULL DEFAULT ''")
        existing = {r[1] for r in con.execute('PRAGMA table_info(frames)')}
        if 'source_timestamp_ms' not in existing:
            con.execute('ALTER TABLE frames ADD COLUMN source_timestamp_ms INTEGER')
        for name in ['calibration_version'] + CAL_VALUES:
            if name not in existing:
                con.execute('ALTER TABLE frames ADD COLUMN ' + name + (' TEXT' if name == 'calibration_version' else ' REAL'))
        for name in SYNC_VALUES:
            if name not in existing:
                con.execute('ALTER TABLE frames ADD COLUMN ' + name + ' INTEGER')
        for name in PAIR_COLUMNS:
            if name not in existing:
                con.execute('ALTER TABLE frames ADD COLUMN ' + name + (' INTEGER' if name == 'pair_id' else ' TEXT'))
        con.execute('CREATE INDEX IF NOT EXISTS frames_slot ON frames(session,scheduled_us)')
        # One row per (session, scheduled_us): the pair slot on the shared left timeline.
        con.execute('CREATE TABLE IF NOT EXISTS pairs ('
                    'id INTEGER PRIMARY KEY, session TEXT NOT NULL, scheduled_us INTEGER NOT NULL, '
                    'left_id INTEGER, right_id INTEGER, aligned_us INTEGER, delta_us INTEGER, '
                    'state TEXT NOT NULL, reason TEXT, opened_ms INTEGER NOT NULL, '
                    'resolved_ms INTEGER, UNIQUE(session, scheduled_us))')
        con.execute('CREATE INDEX IF NOT EXISTS pairs_state ON pairs(state,opened_ms)')
        con.execute('CREATE TABLE IF NOT EXISTS inference_runs ('
                    'id INTEGER PRIMARY KEY, session TEXT NOT NULL, start_us INTEGER NOT NULL, '
                    'end_us INTEGER NOT NULL, pairs INTEGER NOT NULL, state TEXT NOT NULL, '
                    'result TEXT, created_at TEXT NOT NULL, UNIQUE(session, start_us))')
    recover_pairs()


def recover_pairs():
    """Rebuild pair slots after a restart.

    Frames are the source of truth: every confirmed frame is already durable, so
    the slot table can be reconstructed from them. Slots whose partner never
    arrived are marked missing instead of staying pending forever.
    """
    rebuilt = 0
    with connect() as con:
        con.execute('BEGIN IMMEDIATE')
        known = {(r['session'], r['scheduled_us']) for r in
                 con.execute('SELECT session,scheduled_us FROM pairs')}
        rows = con.execute(
            'SELECT id,session,hand,scheduled_us,aligned_us,sync_valid,received_unix_ms '
            'FROM frames WHERE scheduled_us IS NOT NULL ORDER BY session,scheduled_us,id').fetchall()
        for row in rows:
            key = (row['session'], row['scheduled_us'])
            if key in known:
                continue
            known.add(key)
            hand = row['hand']
            if not bool(row['sync_valid']):
                # No shared clock means no slot: never fabricate one on recovery.
                con.execute('UPDATE frames SET pair_state=? WHERE id=?', ('sync_invalid', row['id']))
                continue
            con.execute('INSERT OR IGNORE INTO pairs (session,scheduled_us,left_id,right_id,aligned_us,'
                        'delta_us,state,reason,opened_ms) VALUES (?,?,?,?,?,?,?,?,?)',
                        (row['session'], row['scheduled_us'],
                         row['id'] if hand == 'left' else None,
                         row['id'] if hand == 'right' else None,
                         row['aligned_us'], None, 'pending', None,
                         row['received_unix_ms']))
            rebuilt += 1
        # Anything older than the wait window is closed out; a lone hand stays recorded.
        stale = con.execute(
            "SELECT id,session,left_id,right_id FROM pairs WHERE state='pending' AND opened_ms + ? <= ?",
            (PAIR_WAIT_MS, int(time.time() * 1000))).fetchall()
        for slot in stale:
            state = 'missing_left' if slot['right_id'] else 'missing_right'
            con.execute('UPDATE pairs SET state=?,reason=?,resolved_ms=? WHERE id=?',
                        (state, 'restart_timeout', int(time.time() * 1000), slot['id']))
            # Only frames still claiming 'pending' move with the slot; skipped
            # second frames in the same slot keep their own state.
            con.execute('UPDATE frames SET pair_id=?,pair_state=? WHERE pair_id=? AND pair_state=?',
                        (slot['id'], state, slot['id'], 'pending'))
    return rebuilt


def integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('Invalid integer or range')
    return value


def optional_integer(value, low, high):
    return None if value is None else integer(value, low, high)


def build_rows(device, session, hand, frames, calibrated, version,
               source_timestamps, now, received_ms, sync_quality=None):
    if not isinstance(frames, list) or not 1 <= len(frames) <= 100:
        raise ValueError('Expected 1 to 100 frames')
    if calibrated is not None:
        if not isinstance(version, str) or not IDENTIFIER.fullmatch(version):
            raise ValueError('Invalid calibration version')
        if not isinstance(calibrated, list) or len(calibrated) != len(frames):
            raise ValueError('Calibration batch length mismatch')
    if source_timestamps is not None:
        if not isinstance(source_timestamps, list) or len(source_timestamps) != len(frames):
            raise ValueError('Source timestamp batch length mismatch')
    if sync_quality is not None and (not isinstance(sync_quality, list) or len(sync_quality) != len(frames)):
        raise ValueError('Sync quality batch length mismatch')

    rows = []
    for index, frame in enumerate(frames):
        if not isinstance(frame, list) or len(frame) != 74:
            raise ValueError('Expected 74 values per frame')
        timestamp = integer(frame[0], 0, 2**63 - 1)
        seq = integer(frame[1], 0, 2**32 - 1)
        valid = integer(frame[2], 0, 2047)
        mag = integer(frame[3], 0, 2047)
        epoch = frame[4]
        if epoch is not None:
            integer(epoch, 1577836800000, 7258118400000)
        source = None
        if source_timestamps is not None:
            source = integer(source_timestamps[index], 0, 2**63 - 1)
        raw = [integer(x, -32768, 32767) for x in frame[5:]]
        clean = []
        offset = 0
        for ch in range(11):
            for axis in AXES + (['mx', 'my', 'mz'] if ch == 10 else []):
                present = valid & (1 << ch) and (axis[0] != 'm' or mag & (1 << ch))
                clean.append(raw[offset] if present else None)
                offset += 1
        physical = [None] * 18
        if calibrated is not None:
            physical = calibrated[index]
            if not isinstance(physical, list) or len(physical) != 18:
                raise ValueError('Expected 18 calibrated axes')
            for i, value in enumerate(physical):
                if value is not None and (type(value) not in (int, float) or not math.isfinite(value) or abs(value) > 10000):
                    raise ValueError('Invalid calibrated value')
                if not valid & (1 << (i // 6)) and value is not None:
                    raise ValueError('Invalid channel has calibrated data')
        quality = [None] * len(SYNC_VALUES)
        if sync_quality is not None:
            quality = sync_quality[index]
            if not isinstance(quality, list) or len(quality) != len(SYNC_VALUES):
                raise ValueError('Expected 10 sync quality values')
            for i, value in enumerate(quality):
                integer(value, -2**62, 2**62)
                if i in (0, 1, 4, 5, 6) and value < 0:
                    raise ValueError('Negative sync metric')
                if i in (7, 8) and value not in (0, 1):
                    raise ValueError('Invalid sync flag')
            if quality[8] and not quality[7]:
                raise ValueError('Paired frame must have valid synchronization')
        rows.append([now.isoformat(timespec='milliseconds'), received_ms, device,
                     session, hand, seq, timestamp, source, epoch, valid, mag]
                    + clean + [version if calibrated is not None else None]
                    + physical + quality)
    return rows


# ---------------------------------------------------------------------------
# Per-frame WebSocket ingestion
# ---------------------------------------------------------------------------

def normalize_channels(payload, valid, mag):
    """Return the 69 raw register values for one frame.

    Accepts ``channels`` (11 lists: 6 axes for CH0-CH9, 9 for CH10) or ``values``
    (flat 69). Missing axes become NULL unless the validity masks say otherwise.
    A channel flagged invalid can never contribute a value, so a device cannot
    smuggle an all-zero reading in as a real sample.
    """
    channels = payload.get('channels')
    flat = payload.get('values')
    if channels is not None:
        if not isinstance(channels, list) or len(channels) != 11:
            raise ValueError('Expected 11 channel entries')
        raw = []
        for ch, entry in enumerate(channels):
            width = 9 if ch == 10 else 6
            if entry is None:
                raw.extend([None] * width)
                continue
            if not isinstance(entry, list) or len(entry) != width:
                raise ValueError(f'Expected {width} axes on channel {ch}')
            for value in entry:
                raw.append(None if value is None else integer(value, -32768, 32767))
    elif flat is not None:
        if not isinstance(flat, list) or len(flat) != 69:
            raise ValueError('Expected 69 raw values')
        raw = [None if value is None else integer(value, -32768, 32767) for value in flat]
    else:
        raise ValueError('Missing frame values')

    clean = []
    offset = 0
    for ch in range(11):
        for axis in AXES + (['mx', 'my', 'mz'] if ch == 10 else []):
            present = valid & (1 << ch) and (axis[0] != 'm' or mag & (1 << ch))
            value = raw[offset]
            clean.append(value if present and value is not None else None)
            offset += 1
    return clean


def validate_frame_request(request):
    """Validate one single-hand frame message and return its storage fields."""
    device, hand, session = request.get('device'), request.get('hand'), request.get('session')
    if not isinstance(device, str) or not DEVICE.fullmatch(device):
        raise ValueError('Invalid device')
    if hand not in ('left', 'right'):
        raise ValueError('Invalid hand')
    if not isinstance(session, str) or not IDENTIFIER.fullmatch(session):
        raise ValueError('Invalid session')
    seq = integer(request.get('sequence'), 0, 2 ** 32 - 1)
    valid = integer(request.get('valid_mask', 2047), 0, 2047)
    mag = integer(request.get('mag_mask', 0), 0, 2047)
    timestamp = integer(request.get('timestamp', request.get('timestamp_ms')), 0, 2 ** 63 - 1)
    epoch = request.get('device_unix_ms')
    if epoch is not None:
        integer(epoch, 1577836800000, 7258118400000)
    source_us = optional_integer(request.get('source_us'), 0, 2 ** 62)
    aligned_us = optional_integer(request.get('aligned_us'), 0, 2 ** 62)
    scheduled_us = optional_integer(request.get('scheduled_us'), 0, 2 ** 62)
    clock_offset = request.get('clock_offset_us')
    clock_offset = None if clock_offset is None else integer(clock_offset, -2 ** 62, 2 ** 62)
    scan_us = optional_integer(request.get('scan_us'), 0, 2 ** 32)
    rtt = optional_integer(request.get('sync_rtt_us'), 0, 2 ** 32)
    age = optional_integer(request.get('sync_age_ms'), 0, 2 ** 32)
    sync_valid = request.get('sync_valid', False)
    if not isinstance(sync_valid, bool):
        raise ValueError('Invalid sync_valid')
    if sync_valid and (aligned_us is None or scheduled_us is None):
        raise ValueError('Synchronized frame needs aligned_us and scheduled_us')
    values = normalize_channels(request, valid, mag)
    return {'device': device, 'hand': hand, 'session': session, 'sequence': seq,
            'timestamp_ms': timestamp, 'source_us': source_us,
            'aligned_us': aligned_us, 'scheduled_us': scheduled_us,
            'clock_offset_us': clock_offset, 'scan_us': scan_us,
            'sync_rtt_us': rtt, 'sync_age_ms': age, 'sync_valid': int(sync_valid),
            'device_unix_ms': epoch, 'valid_mask': valid, 'mag_mask': mag,
            'values': values}


FRAME_COLUMNS = ['received_at', 'received_unix_ms', 'device', 'session', 'hand',
                 'sequence', 'timestamp_ms', 'source_timestamp_ms', 'device_unix_ms',
                 'valid_mask', 'mag_mask', 'calibration_version'] + \
                [f'cal_ch{ch}_{axis}' for ch in range(3) for axis in AXES] + \
                VALUES + SYNC_VALUES


def frame_row(fields, now, received_ms):
    row = {
        'received_at': now.isoformat(timespec='milliseconds'),
        'received_unix_ms': received_ms,
        'device': fields['device'], 'session': fields['session'],
        'hand': fields['hand'], 'sequence': fields['sequence'],
        'timestamp_ms': fields['timestamp_ms'],
        'source_timestamp_ms': fields['source_us'],
        'device_unix_ms': fields['device_unix_ms'],
        'valid_mask': fields['valid_mask'], 'mag_mask': fields['mag_mask'],
        'calibration_version': None,
        'source_us': fields['source_us'], 'aligned_us': fields['aligned_us'],
        'scheduled_us': fields['scheduled_us'],
        'clock_offset_us': fields['clock_offset_us'], 'scan_us': fields['scan_us'],
        'sync_rtt_us': fields['sync_rtt_us'], 'sync_age_ms': fields['sync_age_ms'],
        'sync_valid': fields['sync_valid'], 'paired': 0, 'pair_delta_us': None,
    }
    for name, value in zip(VALUES, fields['values']):
        row[name] = value
    for name in ('cal_ch%d_%s' % (ch, axis) for ch in range(3) for axis in AXES):
        row[name] = None
    return row


def pair_slot(con, session, hand, frame_id, aligned_us, sync_valid, scheduled_us, opened_ms):
    """Attach one frame to its (session, scheduled_us) pair slot.

    Returns the new slot state: ``paired``, ``late_paired``, ``pending``,
    ``sync_invalid``, ``out_of_tolerance`` or ``slot_taken``. Pairing never uses
    ``sequence`` or arrival order, only the shared timeline and sync quality.
    """
    if not sync_valid:
        # Saved and tagged, but never offered to a slot: without a valid shared
        # clock the frame cannot honestly be placed on the left timeline.
        con.execute('UPDATE frames SET pair_state=? WHERE id=?', ('sync_invalid', frame_id))
        return 'sync_invalid', None
    if scheduled_us is None:
        return 'no_slot', None

    slot = con.execute('SELECT * FROM pairs WHERE session=? AND scheduled_us=?',
                       (session, scheduled_us)).fetchone()
    if slot is None:
        cursor = con.execute(
            'INSERT INTO pairs (session,scheduled_us,left_id,right_id,aligned_us,state,opened_ms) '
            'VALUES (?,?,?,?,?,?,?)',
            (session, scheduled_us, frame_id if hand == 'left' else None,
             frame_id if hand == 'right' else None, aligned_us, 'pending', opened_ms))
        con.execute('UPDATE frames SET pair_id=?,pair_state=? WHERE id=?',
                    (cursor.lastrowid, 'pending', frame_id))
        return 'pending', cursor.lastrowid

    # Same-hand field for this frame, and the opposite hand's field.
    same_hand_id = slot['right_id'] if hand == 'right' else slot['left_id']
    if same_hand_id:
        # A second frame from the same hand in one slot: keep it, but never pair it.
        con.execute('UPDATE frames SET pair_id=?,pair_state=? WHERE id=?',
                    (slot['id'], 'slot_taken', frame_id))
        return 'slot_taken', slot['id']

    existing_partner = slot['left_id'] if hand == 'right' else slot['right_id']
    delta = None
    partner = con.execute('SELECT aligned_us,sync_valid FROM frames WHERE id=?',
                          (existing_partner,)).fetchone() if existing_partner else None
    if partner is not None and partner['aligned_us'] is not None and aligned_us is not None:
        delta = abs(aligned_us - partner['aligned_us'])

    if partner is not None and partner['sync_valid']:
        if delta is not None and delta <= PAIR_TOLERANCE_US:
            state = 'paired' if slot['state'] not in ('missing_left', 'missing_right') else 'late_paired'
            con.execute('UPDATE pairs SET left_id=?,right_id=?,aligned_us=?,delta_us=?,state=?,'
                        'reason=NULL,resolved_ms=? WHERE id=?',
                        (slot['left_id'] if hand == 'right' else frame_id,
                         slot['right_id'] if hand == 'left' else frame_id,
                         aligned_us if hand == 'left' else partner['aligned_us'],
                         delta, state, opened_ms, slot['id']))
            con.execute('UPDATE frames SET pair_id=?,pair_state=?,paired=1,pair_delta_us=? WHERE id IN (?,?)',
                        (slot['id'], state, delta, existing_partner, frame_id))
            bump('pairs_formed')
            return state, slot['id']
        con.execute('UPDATE pairs SET left_id=?,right_id=?,aligned_us=?,delta_us=?,state=?,reason=? WHERE id=?',
                    (slot['left_id'] if hand == 'right' else frame_id,
                     slot['right_id'] if hand == 'left' else frame_id,
                     aligned_us if hand == 'left' else partner['aligned_us'],
                     delta, 'out_of_tolerance', 'aligned_us delta exceeds tolerance', slot['id']))
        con.execute('UPDATE frames SET pair_id=?,pair_state=? WHERE id=?',
                    (slot['id'], 'out_of_tolerance', frame_id))
        bump('pairs_out_of_tolerance')
        return 'out_of_tolerance', slot['id']

    # No usable partner yet: fill the empty side.
    con.execute('UPDATE pairs SET left_id=?,right_id=?,aligned_us=COALESCE(aligned_us,?),state=? WHERE id=?',
                (slot['left_id'] if hand == 'right' else frame_id,
                 slot['right_id'] if hand == 'left' else frame_id,
                 aligned_us, 'pending', slot['id']))
    con.execute('UPDATE frames SET pair_id=?,pair_state=? WHERE id=?',
                (slot['id'], 'pending', frame_id))
    return 'pending', slot['id']


def resolve_stale_pairs(now_ms=None):
    """Close slots whose partner never arrived, so later frames never wait on them."""
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    closed = []
    with connect() as con:
        con.execute('BEGIN IMMEDIATE')
        stale = con.execute('SELECT id,session,left_id,right_id FROM pairs '
                            "WHERE state='pending' AND opened_ms + ? <= ?",
                            (PAIR_WAIT_MS, now)).fetchall()
        for slot in stale:
            state = 'missing_left' if slot['right_id'] else 'missing_right'
            con.execute('UPDATE pairs SET state=?,reason=?,resolved_ms=? WHERE id=?',
                        (state, 'partner_timeout', now, slot['id']))
            con.execute('UPDATE frames SET pair_state=? WHERE pair_id=? AND pair_state=?',
                        (state, slot['id'], 'pending'))
            closed.append({'slot': slot['id'], 'session': slot['session'], 'state': state})
        if closed:
            bump('pairs_missing', len(closed))
    return closed


def ingest_frame(request, received_ms=None):
    """Store one frame, resolve its slot, then acknowledge.

    The ack is built after the ``connect()`` block exits, i.e. only once the
    transaction has committed. A failed commit raises and no ack is sent.
    """
    fields = validate_frame_request(request)
    now = dt.datetime.now(dt.timezone.utc)
    received_ms = int(now.timestamp() * 1000) if received_ms is None else received_ms
    row = frame_row(fields, now, received_ms)
    columns = ['received_at'] + FRAME_COLUMNS[1:]
    ack = {'type': 'frame_ack', 'device': fields['device'], 'hand': fields['hand'],
           'session': fields['session'], 'sequence': fields['sequence']}
    with connect() as con:
        con.execute('BEGIN IMMEDIATE')
        if con.execute('SELECT 1 FROM deleted_sessions WHERE session=?',
                       (fields['session'],)).fetchone():
            raise ValueError('Recording was deleted; frame not accepted')
        if not con.execute('SELECT 1 FROM samples WHERE session=?',
                           (fields['session'],)).fetchone():
            con.execute('INSERT INTO samples (session) VALUES (?)', (fields['session'],))
        before = con.total_changes
        con.execute('INSERT OR IGNORE INTO frames (' + ','.join(columns) + ') VALUES ('
                    + ','.join('?' for _ in columns) + ')',
                    [row[name] for name in columns])
        inserted = con.total_changes - before
        bump('frames_accepted')
        ack['saved'] = True
        if not inserted:
            bump('duplicates')
            existing = con.execute('SELECT pair_state,pair_id FROM frames WHERE device=? AND session=? AND sequence=?',
                                   (fields['device'], fields['session'], fields['sequence'])).fetchone()
            ack.update({'inserted': False, 'duplicate': True,
                        'pair': existing['pair_state'] if existing else None})
            con.execute('INSERT INTO presence VALUES (?,?,?,?) ON CONFLICT(device) DO UPDATE SET '
                        'hand=excluded.hand,received_unix_ms=excluded.received_unix_ms,recording=excluded.recording',
                        (fields['device'], fields['hand'], received_ms, 1))
        else:
            bump('frames_inserted')
            frame_id = con.execute('SELECT id FROM frames WHERE device=? AND session=? AND sequence=?',
                                   (fields['device'], fields['session'], fields['sequence'])).fetchone()['id']
            state, slot_id = pair_slot(con, fields['session'], fields['hand'], frame_id,
                                       fields['aligned_us'], fields['sync_valid'],
                                       fields['scheduled_us'], received_ms)
            ack.update({'inserted': True, 'pair': state, 'pair_id': slot_id})
            con.execute('INSERT INTO presence VALUES (?,?,?,?) ON CONFLICT(device) DO UPDATE SET '
                        'hand=excluded.hand,received_unix_ms=excluded.received_unix_ms,recording=excluded.recording',
                        (fields['device'], fields['hand'], received_ms, 1))
    if ack.get('pair') in PAIRED_STATES:
        enqueue_inference(fields['session'], fields['scheduled_us'])
    return ack


# ---------------------------------------------------------------------------
# Inference hand-off: bounded queue, never on the receive path
# ---------------------------------------------------------------------------

INFER_QUEUE = queue.Queue(maxsize=INFER_QUEUE_SIZE)
INFER_DONE = queue.Queue()


def enqueue_inference(session, scheduled_us):
    """Queue a session for window assembly; never blocks the WebSocket handler."""
    try:
        INFER_QUEUE.put_nowait((session, scheduled_us))
        bump('infer_enqueued')
    except queue.Full:
        bump('infer_dropped')
        # Dropping is explicit and recoverable: the frames are already committed,
        # and the window pass is idempotent, so a later pair re-scans the session.
        sys.stderr.write('infer queue full; session=%s slot=%s not queued\n'
                         % (session, scheduled_us))


def build_windows(session, upto_us=None):
    """Group consecutive paired slots into windows for the recognizer."""
    with connect() as con:
        sql = ("SELECT scheduled_us,left_id,right_id FROM pairs "
               "WHERE session=? AND state IN ('paired','late_paired')")
        args = [session]
        if upto_us is not None:
            sql += ' AND scheduled_us<=?'
            args.append(upto_us)
        slots = con.execute(sql + ' ORDER BY scheduled_us', args).fetchall()
    windows, current = [], []
    for slot in slots:
        if current and slot['scheduled_us'] - current[-1]['scheduled_us'] > SLOT_US * 3:
            windows.append(current)
            current = []
        current.append(slot)
    if current:
        windows.append(current)
    return [w[i:i + WINDOW_FRAMES] for w in windows for i in range(0, len(w), WINDOW_FRAMES)
            if len(w[i:i + WINDOW_FRAMES]) == WINDOW_FRAMES]


def infer(window):
    """Recognizer entry point.

    No model is wired in yet, so this deliberately reports ``model_not_ready``
    instead of inventing a label. Replace this function to plug the model in.
    """
    return {'status': 'model_not_ready',
            'detail': 'recognizer not connected; frames are paired and stored',
            'frames': len(window)}


def run_windows(session, upto_us=None):
    """Create inference runs for newly completed windows. Idempotent."""
    created = []
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec='milliseconds')
    for window in build_windows(session, upto_us):
        start_us = window[0]['scheduled_us']
        end_us = window[-1]['scheduled_us']
        result = infer(window)
        with connect() as con:
            before = con.total_changes
            con.execute('INSERT OR IGNORE INTO inference_runs (session,start_us,end_us,pairs,state,result,created_at) '
                        'VALUES (?,?,?,?,?,?,?)',
                        (session, start_us, end_us, len(window), result['status'],
                         json.dumps(result, ensure_ascii=False), now))
            if con.total_changes - before:
                bump('infer_runs')
                created.append({'session': session, 'start_us': start_us,
                                'end_us': end_us, 'pairs': len(window), **result})
    for item in created:
        INFER_DONE.put(item)
    return created


def inference_worker():
    while not STOP.is_set():
        try:
            session, _ = INFER_QUEUE.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            run_windows(session)
        except Exception as exc:  # keep the worker alive; frames are already safe
            sys.stderr.write('inference worker error: %r\n' % (exc,))


def pair_janitor():
    while not STOP.is_set():
        try:
            for slot in resolve_stale_pairs():
                enqueue_inference(slot['session'], None)
        except Exception as exc:
            sys.stderr.write('pair janitor error: %r\n' % (exc,))
        STOP.wait(0.25)


# ---------------------------------------------------------------------------
# Legacy batched HTTP ingest
# ---------------------------------------------------------------------------

def ingest(payload):
    if not isinstance(payload, dict):
        raise ValueError('Expected an object')
    now = dt.datetime.now(dt.timezone.utc)
    received_ms = int(now.timestamp() * 1000)

    hands_payload = payload.get('hands')
    if hands_payload is not None:
        # 双手单请求：{"session": "...", "hands": [{...left...}, {...right...}]}
        session = payload.get('session')
        if not isinstance(session, str) or not IDENTIFIER.fullmatch(session):
            raise ValueError('Invalid session')
        if not isinstance(hands_payload, list) or not 1 <= len(hands_payload) <= 2:
            raise ValueError('Expected 1 to 2 hands')
        rows = []
        seen = set()
        for entry in hands_payload:
            if not isinstance(entry, dict):
                raise ValueError('Expected hand object')
            device = entry.get('device')
            hand = entry.get('hand')
            if not isinstance(device, str) or not DEVICE.fullmatch(device):
                raise ValueError('Invalid device')
            if hand not in ('left', 'right') or hand in seen:
                raise ValueError('Invalid hand')
            seen.add(hand)
            rows.extend(build_rows(device, session, hand, entry.get('frames'),
                                   entry.get('calibrated'),
                                   entry.get('calibration_version'),
                                   entry.get('source_timestamps'),
                                   now, received_ms, entry.get('sync_quality')))
        # 同一 sequence 的左右手相邻写入，导出 CSV 可直接按行配对。
        rows.sort(key=lambda row: (row[5], 0 if row[4] == 'left' else 1))
    else:
        # 兼容旧固件/旧回退路径：一次只含一只手。
        device, session, hand = (payload.get(k) for k in ('device', 'session', 'hand'))
        if any(not isinstance(v, str) or not DEVICE.fullmatch(v) for v in (device, session)):
            raise ValueError('Invalid device/session')
        if hand not in ('left', 'right'):
            raise ValueError('Invalid hand')
        rows = build_rows(device, session, hand, payload.get('frames'),
                          payload.get('calibrated'),
                          payload.get('calibration_version'),
                          payload.get('source_timestamps'),
                          now, received_ms, payload.get('sync_quality'))

    columns = META[1:] + VALUES + ['calibration_version'] + CAL_VALUES + SYNC_VALUES
    with connect() as con:
        # Serialize with deletion so an in-flight/retried upload cannot resurrect a record.
        con.execute('BEGIN IMMEDIATE')
        if con.execute('SELECT 1 FROM deleted_sessions WHERE session=?', (session,)).fetchone():
            return {'accepted': len(rows), 'inserted': 0, 'discarded_deleted': True}
        if not con.execute('SELECT 1 FROM samples WHERE session=?', (session,)).fetchone():
            con.execute('INSERT INTO samples (session) VALUES (?)', (session,))
        before = con.total_changes
        con.executemany('INSERT OR IGNORE INTO frames (' + ','.join(columns)
                        + ') VALUES (' + ','.join('?' for _ in columns) + ')', rows)
        inserted = con.total_changes - before
        if inserted:
            # Legacy batches carry sync_quality too, so they get paired as well.
            for row in rows:
                fields = dict(zip(columns, row))
                stored = con.execute('SELECT id FROM frames WHERE device=? AND session=? AND sequence=?',
                                     (fields['device'], fields['session'], fields['sequence'])).fetchone()
                if stored:
                    pair_slot(con, fields['session'], fields['hand'], stored['id'],
                              fields['aligned_us'], fields['sync_valid'],
                              fields['scheduled_us'], received_ms)
    return {'accepted': len(rows), 'inserted': inserted}


def heartbeat(payload):
    if not isinstance(payload, dict):
        raise ValueError('Expected an object')
    device, hand = payload.get('device'), payload.get('hand')
    if not isinstance(device, str) or not DEVICE.fullmatch(device):
        raise ValueError('Invalid device')
    if hand not in ('left', 'right') or type(payload.get('recording')) is not bool:
        raise ValueError('Invalid hand/recording')
    now = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    with connect() as con:
        con.execute('INSERT INTO presence VALUES (?,?,?,?) ON CONFLICT(device) DO UPDATE SET hand=excluded.hand, received_unix_ms=excluded.received_unix_ms, recording=excluded.recording', (device, hand, now, int(payload['recording'])))
    return {'ok': True}


def annotate(payload):
    if not isinstance(payload, dict):
        raise ValueError('Expected object')
    sessions = payload.get('sessions', [payload.get('session')])
    if not isinstance(sessions, list) or not 1 <= len(sessions) <= 500 or any(
            not isinstance(s, str) or not IDENTIFIER.fullmatch(s) for s in sessions):
        raise ValueError('Expected 1 to 500 valid sessions')
    sessions = list(dict.fromkeys(sessions))
    values = []
    for key in ('worker', 'label', 'v'):
        value = payload.get(key)
        if not isinstance(value, str) or not 1 <= len(value.strip()) <= 80 or any(ord(c) < 32 for c in value):
            raise ValueError('Invalid ' + key)
        values.append(value.strip())
    if values[2] not in ('slow', 'medium', 'high'):
        raise ValueError('Invalid speed')
    active_hand = payload.get('active_hand')
    if active_hand not in ('left', 'right', 'both'):
        raise ValueError('Invalid active_hand')
    with connect() as con:
        found = con.execute('SELECT DISTINCT session FROM frames WHERE session IN (' + ','.join('?' for _ in sessions) + ')', sessions).fetchall()
        if len(found) != len(sessions):
            raise ValueError('Recording not found')
        con.executemany('INSERT INTO annotations (session,worker,label,v,active_hand) VALUES (?,?,?,?,?) ON CONFLICT(session) DO UPDATE SET worker=excluded.worker,label=excluded.label,v=excluded.v,active_hand=excluded.active_hand', [[session] + values + [active_hand] for session in sessions])
    return {'ok': True, 'updated': len(sessions)}


def filters(params):
    clauses, args = [], []
    sessions = params.get('session', [])
    if sessions:
        if not 1 <= len(sessions) <= 5000 or any(not IDENTIFIER.fullmatch(s) for s in sessions):
            raise ValueError('Expected 1 to 5000 valid sessions')
        clauses.append('session IN (SELECT value FROM json_each(?))')
        args.append(json.dumps(sessions))
    for key in ('device', 'hand'):
        if params.get(key, [''])[0]:
            clauses.append(key + '=?')
            args.append(params[key][0])
    for key, op in (('start', '>='), ('end', '<=')):
        if params.get(key, [''])[0]:
            clauses.append('received_unix_ms' + op + '?')
            args.append(integer(int(params[key][0]), 0, 2**63 - 1))
    for key in ('worker', 'label', 'v', 'active_hand'):
        if params.get(key, [''])[0]:
            clauses.append('session IN (SELECT session FROM annotations WHERE ' + key + '=?)')
            args.append(params[key][0])
    return (' WHERE ' + ' AND '.join(clauses) if clauses else ''), args


def delete_recordings(payload):
    if not isinstance(payload, dict) or payload.get('confirm') is not True:
        raise ValueError('Explicit deletion confirmation required')
    sessions = payload.get('sessions')
    if not isinstance(sessions, list) or not 1 <= len(sessions) <= 500 or any(
            not isinstance(s, str) or not IDENTIFIER.fullmatch(s) for s in sessions):
        raise ValueError('Expected 1 to 500 valid sessions')
    sessions = list(dict.fromkeys(sessions))
    marks = ','.join('?' for _ in sessions)
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec='milliseconds')
    with connect() as con:
        con.execute('BEGIN IMMEDIATE')
        count = con.execute('SELECT count(DISTINCT session) FROM frames WHERE session IN (' + marks + ')', sessions).fetchone()[0]
        con.executemany('INSERT OR IGNORE INTO deleted_sessions VALUES (?,?)', [(s, now) for s in sessions])
        removed = con.execute('DELETE FROM frames WHERE session IN (' + marks + ')', sessions).rowcount
        con.execute('DELETE FROM annotations WHERE session IN (' + marks + ')', sessions)
        con.execute('DELETE FROM pairs WHERE session IN (' + marks + ')', sessions)
        con.execute('DELETE FROM inference_runs WHERE session IN (' + marks + ')', sessions)
    return {'ok': True, 'deleted_records': count, 'deleted_frames': removed}


def recordings(params):
    """Filter whole sessions by their first server receive time, never split frames."""
    clauses, args = [], []
    start, end = params.get('start', [''])[0], params.get('end', [''])[0]
    if start and end and int(start) > int(end):
        raise ValueError('Start must not be after end')
    for value, op in ((start, '>='), (end, '<=')):
        if value:
            clauses.append('r.started_ms' + op + '?')
            args.append(integer(int(value), 0, 2**63 - 1))
    for key in ('worker', 'label', 'v', 'active_hand'):
        if params.get(key, [''])[0]:
            clauses.append('a.' + key + '=?')
            args.append(params[key][0])
    state = params.get('annotated', [''])[0]
    if state in ('yes', 'no'):
        clauses.append('a.session IS ' + ('NOT NULL' if state == 'yes' else 'NULL'))
    where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
    limit = max(1, min(100, int(params.get('limit', ['50'])[0])))
    offset = max(0, int(params.get('offset', ['0'])[0]))
    order = 'ASC' if params.get('sort', ['desc'])[0] == 'asc' else 'DESC'
    base = '''WITH r AS (SELECT session, min(received_at) started_at,
        max(received_at) ended_at, min(received_unix_ms) started_ms,
        sum(CASE WHEN hand='left' THEN 1 ELSE 0 END) AS left_frames,
        sum(CASE WHEN hand='right' THEN 1 ELSE 0 END) AS right_frames
        FROM frames GROUP BY session) '''
    source = ' FROM r JOIN samples s ON s.session=r.session LEFT JOIN annotations a ON a.session=r.session' + where
    with connect() as con:
        con.execute('BEGIN')
        total = con.execute(base + 'SELECT count(*)' + source, args).fetchone()[0]
        rows = con.execute(base + 'SELECT r.*,s.sample_id,a.worker,a.label,a.v,a.active_hand' + source
                           + ' ORDER BY r.started_ms ' + order + ',r.session ' + order
                           + ' LIMIT ? OFFSET ?', args + [limit, offset]).fetchall()
        pairs = {r['session']: dict(r) for r in con.execute(
            "SELECT session, sum(CASE WHEN state IN ('paired','late_paired') THEN 1 ELSE 0 END) paired, "
            "sum(CASE WHEN state IN ('missing_left','missing_right') THEN 1 ELSE 0 END) missing, "
            "sum(CASE WHEN state='out_of_tolerance' THEN 1 ELSE 0 END) out_of_tolerance, "
            "sum(CASE WHEN state='sync_invalid' THEN 1 ELSE 0 END) sync_invalid "
            "FROM pairs WHERE session IN (SELECT session FROM frames) GROUP BY session")}
    result = []
    for row in rows:
        item = dict(row)
        item.update(pairs.get(row['session'], {}))
        result.append(item)
    return {'records': result, 'total': total, 'limit': limit, 'offset': offset}


def calibrated_counts(row, calibration):
    """Return corrected equivalent counts, without mutating stored raw registers.

    Values may exceed int16 after correction: preserve them rather than clipping.
    Round nearest integer, with halfway cases away from zero.
    """
    output = [row[k] for k in VALUES]
    if row['hand'] != 'left':
        return output
    for channel, fit in calibration['channels'].items():
        ch = int(channel)
        if not 0 <= ch < 10:
            raise ValueError('Unsupported calibration channel')
        for axis in range(6):
            index = ch * 6 + axis
            if output[index] is None:
                continue
            if axis < 3:
                value = (output[index] - fit['accel_bias_raw'][axis]) / fit['accel_sensitivity_counts_per_g'][axis] * 16384
            else:
                value = output[index] - fit['gyro_bias_raw'][axis-3]
            output[index] = math.floor(value + 0.5) if value >= 0 else math.ceil(value - 0.5)
    return output


def dataset_slots(con, where, args):
    """Map each session to its first scheduled slot, for frame_id derivation."""
    origin = {}
    for row in con.execute('SELECT session, min(scheduled_us) AS origin FROM frames'
                           + (where or '') + (' AND ' if where else ' WHERE ')
                           + 'scheduled_us IS NOT NULL GROUP BY session', args):
        origin[row['session']] = row['origin']
    return origin


def dataset_position(row, origin):
    """Frame index and timestamp on the shared timeline.

    ``frame_id`` is the slot index of ``scheduled_us`` relative to the session's
    first slot; ``timestamp/ms`` is the slot time itself. Both come straight from
    the device clock fields, so a dropped slot stays a gap instead of shifting
    later rows. Legacy frames without slots fall back to their own sequence and
    timestamp.
    """
    scheduled = row['scheduled_us']
    base = origin.get(row['session'])
    if scheduled is None or base is None:
        return row['sequence'], row['timestamp_ms']
    return int(round((scheduled - base) / SLOT_US)), int(round(scheduled / 1000))


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def log_message(self, fmt, *args):
        # Never log Authorization headers or request payloads.
        super().log_message(fmt, *args)

    def respond(self, status, body, kind='application/json; charset=utf-8'):
        data = json.dumps(body, ensure_ascii=False).encode() if isinstance(body, (dict, list)) else body
        self.send_response(status)
        self.send_header('Content-Type', kind)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        # Rejected requests may still have unread body bytes on the socket.
        if status >= 400:
            self.close_connection = True
            self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        path = urlsplit(self.path).path
        if path == '/api/export.csv':
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 600000:
                    return self.respond(413, {'error': 'Body too large or empty'})
                self.connection.settimeout(15)
                params = parse_qs(self.rfile.read(size).decode('utf-8'))
                if not params.get('session'):
                    raise ValueError('Select at least one recording')
                where, args = filters(params)
                return self.export(where, args, params)
            except (ValueError, TypeError, UnicodeError) as exc:
                return self.respond(400, {'error': str(exc)})
        if path not in ('/api/ingest', '/api/heartbeat', '/api/annotations', '/api/records/delete'):
            return self.respond(404, {'error': 'Not found'})
        expected = ANNOTATION_TOKEN if path in ('/api/annotations', '/api/records/delete') else TOKEN
        if not expected or not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + expected):
            return self.respond(401, {'error': 'Unauthorized'})
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 120000:
                return self.respond(413, {'error': 'Body too large or empty'})
            self.connection.settimeout(15)
            payload = json.loads(self.rfile.read(size))
            operation = {'/api/annotations': annotate, '/api/records/delete': delete_recordings,
                         '/api/heartbeat': heartbeat, '/api/ingest': ingest}[path]
            self.respond(200, operation(payload))
        except (ValueError, TypeError, UnicodeError) as exc:
            self.respond(400, {'error': str(exc)})
        except sqlite3.Error:
            self.respond(503, {'error': 'Database unavailable; retry batch'})

    def do_GET(self):
        parsed = urlsplit(self.path)
        params = parse_qs(parsed.query)
        if parsed.path == '/api/stream':
            return self.websocket_stream()
        try:
            if parsed.path == '/calibration_coefficients.json':
                return self.respond(200, Path(__file__).with_name('calibration_coefficients.json').read_bytes())
            if parsed.path in ('/', '/index.html', '/app.js', '/records.js', '/style.css', '/calibration_view.js'):
                name = 'index.html' if parsed.path == '/' else parsed.path[1:]
                kind = {'index.html': 'text/html', 'app.js': 'text/javascript', 'records.js': 'text/javascript', 'calibration_view.js': 'text/javascript', 'style.css': 'text/css'}[name]
                return self.respond(200, Path(__file__).with_name(name).read_bytes(), kind + '; charset=utf-8')
            if parsed.path == '/api/health':
                with connect() as con:
                    con.execute('SELECT 1 FROM frames LIMIT 1').fetchone()
                return self.respond(200, {'ok': True})
            if parsed.path == '/api/pairing/stats':
                return self.respond(200, pairing_stats())
            if parsed.path == '/api/pairs':
                return self.respond(200, list_pairs(params))
            where, args = filters(params)
            if parsed.path == '/api/records':
                return self.respond(200, recordings(params))
            if parsed.path == '/api/status':
                with connect() as con:
                    total = con.execute('SELECT count(*) FROM frames').fetchone()[0]
                    latest = con.execute('SELECT * FROM frames WHERE id IN '
                                         '(SELECT max(id) FROM frames GROUP BY device)').fetchall()
                    presence = con.execute('SELECT * FROM presence').fetchall()
                    annotations = {r['session']: dict(r) for r in con.execute('SELECT * FROM annotations')}
                    sessions = con.execute('SELECT device,session,(SELECT sample_id FROM samples s WHERE s.session=frames.session) sample_id,hand,min(received_at) started_at,'
                                           'max(received_at) ended_at,count(*) frames FROM frames '
                                           'GROUP BY device,session ORDER BY max(id) DESC LIMIT 100').fetchall()
                devices = {r['device']: dict(r) for r in latest}
                for row in presence:
                    device = devices.setdefault(row['device'], {'device': row['device'], 'hand': row['hand']})
                    device['heartbeat_unix_ms'] = row['received_unix_ms']
                    device['recording'] = bool(row['recording'])
                return self.respond(200, {'total_frames': total, 'devices': list(devices.values()),
                                          'sessions': [dict(r) for r in sessions],
                                          'annotations': annotations,
                                          'server_unix_ms': int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)})
            if parsed.path == '/api/frames':
                limit = max(1, min(500, int(params.get('limit', ['120'])[0])))
                with connect() as con:
                    rows = con.execute('SELECT * FROM frames' + where + ' ORDER BY id DESC LIMIT ?', args + [limit]).fetchall()
                return self.respond(200, {'frames': [dict(row) for row in reversed(rows)]})
            if parsed.path == '/api/export.csv':
                return self.export(where, args, params)
            self.respond(404, {'error': 'Not found'})
        except (ValueError, TypeError) as exc:
            self.respond(400, {'error': str(exc)})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def websocket_stream(self):
        expected = 'Bearer ' + TOKEN
        if not TOKEN or not hmac.compare_digest(self.headers.get('Authorization', ''), expected):
            return self.respond(401, {'error': 'Unauthorized'})
        key = self.headers.get('Sec-WebSocket-Key', '')
        connection = {part.strip().lower() for part in
                      self.headers.get('Connection', '').split(',')}
        if (self.headers.get('Upgrade', '').lower() != 'websocket' or
                'upgrade' not in connection or
                self.headers.get('Sec-WebSocket-Version') != '13' or
                not key):
            return self.respond(400, {'error': 'Invalid WebSocket upgrade'})
        accept = base64.b64encode(hashlib.sha1(
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode('ascii')
        ).digest()).decode('ascii')
        self.send_response(101, 'Switching Protocols')
        self.send_header('Upgrade', 'websocket')
        self.send_header('Connection', 'Upgrade')
        self.send_header('Sec-WebSocket-Accept', accept)
        self.end_headers()
        self.close_connection = True
        self.connection.settimeout(60)
        try:
            while True:
                opcode, payload = self.read_websocket_frame()
                if opcode == 8:
                    self.write_websocket_frame(8, payload[:125])
                    return
                if opcode == 9:
                    self.write_websocket_frame(10, payload[:125])
                    continue
                if opcode != 1:
                    raise ValueError('Expected a text WebSocket frame')
                try:
                    request = json.loads(payload.decode('utf-8'))
                    response = self.websocket_operation(request)
                except (ValueError, TypeError, UnicodeError) as exc:
                    response = {'type': 'error', 'error': str(exc)}
                except sqlite3.Error:
                    response = {'type': 'error', 'error': 'Database unavailable; retry frame'}
                self.write_websocket_frame(1, json.dumps(
                    response, separators=(',', ':')).encode('utf-8'))
        except (BrokenPipeError, ConnectionError, OSError, ValueError):
            return

    def read_websocket_frame(self):
        header = self.rfile.read(2)
        if len(header) != 2:
            raise ConnectionError('WebSocket closed')
        first, second = header
        if first & 0x70 or not first & 0x80:
            raise ValueError('Fragmented or reserved WebSocket frame')
        opcode = first & 0x0f
        if not second & 0x80:
            raise ValueError('Client WebSocket frames must be masked')
        length = second & 0x7f
        if length == 126:
            raw = self.rfile.read(2)
            if len(raw) != 2:
                raise ConnectionError('Incomplete WebSocket length')
            length = struct.unpack('!H', raw)[0]
        elif length == 127:
            raw = self.rfile.read(8)
            if len(raw) != 8:
                raise ConnectionError('Incomplete WebSocket length')
            length = struct.unpack('!Q', raw)[0]
        if length > 120000:
            raise ValueError('WebSocket message too large')
        mask = self.rfile.read(4)
        payload = self.rfile.read(length)
        if len(mask) != 4 or len(payload) != length:
            raise ConnectionError('Incomplete WebSocket payload')
        return opcode, bytes(value ^ mask[index & 3]
                             for index, value in enumerate(payload))

    def write_websocket_frame(self, opcode, payload):
        length = len(payload)
        if length < 126:
            header = bytes((0x80 | opcode, length))
        elif length <= 0xffff:
            header = bytes((0x80 | opcode, 126)) + struct.pack('!H', length)
        else:
            header = bytes((0x80 | opcode, 127)) + struct.pack('!Q', length)
        self.wfile.write(header + payload)
        self.wfile.flush()

    def websocket_operation(self, request):
        if not isinstance(request, dict):
            raise ValueError('Expected an object')
        kind = request.get('type')
        if kind == 'frame':
            return ingest_frame(request)
        if kind == 'heartbeat':
            return {'type': 'heartbeat_ack', **heartbeat(request)}
        if kind == 'ingest':
            batch_id = request.get('batch_id')
            if not isinstance(batch_id, str) or not IDENTIFIER.fullmatch(batch_id):
                raise ValueError('Invalid batch_id')
            payload = {key: value for key, value in request.items()
                       if key not in ('type', 'batch_id')}
            result = ingest(payload)
            if result.get('discarded_deleted'):
                raise ValueError('Recording was deleted; batch not accepted')
            return {'type': 'ingest_ack', 'batch_id': batch_id, **result}
        raise ValueError('Unknown WebSocket message type')

    def export(self, where, args, params):
        dataset = params.get('format', ['dataset'])[0] == 'dataset'
        counts_mode = params.get('counts', ['raw'])[0]
        if counts_mode not in ('raw', 'calibrated'):
            raise ValueError('Invalid counts mode')
        if counts_mode == 'calibrated' and not dataset:
            raise ValueError('Calibrated counts require dataset format')
        calibration = None
        if counts_mode == 'calibrated':
            calibration = json.loads(Path(__file__).with_name('calibration_coefficients.json').read_text(encoding='utf-8'))
        renumber = params.get('renumber', [''])[0] == '1'
        sample_start = integer(int(params.get('sample_start', ['1'])[0]), 1, 10**12)
        if renumber and not dataset:
            raise ValueError('Renumbering requires dataset format')
        channel = params.get('channel', ['all'])[0]
        if channel == 'all':
            columns = META + VALUES + ['calibration_version'] + CAL_VALUES + SYNC_VALUES + PAIR_COLUMNS
        else:
            ch = integer(int(channel), 0, 10)
            columns = META + [name for name in VALUES if name.startswith(f'ch{ch}_')] + ['calibration_version'] + [name for name in CAL_VALUES if name.startswith(f'cal_ch{ch}_')] + SYNC_VALUES + PAIR_COLUMNS
        self.send_response(200)
        self.send_header('Content-Type', 'text/csv; charset=utf-8')
        suffix = '_calibrated_' + calibration['version'] if calibration else ''
        self.send_header('Content-Disposition', 'attachment; filename="imu_' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S') + suffix + '.csv"')
        self.send_header('Cache-Control', 'no-store')
        # Streaming CSV has no Content-Length; EOF must delimit its body.
        self.close_connection = True
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(b'\xef\xbb\xbf')
        buffer = io.StringIO(newline='')
        writer = csv.writer(buffer)
        def write(row):
            writer.writerow(row)
            self.wfile.write(buffer.getvalue().encode('utf-8'))
            buffer.seek(0)
            buffer.truncate()
        write(DATASET_COLUMNS if dataset else columns + ['worker', 'label', 'v', 'active_hand'])
        # Cursor streams rows without loading a large export into RAM.
        with connect() as con:
            # One SQLite snapshot keeps labels consistent throughout the export.
            con.execute('BEGIN')
            annotations = {r['session']: dict(r) for r in con.execute('SELECT * FROM annotations')}
            sample_ids = {r['session']: r['sample_id'] for r in con.execute('SELECT * FROM samples')}
            origin = dataset_slots(con, where, args) if dataset else {}
            if renumber:
                chronological = con.execute('SELECT session FROM frames' + where
                                            + ' GROUP BY session ORDER BY min(received_unix_ms),session', args).fetchall()
                sample_ids = {r['session']: sample_start + i for i, r in enumerate(chronological)}
            selected = '*' if dataset else ','.join(columns)
            order = ("(SELECT sample_id FROM samples s WHERE s.session=frames.session),"
                     "COALESCE(scheduled_us,timestamp_ms*1000),"
                     "CASE hand WHEN 'left' THEN 0 ELSE 1 END,id") if dataset else 'id'
            if renumber:
                # Rank once per session; avoid a correlated time aggregation per frame.
                order = "CASE session " + ' '.join('WHEN ? THEN ' + str(rank) for rank, session in enumerate(sample_ids)) + " ELSE -1 END,COALESCE(scheduled_us,timestamp_ms*1000),CASE hand WHEN 'left' THEN 0 ELSE 1 END,id" if sample_ids else 'id'
                args = list(args) + list(sample_ids)
            for row in con.execute('SELECT ' + selected + ' FROM frames' + where + ' ORDER BY ' + order, args):
                annotation = annotations.get(row['session'], {})
                if dataset:
                    frame_id, timestamp = dataset_position(row, origin)
                    write([annotation.get(k, '') for k in ('worker', 'v', 'label', 'active_hand')]
                          + [sample_ids[row['session']], timestamp, frame_id, row['hand']]
                          + (calibrated_counts(row, calibration) if calibration else [row[k] for k in VALUES]))
                else:
                    write(tuple(row) + tuple(annotation.get(k, '') for k in ('worker', 'label', 'v', 'active_hand')))


def pairing_stats():
    with STATS_LOCK:
        counters = dict(STATS)
    with connect() as con:
        states = {r['state']: r['n'] for r in con.execute(
            'SELECT state,count(*) n FROM pairs GROUP BY state')}
        runs = {r['state']: r['n'] for r in con.execute(
            'SELECT state,count(*) n FROM inference_runs GROUP BY state')}
    return {'counters': counters, 'pairs': states, 'inference': runs,
            'config': {'slot_us': SLOT_US, 'tolerance_us': PAIR_TOLERANCE_US,
                       'wait_ms': PAIR_WAIT_MS, 'late_pair_ms': LATE_PAIR_MS,
                       'window_frames': WINDOW_FRAMES, 'infer_queue': INFER_QUEUE_SIZE},
            'queue_depth': INFER_QUEUE.qsize()}


def list_pairs(params):
    session = params.get('session', [''])[0]
    if session and not IDENTIFIER.fullmatch(session):
        raise ValueError('Invalid session')
    limit = max(1, min(2000, int(params.get('limit', ['200'])[0])))
    sql = 'SELECT * FROM pairs'
    args = []
    if session:
        sql += ' WHERE session=?'
        args.append(session)
    with connect() as con:
        rows = con.execute(sql + ' ORDER BY id DESC LIMIT ?', args + [limit]).fetchall()
    return {'pairs': [dict(r) for r in rows], 'config': {'tolerance_us': PAIR_TOLERANCE_US,
                                                          'wait_ms': PAIR_WAIT_MS}}


def start_background():
    """Start the inference worker and the pair timeout janitor (idempotent)."""
    if getattr(start_background, 'started', False):
        return
    start_background.started = True
    threading.Thread(target=inference_worker, name='inference', daemon=True).start()
    threading.Thread(target=pair_janitor, name='pair-janitor', daemon=True).start()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == '__main__':
    if not TOKEN:
        raise SystemExit('IMU_TOKEN must be configured')
    initialize()
    start_background()
    print('glove-imu listening on 127.0.0.1:%s (pair tolerance %d us, wait %d ms)'
          % (os.environ.get('IMU_PORT', '8765'), PAIR_TOLERANCE_US, PAIR_WAIT_MS),
          file=sys.stderr)
    Server(('127.0.0.1', int(os.environ.get('IMU_PORT', '8765'))), Handler).serve_forever()
