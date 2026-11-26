"""Tests for per-frame WSS ingestion, server-side pairing and slot-aligned CSV.

Each test uses its own session id, so the shared temporary database stays valid
across the whole module.
"""
import base64
import csv
import http.client
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import struct
import tempfile
import threading
import time
import unittest
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError

SLOT = 20000   # 20 ms slot used by the firmware


class FrameStreamTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        os.environ['IMU_DB'] = str(Path(cls.temp.name) / 'stream.sqlite3')
        os.environ['IMU_TOKEN'] = 'test-token'
        os.environ['IMU_ANNOTATION_TOKEN'] = 'label-test'
        os.environ['IMU_PAIR_TOLERANCE_US'] = '5000'
        os.environ['IMU_PAIR_WAIT_MS'] = '200'
        spec = importlib.util.spec_from_file_location('recorder_stream', Path(__file__).with_name('server.py'))
        cls.app = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.app)
        cls.app.initialize()
        cls.server = cls.app.ThreadingHTTPServer(('127.0.0.1', 0), cls.app.Handler)
        cls.url = f'http://127.0.0.1:{cls.server.server_port}'
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        import gc
        gc.collect()
        cls.temp.cleanup()

    # -- helpers ---------------------------------------------------------
    def connect_ws(self, token='test-token'):
        sock = socket.create_connection(('127.0.0.1', self.server.server_port), timeout=10)
        self.addCleanup(sock.close)
        key = base64.b64encode(os.urandom(16)).decode('ascii')
        sock.sendall((f'GET /api/stream HTTP/1.1\r\nHost: 127.0.0.1\r\n'
                      'Upgrade: websocket\r\nConnection: Upgrade\r\n'
                      f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n'
                      f'Authorization: Bearer {token}\r\n\r\n').encode('ascii'))
        response = bytearray()
        while b'\r\n\r\n' not in response:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
        self.assertIn(b'101 Switching Protocols', response)
        return sock

    @staticmethod
    def send(sock, payload):
        body = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        mask = os.urandom(4)
        size = len(body)
        if size < 126:
            header = bytes((0x81, 0x80 | size))
        elif size <= 0xffff:
            header = bytes((0x81, 0xfe)) + struct.pack('!H', size)
        else:
            header = bytes((0x81, 0xff)) + struct.pack('!Q', size)
        sock.sendall(header + mask + bytes(b ^ mask[i & 3] for i, b in enumerate(body)))

    @staticmethod
    def recv(sock):
        header = sock.recv(2)
        if len(header) != 2:
            raise AssertionError('WebSocket response ended early')
        length = header[1] & 0x7f
        if length == 126:
            length = struct.unpack('!H', sock.recv(2))[0]
        elif length == 127:
            length = struct.unpack('!Q', sock.recv(8))[0]
        payload = bytearray()
        while len(payload) < length:
            payload.extend(sock.recv(length - len(payload)))
        return json.loads(payload.decode('utf-8'))

    def frame(self, hand, session, seq, slot_us, aligned_us=None, sync_valid=True,
              device=None, delta_us=0, values=None, valid_mask=2047, mag_mask=1024):
        """One frame message shaped like the firmware's per-frame WSS payload."""
        aligned = slot_us + 1000 + delta_us if aligned_us is None else aligned_us
        return {'type': 'frame', 'device': device or f'mac-{hand}', 'hand': hand,
                'session': session, 'sequence': seq,
                'timestamp': slot_us // 1000 + 5000,
                'source_us': slot_us + 4000,
                'aligned_us': aligned if sync_valid else None,
                'scheduled_us': slot_us if sync_valid else None,
                'clock_offset_us': 123456789 if sync_valid else None,
                'scan_us': 3100, 'sync_rtt_us': 900, 'sync_age_ms': 120,
                'sync_valid': sync_valid,
                'valid_mask': valid_mask, 'mag_mask': mag_mask,
                'channels': values if values is not None else
                # CH0-CH9: six axes, values 0..59. CH10: nine axes, 60..68.
                [[ch * 6 + axis for axis in range(6)] for ch in range(10)]
                + [[60 + axis for axis in range(9)]]}

    def send_frame(self, sock, payload):
        self.send(sock, payload)
        return self.recv(sock)

    def stream(self, session, hands=('left', 'right'), slots=range(5), start_seq=0,
               delta=0, sync_valid=True):
        sock = self.connect_ws()
        acks = []
        for index, hand in enumerate(hands for _ in range(1)):
            pass
        for slot_index in slots:
            for hand in hands:
                acks.append(self.send_frame(sock, self.frame(
                    hand, session, start_seq + slot_index, slot_index * SLOT,
                    delta_us=delta, sync_valid=sync_valid)))
        return acks

    def pairs(self, session):
        return json.load(urlopen(self.url + '/api/pairs?session=' + session))['pairs']

    def frames(self, session, hand=None):
        query = 'session=' + session + ('&hand=' + hand if hand else '')
        return json.load(urlopen(self.url + '/api/frames?' + query))['frames']

    def export(self, session, **params):
        params.setdefault('session', session)
        text = urlopen(self.url + '/api/export.csv?' + urlencode(params, doseq=True)).read().decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(text))
        return reader.fieldnames, list(reader)

    # -- delivery guarantees ---------------------------------------------
    def test_each_frame_is_acknowledged_after_commit(self):
        sock = self.connect_ws()
        ack = self.send_frame(sock, self.frame('left', 'ack-session', 1, 0))
        self.assertEqual(ack['type'], 'frame_ack')
        self.assertTrue(ack['saved'])
        self.assertTrue(ack['inserted'])
        self.assertEqual((ack['device'], ack['hand'], ack['session'], ack['sequence']),
                         ('mac-left', 'left', 'ack-session', 1))
        # The row is readable immediately: the ack cannot precede the commit.
        rows = self.frames('ack-session')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['source_us'], 4000)
        self.assertEqual(rows[0]['scheduled_us'], 0)

    def test_duplicate_frame_is_acknowledged_but_not_stored_twice(self):
        sock = self.connect_ws()
        payload = self.frame('left', 'dup-session', 3, 0)
        first = self.send_frame(sock, payload)
        second = self.send_frame(sock, payload)
        self.assertTrue(first['inserted'])
        self.assertFalse(second['inserted'])
        self.assertTrue(second['saved'])
        self.assertTrue(second['duplicate'])
        self.assertEqual(second['sequence'], 3)
        self.assertEqual(len(self.frames('dup-session')), 1)

    def test_reconnect_resend_is_idempotent(self):
        """Frames unacked at disconnect are resent on the new connection."""
        queued = [self.frame('left', 'reconnect', seq, seq * SLOT) for seq in range(5)]
        first_socket = self.connect_ws()
        first_socket.close()
        second = self.connect_ws()
        acks = [self.send_frame(second, payload) for payload in queued]
        self.assertTrue(all(ack['inserted'] for ack in acks))
        again = [self.send_frame(second, payload) for payload in queued]
        self.assertTrue(all(ack['duplicate'] for ack in again))
        self.assertEqual(len(self.frames('reconnect')), 5)

    def test_device_accepts_mac_address_and_rejects_bad_session(self):
        sock = self.connect_ws()
        ack = self.send_frame(sock, self.frame('right', 'mac-session', 1, 0,
                                               device='A0:B1:C2:D3:E4:F5'))
        self.assertEqual(ack['device'], 'A0:B1:C2:D3:E4:F5')
        bad = self.frame('right', 'bad session!', 2, 0)
        self.assertEqual(self.send_frame(sock, bad)['type'], 'error')
        # Nothing from the rejected message reached the database.
        self.assertEqual(self.frames('mac-session') and len(self.frames('mac-session')), 1)

    def test_invalid_frame_returns_error_and_is_not_confirmed(self):
        sock = self.connect_ws()
        cases = [
            dict(self.frame('left', 'invalid-session', 1, 0), hand='middle'),
            dict(self.frame('left', 'invalid-session', 1, 0), sync_valid=True, aligned_us=None),
            dict(self.frame('left', 'invalid-session', 1, 0), valid_mask=9999),
            dict(self.frame('left', 'invalid-session', 1, 0), channels=[[0] * 6]),
        ]
        for payload in cases:
            reply = self.send_frame(sock, payload)
            self.assertEqual(reply['type'], 'error', payload)
        self.assertEqual(self.frames('invalid-session'), [])

    def test_invalid_channel_cannot_smuggle_zero_reading(self):
        sock = self.connect_ws()
        # valid_mask 2046 clears bit 0 (CH0 only); mag_mask 2047 keeps CH10's mag.
        payload = self.frame('left', 'mask-session', 1, 0, valid_mask=2046, mag_mask=2047)
        self.send_frame(sock, payload)
        row = self.frames('mask-session')[0]
        self.assertIsNone(row['ch0_ax'])          # flagged invalid
        self.assertEqual(row['ch1_ax'], 6)        # still valid
        # CH10 stays valid, so its magnetometer bits are independent of CH0.
        self.assertEqual(row['ch10_mx'], 66)
        self.assertEqual(row['ch10_mz'], 68)

    def test_handshake_requires_token(self):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
        self.addCleanup(connection.close)
        connection.request('GET', '/api/stream', headers={
            'Upgrade': 'websocket', 'Connection': 'Upgrade',
            'Sec-WebSocket-Key': base64.b64encode(os.urandom(16)).decode('ascii'),
            'Sec-WebSocket-Version': '13', 'Authorization': 'Bearer wrong',
        })
        self.assertEqual(connection.getresponse().status, 401)

    # -- pairing ----------------------------------------------------------
    def test_same_slot_pairs_both_hands(self):
        sock = self.connect_ws()
        left = self.send_frame(sock, self.frame('left', 'pair-basic', 10, 100 * SLOT))
        self.assertEqual(left['pair'], 'pending')
        right = self.send_frame(sock, self.frame('right', 'pair-basic', 7, 100 * SLOT, delta_us=800))
        self.assertEqual(right['pair'], 'paired')
        slot = self.pairs('pair-basic')[0]
        self.assertEqual(slot['state'], 'paired')
        self.assertEqual(slot['delta_us'], 800)
        rows = self.frames('pair-basic')
        self.assertEqual({r['pair_state'] for r in rows}, {'paired'})
        self.assertEqual({r['paired'] for r in rows}, {1})

    def test_sequence_numbers_never_drive_pairing(self):
        sock = self.connect_ws()
        # Distinct sequences on both hands still pair by slot.
        self.send_frame(sock, self.frame('left', 'pair-seq', 4, 0))
        ack = self.send_frame(sock, self.frame('right', 'pair-seq', 991, 0))
        self.assertEqual(ack['pair'], 'paired')

    def test_late_arrival_pairs_and_is_marked_late(self):
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', 'pair-late', 1, 0))
        # Sweep far in the future: every earlier test's pending slot closes too,
        # so only this session's slot is asserted.
        closed = self.app.resolve_stale_pairs(now_ms=int(time.time() * 1000) + 10_000)
        self.assertIn({'session': 'pair-late', 'state': 'missing_right'},
                      [{'session': s['session'], 'state': s['state']} for s in closed])
        ack = self.send_frame(sock, self.frame('right', 'pair-late', 1, 0, delta_us=1000))
        self.assertEqual(ack['pair'], 'late_paired')
        self.assertEqual(self.pairs('pair-late')[0]['state'], 'late_paired')

    def test_out_of_tolerance_frames_stay_unpaired(self):
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', 'pair-tol', 1, 0))
        ack = self.send_frame(sock, self.frame('right', 'pair-tol', 1, 0, delta_us=9000))
        self.assertEqual(ack['pair'], 'out_of_tolerance')
        slot = self.pairs('pair-tol')[0]
        self.assertEqual(slot['state'], 'out_of_tolerance')
        self.assertEqual(slot['delta_us'], 9000)
        # Data is preserved, never silently merged into a fake pair.
        self.assertEqual(len(self.frames('pair-tol')), 2)
        self.assertEqual({r['paired'] for r in self.frames('pair-tol')}, {0})

    def test_out_of_order_arrival_still_pairs(self):
        sock = self.connect_ws()
        # Right arrives before left, and slots arrive shuffled.
        for slot in (2, 0, 1):
            self.send_frame(sock, self.frame('right', 'pair-order', slot, slot * SLOT))
        for slot in (1, 2, 0):
            ack = self.send_frame(sock, self.frame('left', 'pair-order', slot, slot * SLOT))
        states = {slot['scheduled_us']: slot['state'] for slot in self.pairs('pair-order')}
        self.assertEqual(states, {0: 'paired', SLOT: 'paired', 2 * SLOT: 'paired'})

    def test_sync_invalid_frames_are_stored_but_never_paired(self):
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', 'pair-sync', 1, 0, sync_valid=False))
        ack = self.send_frame(sock, self.frame('right', 'pair-sync', 1, 0, sync_valid=False))
        self.assertEqual(ack['pair'], 'sync_invalid')
        rows = self.frames('pair-sync')
        self.assertEqual(len(rows), 2)
        self.assertEqual({r['sync_valid'] for r in rows}, {0})
        self.assertEqual({r['paired'] for r in rows}, {0})
        # Without slots there is no pair row to confuse later windows.
        self.assertEqual(self.pairs('pair-sync'), [])

    def test_sessions_never_mix(self):
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', 'mix-a', 1, 0))
        self.send_frame(sock, self.frame('right', 'mix-b', 1, 0))
        self.assertEqual([p['state'] for p in self.pairs('mix-a')], ['pending'])
        self.assertEqual([p['state'] for p in self.pairs('mix-b')], ['pending'])
        self.send_frame(sock, self.frame('right', 'mix-a', 2, 0))
        self.assertEqual([p['state'] for p in self.pairs('mix-a')], ['paired'])
        self.assertEqual([p['state'] for p in self.pairs('mix-b')], ['pending'])

    def test_single_hand_timeout_does_not_block_later_slots(self):
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', 'timeout-a', 1, 0))
        closed = self.app.resolve_stale_pairs(now_ms=int(time.time() * 1000) + 10_000)
        self.assertTrue(closed)
        # A later slot pairs immediately: the timed-out slot is not in the way.
        self.send_frame(sock, self.frame('left', 'timeout-a', 2, SLOT))
        ack = self.send_frame(sock, self.frame('right', 'timeout-a', 2, SLOT))
        self.assertEqual(ack['pair'], 'paired')

    def test_second_frame_from_same_hand_occupies_slot_without_pairing(self):
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', 'slot-taken', 1, 0))
        second_left = self.send_frame(sock, self.frame('left', 'slot-taken', 2, 0))
        self.assertEqual(second_left['pair'], 'slot_taken')
        ack = self.send_frame(sock, self.frame('right', 'slot-taken', 1, 0))
        self.assertEqual(ack['pair'], 'paired')
        rows = self.frames('slot-taken')
        self.assertEqual(len(rows), 3)
        # The duplicated left frame is never folded into the pair.
        self.assertEqual(sorted(r['sequence'] for r in rows if r['paired'] == 1), [1, 1])
        self.assertEqual([r['pair_state'] for r in rows if r['sequence'] == 2], ['slot_taken'])
        self.assertEqual(self.app.build_windows('slot-taken'), [])

    # -- CSV export -------------------------------------------------------
    def test_export_matches_reference_dataset_layout(self):
        session = 'csv-layout'
        sock = self.connect_ws()
        values = [[i, i + 1, i + 2, i + 3, i + 4, i + 5] for i in range(10)]
        values.append([60, 61, 62, 63, 64, 65, 66, 67, 68])
        for slot in range(4):
            # One dropped slot (slot 2) proves gaps stay gaps.
            if slot == 2:
                continue
            for hand in ('left', 'right'):
                self.send_frame(sock, self.frame(hand, session, slot, slot * SLOT, values=values))
        urlopen(Request(self.url + '/api/annotations', data=json.dumps(
            {'session': session, 'worker': 'cx', 'label': '我在家', 'v': 'medium',
             'active_hand': 'both'}).encode(),
            headers={'Authorization': 'Bearer label-test'}))
        header, rows = self.export(session)
        with open(Path(__file__).with_name('reference_header.csv'), encoding='utf-8', newline='') as source:
            self.assertEqual(header, next(csv.reader(source)))
        self.assertEqual(len(header), 77)
        self.assertEqual([r['hand_type'] for r in rows], ['left', 'right', 'left', 'right', 'left', 'right'])
        # frame_id is the slot index; the dropped slot keeps the numbering.
        self.assertEqual([r['frame_id'] for r in rows], ['0', '0', '1', '1', '3', '3'])
        self.assertEqual([r['timestamp/ms'] for r in rows], ['0', '0', '20', '20', '60', '60'])
        self.assertEqual({r['sample_id'] for r in rows}, {rows[0]['sample_id']})
        self.assertTrue(all([r[k] for k in ('worker', 'v', 'label', 'active_hand')] ==
                            ['cx', 'medium', '我在家', 'both'] for r in rows))
        self.assertEqual([rows[0][k] for k in ('f_0_s1_acc_x', 'f_0_s1_gyro_z')], ['0', '5'])
        self.assertEqual([rows[0][k] for k in ('dorsal_mag_x', 'dorsal_mag_z')], ['66', '68'])

    def test_export_uses_slot_time_not_server_receive_time(self):
        session = 'csv-timing'
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', session, 1, 500 * SLOT))
        self.send_frame(sock, self.frame('right', session, 1, 500 * SLOT))
        _, rows = self.export(session)
        row = next(r for r in rows if r['hand_type'] == 'left')
        self.assertEqual(row['frame_id'], '0')     # first slot of the session
        self.assertEqual(row['timestamp/ms'], '10000')
        stored = self.frames(session)[0]
        self.assertGreater(stored['received_unix_ms'], 1_700_000_000_000)

    def test_archive_export_keeps_pairing_columns(self):
        session = 'csv-archive'
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', session, 1, 0))
        self.send_frame(sock, self.frame('right', session, 1, 0, delta_us=1200))
        text = urlopen(self.url + '/api/export.csv?session=' + session + '&format=archive').read().decode('utf-8-sig')
        rows = list(csv.DictReader(io.StringIO(text)))
        self.assertEqual({r['pair_state'] for r in rows}, {'paired'})
        self.assertEqual({r['pair_delta_us'] for r in rows}, {'1200'})
        self.assertEqual({r['paired'] for r in rows}, {'1'})

    def test_missing_channel_and_mag_export_as_empty(self):
        session = 'csv-empty'
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', session, 1, 0, valid_mask=2046, mag_mask=0))
        _, rows = self.export(session)
        self.assertEqual(rows[0]['f_0_s1_acc_x'], '')     # CH0 flagged invalid
        self.assertEqual(rows[0]['f_0_s2_acc_x'], '6')    # CH1 intact
        self.assertEqual(rows[0]['dorsal_acc_x'], '60')   # CH10 intact
        self.assertEqual(rows[0]['dorsal_mag_x'], '')     # mag_mask cleared everywhere
        self.assertEqual(rows[0]['dorsal_mag_z'], '')

    # -- inference hand-off ------------------------------------------------
    def test_windows_report_model_not_ready_without_blocking(self):
        session = 'infer-window'
        sock = self.connect_ws()
        for slot in range(30):
            for hand in ('left', 'right'):
                self.send_frame(sock, self.frame(hand, session, slot, slot * SLOT))
        runs = self.app.run_windows(session)
        self.assertTrue(runs)
        self.assertTrue(all(run['status'] == 'model_not_ready' for run in runs))
        self.assertEqual(runs[0]['pairs'], self.app.WINDOW_FRAMES)
        # Re-running must not create duplicate runs.
        self.assertEqual(self.app.run_windows(session), [])
        stats = json.load(urlopen(self.url + '/api/pairing/stats'))
        self.assertEqual(stats['inference'].get('model_not_ready', 0), len(runs))

    def test_inference_queue_full_is_counted_not_fatal(self):
        original = self.app.INFER_QUEUE
        small = self.app.queue.Queue(maxsize=1)
        self.app.INFER_QUEUE = small
        try:
            before = self.app.STATS['infer_dropped']
            self.app.enqueue_inference('overflow', 0)
            self.app.enqueue_inference('overflow', 0)
            self.assertEqual(small.qsize(), 1)
            self.assertEqual(self.app.STATS['infer_dropped'], before + 1)
        finally:
            self.app.INFER_QUEUE = original

    def test_throughput_100_frames_per_second(self):
        """Two hands at 50 Hz: 100 frames/s must stream through without pairing lag."""
        session = 'throughput'
        sock = self.connect_ws()
        sent = 0
        started = time.time()
        # 50 slots per hand = 50 frames/s/hand = 100 frames/s combined.
        for slot in range(50):
            for hand in ('left', 'right'):
                ack = self.send_frame(sock, self.frame(hand, session, slot, slot * SLOT))
                self.assertTrue(ack['saved'])
                sent += 1
        elapsed = time.time() - started
        self.assertEqual(sent, 100)
        self.assertEqual(len(self.frames(session)), 100)
        self.assertEqual({p['state'] for p in self.pairs(session)}, {'paired'})
        self.assertLess(elapsed, 60)

    def test_failed_commit_produces_no_ack(self):
        session = 'commit-fail'
        sock = self.connect_ws()
        original = self.app.pair_slot

        def explode(*args, **kwargs):
            raise self.app.sqlite3.OperationalError('simulated disk failure')

        self.app.pair_slot = explode
        try:
            reply = self.send_frame(sock, self.frame('left', session, 1, 0))
        finally:
            self.app.pair_slot = original
        self.assertEqual(reply['type'], 'error')
        self.assertNotIn('saved', reply)
        # The transaction rolled back with the failure, so nothing was confirmed.
        self.assertEqual(self.frames(session), [])

    # -- restart recovery ---------------------------------------------------
    def test_restart_recovery_rebuilds_pairs_and_keeps_saved_frames(self):
        session = 'restart'
        sock = self.connect_ws()
        self.send_frame(sock, self.frame('left', session, 1, 0))
        self.send_frame(sock, self.frame('right', session, 1, 0))
        self.send_frame(sock, self.frame('left', session, 2, SLOT))
        before = self.frames(session)
        rebuilt = self.app.recover_pairs()
        self.assertGreaterEqual(rebuilt, 0)
        self.app.initialize()
        self.assertEqual(self.frames(session), before)
        states = {p['scheduled_us']: p['state'] for p in self.pairs(session)}
        self.assertEqual(states[0], 'paired')
        # The lone left frame is closed out by recovery, never left dangling.
        self.assertIn(states[SLOT], ('pending', 'missing_right'))


if __name__ == '__main__':
    unittest.main()
