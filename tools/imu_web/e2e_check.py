#!/usr/bin/env python3
"""Local end-to-end check for the per-frame WSS path.

Starts nothing on the network you do not control: it boots the recorder on a
temporary database on 127.0.0.1, drives two simulated gloves at 50 Hz each
through the real WebSocket handshake, then packages the result with
pack_dataset.py and prints the CSV head.
"""
import base64
import csv
import io
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

HERE = Path(__file__).resolve().parent
SLOT_US = 20000


def handshake(port, token):
    sock = socket.create_connection(('127.0.0.1', port), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode('ascii')
    sock.sendall((f'GET /api/stream HTTP/1.1\r\nHost: 127.0.0.1\r\n'
                  'Upgrade: websocket\r\nConnection: Upgrade\r\n'
                  f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n'
                  f'Authorization: Bearer {token}\r\n\r\n').encode('ascii'))
    reply = bytearray()
    while b'\r\n\r\n' not in reply:
        reply.extend(sock.recv(4096))
    if b'101 Switching Protocols' not in reply:
        raise SystemExit('handshake failed: %r' % bytes(reply[:120]))
    return sock


def send_frame(sock, payload):
    body = json.dumps(payload, separators=(',', ':')).encode('utf-8')
    mask = os.urandom(4)
    size = len(body)
    if size < 126:
        header = bytes((0x81, 0x80 | size))
    else:
        header = bytes((0x81, 0xfe)) + struct.pack('!H', size)
    sock.sendall(header + mask + bytes(b ^ mask[i & 3] for i, b in enumerate(body)))


def recv_frame(sock):
    header = sock.recv(2)
    length = header[1] & 0x7f
    if length == 126:
        length = struct.unpack('!H', sock.recv(2))[0]
    elif length == 127:
        length = struct.unpack('!Q', sock.recv(8))[0]
    payload = bytearray()
    while len(payload) < length:
        payload.extend(sock.recv(length - len(payload)))
    return json.loads(payload.decode('utf-8'))


def main():
    temp = tempfile.TemporaryDirectory()
    db = str(Path(temp.name) / 'e2e.sqlite3')
    os.environ['IMU_DB'] = db
    os.environ['IMU_TOKEN'] = 'e2e-token'

    import importlib.util
    spec = importlib.util.spec_from_file_location('e2e_server', HERE / 'server.py')
    app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)
    app.initialize()
    app.start_background()
    server = app.Server(('127.0.0.1', 0), app.Handler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    session = 'e2e' + str(int(time.time()))
    acks, started = 0, time.time()
    try:
        left = handshake(port, 'e2e-token')
        right = handshake(port, 'e2e-token')
        for slot in range(50):                       # 1 s of capture at 50 Hz per hand
            scheduled = slot * SLOT_US
            for hand, sock in (('left', left), ('right', right)):
                if slot == 12 and hand == 'right':
                    continue                          # simulate one dropped right frame
                send_frame(sock, {
                    'type': 'frame', 'device': 'e2e-' + hand, 'hand': hand,
                    'session': session, 'sequence': slot,
                    'timestamp': 60000 + slot * 20,
                    'source_us': 4000000 + scheduled,
                    'aligned_us': scheduled + (300 if hand == 'left' else 900),
                    'scheduled_us': scheduled,
                    'clock_offset_us': -148000000, 'scan_us': 3100,
                    'sync_rtt_us': 900, 'sync_age_ms': 120, 'sync_valid': True,
                    'valid_mask': 2047, 'mag_mask': 1024,
                    'channels': [[ch * 6 + axis for axis in range(6)] for ch in range(10)]
                                + [[60 + axis for axis in range(9)]],
                })
                ack = recv_frame(sock)
                if not ack.get('saved'):
                    raise SystemExit('frame not saved: %r' % ack)
                acks += 1
        elapsed = time.time() - started
        left.close()
        right.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    app.resolve_stale_pairs(now_ms=int(time.time() * 1000) + 10_000)
    for _ in range(20):                              # let the worker drain the queue
        if app.INFER_QUEUE.empty():
            break
        time.sleep(0.05)

    with app.connect() as con:
        app.annotate({'session': session, 'worker': 'cx', 'label': '我在家',
                      'v': 'medium', 'active_hand': 'both'})
        states = {r['state']: r['n'] for r in con.execute(
            'SELECT state,count(*) n FROM pairs WHERE session=? GROUP BY state', (session,))}
        frames = con.execute('SELECT count(*) FROM frames WHERE session=?', (session,)).fetchone()[0]
        runs = con.execute('SELECT count(*) FROM inference_runs WHERE session=?', (session,)).fetchone()[0]

    print('frames acknowledged : %d in %.2f s (%.0f frames/s)' % (acks, elapsed, acks / elapsed))
    print('frames stored       : %d' % frames)
    print('pair states         : %s' % states)
    print('inference runs      : %d' % runs)

    out = Path(temp.name) / 'dataset.csv'
    env = dict(os.environ, IMU_DB=db, PYTHONPATH=str(HERE))
    result = subprocess.run([sys.executable, str(HERE / 'pack_dataset.py'),
                             '--session', session, '--out', str(out)],
                            capture_output=True, text=True, env=env)
    if result.returncode:
        raise SystemExit(result.stderr)
    with out.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = list(reader)
    with out.open(encoding='utf-8-sig', newline='') as handle:
        rows = list(csv.DictReader(handle))
    print('csv                 : %d rows, %d columns' % (len(rows), len(header)))
    print('first rows (frame_id,timestamp,hand):')
    for row in rows[:6]:
        print('  %s, %s, %s' % (row['frame_id'], row['timestamp/ms'], row['hand_type']))
    print('dropped slot 12 leaves a gap, no renumbering: %s'
          % [row['frame_id'] for row in rows if row['hand_type'] == 'left'][11:15])
    temp.cleanup()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
