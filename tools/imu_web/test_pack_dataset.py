"""End-to-end check: stream frames over WSS, then package them with pack_dataset.py."""
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

HERE = Path(__file__).with_name('test_frame_stream.py')


class PackDatasetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import importlib.util
        cls.temp = tempfile.TemporaryDirectory()
        cls.db = str(Path(cls.temp.name) / 'pack.sqlite3')
        os.environ['IMU_DB'] = cls.db
        os.environ['IMU_TOKEN'] = 'test-token'
        spec = importlib.util.spec_from_file_location('pack_server', Path(__file__).with_name('server.py'))
        cls.app = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.app)
        cls.app.initialize()
        # Two paired slots plus one slot missing its right hand.
        for slot in range(3):
            for hand in ('left', 'right'):
                if slot == 1 and hand == 'right':
                    continue
                cls.app.ingest_frame({
                    'type': 'frame', 'device': 'mac-' + hand, 'hand': hand,
                    'session': 'pack-session', 'sequence': slot,
                    'timestamp': slot * 20 + 60000, 'source_us': slot * 20000,
                    'aligned_us': slot * 20000 + 900, 'scheduled_us': slot * 20000,
                    'sync_valid': True, 'valid_mask': 2047, 'mag_mask': 1024,
                    'channels': [[i * 6 + a for a in range(6)] for i in range(10)]
                                + [[60 + a for a in range(9)]],
                })
        cls.app.annotate({'session': 'pack-session', 'worker': 'cx', 'label': '我在家',
                          'v': 'medium', 'active_hand': 'both'})

    @classmethod
    def tearDownClass(cls):
        import gc
        gc.collect()
        cls.temp.cleanup()

    def run_pack(self, *extra):
        out = Path(self.temp.name) / 'out.csv'
        if out.exists():
            out.unlink()
        env = dict(os.environ, IMU_DB=self.db, PYTHONPATH=str(HERE.parent))
        result = subprocess.run([sys.executable, str(HERE.parent / 'pack_dataset.py'),
                                 '--session', 'pack-session', '--out', str(out), *extra],
                                capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        return out

    def test_pack_matches_reference_layout_and_keeps_gap(self):
        out = self.run_pack()
        with out.open(encoding='utf-8-sig', newline='') as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
        with (HERE.parent / 'reference_header.csv').open(encoding='utf-8', newline='') as handle:
            self.assertEqual(reader.fieldnames, next(csv.reader(handle)))
        # Slot 1 has no right hand, so it contributes a left-only row.
        self.assertEqual([(r['frame_id'], r['hand_type']) for r in rows],
                         [('0', 'left'), ('0', 'right'), ('1', 'left'), ('2', 'left'), ('2', 'right')])
        self.assertTrue(all(r['worker'] == 'cx' and r['label'] == '我在家' for r in rows))
        self.assertEqual(rows[0]['f_0_s1_acc_x'], '0')
        self.assertEqual(rows[0]['dorsal_mag_z'], '68')

    def test_archive_sidecar_written(self):
        out = self.run_pack('--archive')
        archive = out.with_name(out.stem + '_archive.csv')
        self.assertTrue(archive.exists())
        with archive.open(encoding='utf-8-sig', newline='') as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 5)
        self.assertIn('pair_state', rows[0])
        # Archive rows carry the slot itself, so pairing is checked there.
        self.assertEqual({r['pair_state'] for r in rows if r['scheduled_us'] == '0'}, {'paired'})
        self.assertEqual({r['pair_state'] for r in rows if r['scheduled_us'] == '40000'}, {'paired'})

    def test_empty_filter_exits_nonzero(self):
        out = Path(self.temp.name) / 'empty.csv'
        env = dict(os.environ, IMU_DB=self.db, PYTHONPATH=str(HERE.parent))
        result = subprocess.run([sys.executable, str(HERE.parent / 'pack_dataset.py'),
                                 '--session', 'does-not-exist', '--out', str(out)],
                                capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 1)
        self.assertIn('no frames matched', result.stderr)


if __name__ == '__main__':
    unittest.main()
