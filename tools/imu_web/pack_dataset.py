#!/usr/bin/env python3
"""Build the 77-column dataset CSV from the recorder database.

The web dashboard already exports through ``/api/export.csv``; this script is the
server-side equivalent for cron jobs and offline packaging. It uses the same
export code, so the layout cannot drift between the two paths.

    python pack_dataset.py --out /var/lib/glove-imu/exports/dataset.csv
    python pack_dataset.py --session 65123 --counts calibrated --out one.csv
    python pack_dataset.py --worker P001 --v medium --out p001.csv
"""
import argparse
import csv
import datetime as dt
import io
import json
from pathlib import Path
import sys

import server


def collect(params):
    """Run the same query the HTTP export uses and return (header, rows)."""
    where, args = server.filters(params)
    columns = server.META + server.VALUES + ['calibration_version'] + \
        server.CAL_VALUES + server.SYNC_VALUES + server.PAIR_COLUMNS
    calibration = None
    if params.get('counts', ['raw'])[0] == 'calibrated':
        calibration = json.loads(
            Path(__file__).with_name('calibration_coefficients.json').read_text(encoding='utf-8'))
    rows = []
    with server.connect() as con:
        con.execute('BEGIN')
        annotations = {r['session']: dict(r) for r in con.execute('SELECT * FROM annotations')}
        sample_ids = {r['session']: r['sample_id'] for r in con.execute('SELECT * FROM samples')}
        origin = server.dataset_slots(con, where, args)
        order = ('(SELECT sample_id FROM samples s WHERE s.session=frames.session),'
                 'COALESCE(scheduled_us,timestamp_ms*1000),'
                 "CASE hand WHEN 'left' THEN 0 ELSE 1 END,id")
        for row in con.execute('SELECT * FROM frames' + where + ' ORDER BY ' + order, args):
            annotation = annotations.get(row['session'], {})
            frame_id, timestamp = server.dataset_position(row, origin)
            rows.append([annotation.get(k, '') for k in ('worker', 'v', 'label', 'active_hand')]
                        + [sample_ids[row['session']], timestamp, frame_id, row['hand']]
                        + (server.calibrated_counts(row, calibration) if calibration
                           else [row[k] for k in server.VALUES]))
    return server.DATASET_COLUMNS, rows, columns


def main(argv=None):
    parser = argparse.ArgumentParser(description='Package glove IMU frames as the 77-column dataset.')
    parser.add_argument('--out', help='destination .csv (default: stdout)')
    parser.add_argument('--session', action='append', default=[], help='session id (repeatable)')
    parser.add_argument('--device', help='device id filter')
    parser.add_argument('--hand', choices=['left', 'right'], help='single hand only')
    parser.add_argument('--worker', help='annotation worker filter')
    parser.add_argument('--label', help='annotation label filter')
    parser.add_argument('--v', choices=['slow', 'medium', 'high'], help='speed filter')
    parser.add_argument('--active-hand', choices=['left', 'right', 'both'], help='annotation active_hand filter')
    parser.add_argument('--counts', choices=['raw', 'calibrated'], default='raw',
                        help='raw registers (default) or calibrated equivalent counts')
    parser.add_argument('--archive', action='store_true',
                        help='also write the diagnostic archive CSV next to --out')
    args = parser.parse_args(argv)

    params = {}
    for key, value in (('session', args.session), ('device', args.device), ('hand', args.hand),
                       ('worker', args.worker), ('label', args.label), ('v', args.v),
                       ('active_hand', args.active_hand), ('counts', args.counts)):
        if value:
            params[key] = value if isinstance(value, list) else [value]

    header, rows, archive_columns = collect(params)
    if not rows:
        print('no frames matched the filter', file=sys.stderr)
        return 1

    buffer = io.StringIO(newline='')
    writer = csv.writer(buffer)
    writer.writerow(header)
    writer.writerows(rows)
    text = buffer.getvalue()

    if args.out:
        target = Path(args.out)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('\ufeff' + text, encoding='utf-8')
        print('wrote %s (%d rows, %d columns)' % (target, len(rows), len(header)), file=sys.stderr)
        if args.archive:
            archive = target.with_name(target.stem + '_archive.csv')
            archive_buffer = io.StringIO(newline='')
            archive_writer = csv.writer(archive_buffer)
            archive_writer.writerow(archive_columns + ['worker', 'label', 'v', 'active_hand'])
            where, args_list = server.filters(params)
            with server.connect() as con:
                annotations = {r['session']: dict(r) for r in con.execute('SELECT * FROM annotations')}
                for row in con.execute('SELECT ' + ','.join(archive_columns) + ' FROM frames' + where + ' ORDER BY id', args_list):
                    annotation = annotations.get(row['session'], {})
                    archive_writer.writerow(tuple(row) + tuple(
                        annotation.get(k, '') for k in ('worker', 'label', 'v', 'active_hand')))
            archive.write_text('\ufeff' + archive_buffer.getvalue(), encoding='utf-8')
            print('wrote %s' % archive, file=sys.stderr)
    else:
        sys.stdout.write('\ufeff' + text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
