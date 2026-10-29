#!/usr/bin/env python3
"""Real candidate negative controls; takes the directory exported by CI."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('candidate', type=Path)
    args = parser.parse_args()
    candidate = args.candidate.resolve()
    receipt = json.loads((candidate / 'receipt.json').read_text())
    runtime = json.loads((candidate / 'runtime.json').read_text())
    helper = Path(__file__).with_name('tested_image.py')
    common = [item for key in ('source', 'revision', 'version', 'platform')
              for item in ('--' + key, receipt[key])]
    with tempfile.TemporaryDirectory(prefix='candidate-negative-') as temp:
        fixture = Path(temp)
        (fixture / 'image.tar.gz').symlink_to(candidate / 'image.tar.gz')
        def reject(name, changed_receipt, changed_runtime, expected, extra=()):
            (fixture / 'receipt.json').write_text(json.dumps(changed_receipt))
            (fixture / 'runtime.json').write_text(json.dumps(changed_runtime))
            command = [sys.executable, str(helper), 'load', '--dir', str(fixture), *common, *extra]
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0 or expected not in result.stderr:
                raise AssertionError(f'{name} was not rejected at its intended boundary: {result.stderr}')
            print(f'PASS: {name}')
        for key, value in [('source', 'https://example.com/unrelated'), ('revision', '0' * 40), ('version', 'v0.0.0')]:
            reject('wrong ' + key, receipt, runtime, 'another source or release', ('--' + key, value))
        reject('changed archive checksum', {**receipt, 'archive_sha256': '0' * 64}, runtime, 'checksum')
        reject('unbound runtime proof', receipt, {**runtime, 'image_id': 'sha256:' + '0' * 64},
               'Runtime acceptance does not match')
        (fixture / 'runtime.json').write_text(json.dumps({**runtime, 'image_id': 'sha256:' + '0' * 64}))
        result = subprocess.run([sys.executable, str(helper), 'export', '--dir', str(fixture / 'bad-export'),
                                 '--image', receipt['image_id'], '--runtime', str(fixture / 'runtime.json'), *common],
                                capture_output=True, text=True)
        if result.returncode == 0 or 'not bound to the image' not in result.stderr:
            raise AssertionError(f'An untested export was not rejected: {result.stderr}')
        print('PASS: export of an image without its matching runtime proof')


if __name__ == '__main__':
    main()
