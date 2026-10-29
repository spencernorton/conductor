#!/usr/bin/env python3
"""Export a runtime-qualified image, or load it for publication without rebuilding."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True)


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def inspect(image, expected):
    info = json.loads(docker('image', 'inspect', image))[0]
    labels = info['Config'].get('Labels') or {}
    actual = {key: labels.get('org.opencontainers.image.' + key)
              for key in ('source', 'revision', 'version')}
    actual['platform'] = info['Os'] + '/' + info['Architecture']
    if actual != expected:
        raise ValueError(f'Image metadata does not match this release: {actual!r}')
    return info['Id']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['export', 'load'])
    parser.add_argument('--dir', type=Path, required=True)
    parser.add_argument('--image')
    parser.add_argument('--runtime', type=Path)
    parser.add_argument('--source', required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--platform', default='linux/amd64')
    args = parser.parse_args()
    expected = {key: getattr(args, key) for key in ('source', 'revision', 'version', 'platform')}
    archive = args.dir / 'image.tar.gz'
    receipt_file = args.dir / 'receipt.json'
    runtime_file = args.dir / 'runtime.json'
    if args.action == 'export':
        if not args.image or not args.runtime:
            parser.error('export requires --image and --runtime')
        image_id = inspect(args.image, expected)
        runtime = json.loads(args.runtime.read_text())
        if runtime.get('image_id') != image_id or not runtime.get('checks'):
            raise ValueError('Runtime acceptance is not bound to the image being exported')
        args.dir.mkdir(parents=True, exist_ok=True)
        # Save by immutable ID after the real runtime check. Stream compression
        # keeps the GPU variant from requiring two uncompressed copies on disk.
        with archive.open('wb') as destination:
            with subprocess.Popen(['docker', 'image', 'save', image_id], stdout=subprocess.PIPE) as save:
                try:
                    with gzip.GzipFile(fileobj=destination, mode='wb', mtime=0) as compressed:
                        shutil.copyfileobj(save.stdout, compressed)
                except BaseException:
                    save.kill()
                    raise
                if save.wait() != 0:
                    raise RuntimeError('docker image save failed')
        runtime_file.write_text(json.dumps(runtime, indent=2) + '\n')
        receipt = {'schema': 1, 'image_id': image_id, 'archive_sha256': digest(archive), **expected}
        receipt_file.write_text(json.dumps(receipt, indent=2) + '\n')
    else:
        receipt = json.loads(receipt_file.read_text())
        if receipt.get('schema') != 1 or any(receipt.get(key) != value for key, value in expected.items()):
            raise ValueError('The candidate receipt belongs to another source or release')
        image_id = receipt.get('image_id', '')
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', image_id):
            raise ValueError('Invalid candidate image ID')
        if digest(archive) != receipt.get('archive_sha256'):
            raise ValueError('The image archive checksum does not match the tested candidate')
        runtime = json.loads(runtime_file.read_text())
        if runtime.get('image_id') != image_id or not runtime.get('checks'):
            raise ValueError('Runtime acceptance does not match the loaded candidate')
        # Docker's informational output goes to stderr; stdout is just the ID
        # which the caller tags and pushes. No code or layer is rebuilt here.
        print(docker('image', 'load', '--input', str(archive)), file=sys.stderr, end='')
        if inspect(image_id, expected) != image_id:
            raise ValueError('Loaded image identity differs from the runtime-qualified image')
    print(image_id)


if __name__ == '__main__':
    main()
