"""Tải đúng 400 video split val RWF-2000 từ mirror public, không tải train.

Nhãn lấy từ thư mục Fight/NonFight của dataset, không từ dự đoán của model.
Revision được cố định, kiểm tra LFS SHA256 nếu metadata cung cấp; manifest
ghi nguồn và hash để tái lập. Video chỉ lưu trong data/ bị loại khỏi Git.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote
import requests

REPO = 'A1mal/RWF-2000-Dataset'
REVISION = '643fe417947d4265fcfaef407332468709728aa8'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parent/'data/eval_rwf2000')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    files = []
    for name, label in [('Fight', 1), ('NonFight', 0)]:
        path = f'data/RWF-2000 Sliced/val/{name}'
        url = f'https://huggingface.co/api/datasets/{REPO}/tree/{REVISION}/{quote(path,safe="/")}'
        response = requests.get(url, params={'limit': 1000}, timeout=30)
        response.raise_for_status()
        entries = [v for v in response.json() if v.get('type') == 'file' and v['path'].endswith('.avi')]
        if len(entries) != 200:
            raise RuntimeError(f'Expected 200 files in {name}, got {len(entries)}')
        for entry in entries:
            files.append({'dataset_path': entry['path'], 'label': label,
                          'relative_path': f"{name}/{Path(entry['path']).name}",
                          'bytes': entry['size'], 'expected_sha256': (entry.get('lfs') or {}).get('oid')})

    def download(row):
        path = args.output/row['relative_path']
        path.parent.mkdir(exist_ok=True)
        if path.exists() and path.stat().st_size == row['bytes']:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if not row['expected_sha256'] or digest == row['expected_sha256']:
                return {**row, 'sha256': digest}
        url = f'https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/{quote(row["dataset_path"],safe="/")}'
        error = None
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=(15, 60))
                response.raise_for_status()
                content = response.content
                digest = hashlib.sha256(content).hexdigest()
                if len(content) != row['bytes'] or (row['expected_sha256'] and digest != row['expected_sha256']):
                    raise ValueError('dataset file size/hash mismatch')
                part = path.with_suffix('.part')
                part.write_bytes(content)
                part.replace(path)
                return {**row, 'sha256': digest}
            except Exception as exc:
                error = exc
                time.sleep(attempt+1)
        raise RuntimeError(f'Cannot download {row["relative_path"]}') from error

    rows = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(download, row) for row in files]
        for future in as_completed(futures):
            rows.append(future.result())
            if len(rows) % 20 == 0:
                print(f'Downloaded/verified {len(rows)}/400', flush=True)
    manifest = {'source': f'https://huggingface.co/datasets/{REPO}', 'revision': REVISION,
                'split': 'val', 'label_source': 'provided dataset directory Fight/NonFight',
                'note': 'Mirror validation split; not an independently audited upstream training split.',
                'samples': sorted(rows, key=lambda row: row['relative_path'])}
    (args.output/'manifest.json').write_text(json.dumps(manifest, indent=2))
    print('Manifest written; videos remain local and are not redistributed.', flush=True)


if __name__ == '__main__':
    main()
