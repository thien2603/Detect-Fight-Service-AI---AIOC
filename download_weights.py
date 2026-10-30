"""Tải đúng ba trọng số đã dùng trên staging; kiểm tra SHA256 trước khi sử dụng.

Luồng: đọc manifest trong file → kiểm tra file có sẵn → tải vào .part →
so hash → đổi tên nguyên tử. Không chạy model hoặc kết nối MQTT.
"""
import hashlib
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent / 'weights'
ASSETS = {
    'yolo11n.pt': (
        'https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n.pt',
        '0ebbc80d4a7680d14987a577cd21342b65ecfd94632bd9a8da63ae6417644ee1'),
    'modelA0_stream_statedict_v3': (
        'https://raw.githubusercontent.com/Atze00/MoViNet-pytorch/main/weights/modelA0_stream_statedict_v3',
        '447c0554daa6bebdcf6fc69b2651b25b29cc69e003da4e6ff56f9a2488f403cf'),
    'model_20.pt': (
        'https://raw.githubusercontent.com/roggerfq/violence-detection-mil/main/test/model_20.pt',
        '25f6a84b86c9b91d84fba3c6f267e2bc50b30c1f6bd62d73bcd095b9084f9cef'),
}


def main():
    ROOT.mkdir(exist_ok=True)
    for name, (url, expected) in ASSETS.items():
        path = ROOT / name
        if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == expected:
            print(name, 'already verified')
            continue
        temporary = path.with_name(name + '.part')
        digest = hashlib.sha256()
        with requests.get(url, stream=True, timeout=(10, 60)) as response:
            response.raise_for_status()
            with temporary.open('wb') as stream:
                for chunk in response.iter_content(1024 * 1024):
                    stream.write(chunk)
                    digest.update(chunk)
        if digest.hexdigest() != expected:
            raise RuntimeError(f'{name}: SHA256 mismatch; .part is not used')
        temporary.replace(path)
        print(name, 'downloaded and verified')


if __name__ == '__main__':
    main()
