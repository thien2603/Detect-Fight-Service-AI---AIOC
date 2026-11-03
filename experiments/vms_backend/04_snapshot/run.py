"""04 - Upload một ảnh JPG/PNG, lấy object_key và tải lại kiểm tra.
Luồng chạy: đọc ảnh -> SEND=True mới kiểm camera -> POST một lần -> GET so hash -> lưu key.
Chạy: python run.py. Sửa các biến ở đầu file; không dùng tham số --seconds/--send.
"""
import json
from pathlib import Path
import socket
import ssl
import threading
import time
import uuid
import hashlib
import requests
from dotenv import dotenv_values
import paho.mqtt.client as mqtt

# Cấu hình dùng chung: tài khoản, zone, organization, camera và module trong .env.
HERE = Path(__file__).resolve().parent
SUITE = HERE.parent
OUT = HERE / 'output'
config = dotenv_values(SUITE / '.env')
HOST = config.get('VMS_MQTT_HOST') or 'aioc-stg.atin.vn'
BROKER_IP = config.get('VMS_BROKER_IP') or ''
USERNAME = config.get('VMS_MQTT_USERNAME') or ''
PASSWORD = config.get('VMS_MQTT_PASSWORD') or ''
ZONE = int(config.get('VMS_ZONE_ID') or 0)
ORGANIZATION = int(config.get('VMS_ORGANIZATION_ID') or 0)
CAMERA = config.get('VMS_CAMERA_CODE') or ''
CAMERA_ID = config.get('VMS_CAMERA_ID') or ''
MODULE = config.get('VMS_MODULE') or 'FIGHT_DETECTION'

SEND = False
IMAGE = SUITE / "snapshot.jpg"  # Chép ảnh từ VLC vào đây hoặc sửa đường dẫn.


def main():

    if not CAMERA or not CAMERA_ID or not ORGANIZATION:
        raise ValueError('Chạy case01 và điền CAMERA_CODE, CAMERA_ID, ORGANIZATION_ID trong .env trước.')

    image = IMAGE.read_bytes()
    if not 0 < len(image) <= 10_000_000:
        raise ValueError('Ảnh phải >0 và không quá 10 MB.')
    if image.startswith(bytes.fromhex('ffd8ff')):
        mime, suffix = 'image/jpeg', '.jpg'
    elif image.startswith(bytes.fromhex('89504e470d0a1a0a')):
        mime, suffix = 'image/png', '.png'
    else:
        raise ValueError('Chọn file JPG/PNG thật.')
    category = 'anomaly' if MODULE == 'FIGHT_DETECTION' else 'plate'
    print('UPLOAD:', IMAGE, '; bytes:', len(image), '; camera:', CAMERA, '; category:', category)
    if not SEND:
        print('Chỉ xem trước. Đổi SEND = True để upload ảnh.')
        return

    # Nếu cần IP nội bộ, chỉ đổi đích TCP; TLS vẫn kiểm chứng chỉ theo HOST.
    original_dns = socket.getaddrinfo
    if BROKER_IP:
        def resolve(host, port, *args, **kwargs):
            address = BROKER_IP if host == HOST else host
            return original_dns(address, port, *args, **kwargs)
        socket.getaddrinfo = resolve

    # Tạo kết nối riêng cho bài test, không dùng trùng client_id của worker thật.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                        client_id='tts_test_' + uuid.uuid4().hex[:8] + '_01',
                        transport='websockets')
    client.ws_set_options(path='/mqtt')
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.username_pw_set(USERNAME, PASSWORD)

    # Chỉ chọn camera trong đúng company/zone/UUID đã cấu hình, không đoán ID.
    camera = {}
    camera_ready = threading.Event()
    def on_connect(c, userdata, flags, reason, properties):
        print('CONNECTED:', reason)
        if reason == 0:
            c.subscribe(f'smart_vms/cameras/company/{ORGANIZATION}', qos=1)

    def on_subscribe(c, userdata, mid, reasons, properties):
        print('SUBACK:', mid, reasons)
        if not any(reason.is_failure for reason in reasons):
            c.publish('smart_vms/cameras/getdevice',
                      json.dumps({'zone_id': ZONE, 'organization_id': ORGANIZATION}),
                      qos=1, retain=False)

    def on_message(c, userdata, msg):
        try:
            data = json.loads(msg.payload)
            for item in data.get('cameras', []):
                if item.get('id') == CAMERA_ID and item.get('code') == CAMERA and item.get('zone_id') == ZONE:
                    camera.update(item)
                    camera_ready.set()
        except (ValueError, AttributeError, TypeError):
            print('Bỏ qua bản tin cấu hình không hợp lệ:', msg.topic)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    try:

        client.connect(HOST, 443, keepalive=30)
        client.loop_start()
        if not camera_ready.wait(15):
            raise RuntimeError('Không nhận đúng camera. Kiểm tra .env và chạy case01 trước.')

        if MODULE not in camera.get('ai_modules', []):
            raise RuntimeError('Camera chưa bật module ' + MODULE)

        response = requests.post(f'https://{HOST}/storage/upload/file',
            files={'file': ('snapshot' + suffix, image, mime)},
            data={'camera_id': CAMERA_ID, 'category': category}, timeout=(8, 40))
        response.raise_for_status()
        data = response.json()
        key = data.get('object_key')
        if data.get('success') is not True or not isinstance(key, str) or not key.startswith(category + '/'):
            raise ValueError('Upload chưa trả object_key hợp lệ.')
        OUT.mkdir(exist_ok=True)
        record = {'camera_id': CAMERA_ID, 'camera_code': CAMERA, 'module': MODULE,
                  'object_key': key, 'sha256': hashlib.sha256(image).hexdigest(), 'download_verified': False}
        record_file = OUT / 'snapshot.json'
        # Lưu key ngay để không phải upload lại nếu bước GET bên dưới lỗi.
        record_file.write_text(json.dumps(record, indent=2), encoding='utf-8')
        print('UPLOAD HTTP:', response.status_code, '; OBJECT_KEY:', key)
        check = requests.get(f'https://{HOST}/api/v1/snapshot/image',
                             params={'object_key': key}, timeout=(8, 20))
        check.raise_for_status()
        if hashlib.sha256(check.content).hexdigest() != record['sha256']:
            raise ValueError('Ảnh tải lại không khớp ảnh đã upload.')
        (OUT / ('retrieved_snapshot' + suffix)).write_bytes(check.content)
        record['download_verified'] = True
        record_file.write_text(json.dumps(record, indent=2), encoding='utf-8')
        print('GET ảnh thành công; SHA256 khớp. Case05 sẽ đọc snapshot.json này.')

    finally:
        client.disconnect()
        client.loop_stop()
        socket.getaddrinfo = original_dns


if __name__ == "__main__":
    main()
