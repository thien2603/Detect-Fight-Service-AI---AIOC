"""02 - Nghe polygon/lines và báo khi bạn sửa vùng trên VMS.
Luồng chạy: đọc .env -> nghe hai topic -> kiểm JSON -> so sánh dữ liệu -> in/lưu -> dừng.
Chạy: python run.py. Sửa các biến ở đầu file; không dùng tham số --seconds/--send.
"""
import json
from pathlib import Path
import socket
import ssl
import threading
import time
import uuid

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

SECONDS = 300  # Sửa thời gian quan sát ở đây, tối đa nên dùng vài phút.


def main():
    if not CAMERA or not CAMERA_ID:
        raise ValueError('Điền đúng camera code và UUID trong .env trước.')
    OUT.mkdir(exist_ok=True)
    topic = f'smart_vms/cameras/{CAMERA}/ai_zones/{MODULE}'
    previous = {}  # Lưu danh sách trước đó theo từng topic để so sánh.
    counts = {'messages': 0, 'changes': 0, 'empty': 0, 'invalid': 0}
    subscribed = threading.Event()

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

    def on_connect(c, userdata, flags, reason, properties):
        print('CONNECTED:', reason)
        if reason == 0:
            c.subscribe([(topic, 1), (topic + '/lines', 1)])

    def on_subscribe(c, userdata, mid, reasons, properties):
        print('SUBACK:', mid, reasons)
        if not any(reason.is_failure for reason in reasons):
            subscribed.set()

    def on_message(c, userdata, msg):
        try:
            field = 'lines' if msg.topic.endswith('/lines') else 'ai_zones'
            data = json.loads(msg.payload) if msg.payload else {field: []}
            if not isinstance(data, dict) or not isinstance(data.get(field), list):
                raise ValueError('JSON phải có danh sách ' + field)
            if data.get('camera_id', CAMERA_ID) != CAMERA_ID or data.get('camera_code', CAMERA) != CAMERA:
                raise ValueError('Cấu hình của camera khác')
            items = data[field]
            for zone in items:
                points = (zone.get('line') or {}).get('points') if field == 'lines' else zone.get('points')
                if not isinstance(points, list) or len(points) < (2 if field == 'lines' else 3):
                    raise ValueError('Vùng/vạch không đủ điểm')
                for point in points:
                    if not isinstance(point, dict) or any(isinstance(point.get(k), bool) or not isinstance(point.get(k), (int, float)) or not 0 <= point[k] <= 1 for k in ('x', 'y')):
                        raise ValueError('Tọa độ phải là số trong 0..1')

            # Kiểm JSON hữu hạn trước khi thay bản ghi đang dùng.
            text = json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)
            # Python so sánh trực tiếp list/dict; không cần tạo chữ ký JSON.
            changed = msg.topic in previous and previous[msg.topic] != items
            previous[msg.topic] = items
            counts['messages'] += 1
            counts['changes'] += int(changed)
            counts['empty'] += int(not items)
            filename = 'lines.json' if field == 'lines' else 'polygons.json'
            (OUT / filename).write_text(text, encoding='utf-8')
            print(json.dumps({'topic': msg.topic, 'retained': msg.retain,
                              'changed': changed, 'empty': not items, 'payload': data}, ensure_ascii=False))
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            counts['invalid'] += 1
            print('GEOMETRY INVALID:', error)  # Không ghi đè file tốt bằng dữ liệu sai.

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    try:
        client.connect(HOST, 443, keepalive=30)
        client.loop_start()
        if not subscribed.wait(10):
            raise RuntimeError('Chưa nhận SUBACK thành công.')
        print('Bây giờ sửa polygon đúng camera/module trên VMS rồi bấm Lưu.')
        time.sleep(SECONDS)
        print('RESULT:', counts)
        (OUT / 'last_result.json').write_text(json.dumps(counts, indent=2), encoding='utf-8')

    finally:
        client.disconnect()
        client.loop_stop()
        socket.getaddrinfo = original_dns


if __name__ == "__main__":
    main()
