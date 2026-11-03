"""01 - Xem camera và tìm organization. Chỉ đọc cấu hình.
Luồng chạy: đọc .env -> kết nối -> subscribe -> xin camera -> in/lưu camera được chọn.
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

SECONDS = 10  # Thời gian nhận danh sách camera.


def main():
    if not CAMERA:
        raise ValueError('Điền VMS_CAMERA_CODE trong .env.')
    OUT.mkdir(exist_ok=True)
    selected = {}
    # Nếu chưa biết organization, đọc company/+ và lấy số từ topic của đúng camera.
    company_topic = f'smart_vms/cameras/company/{ORGANIZATION}' if ORGANIZATION else 'smart_vms/cameras/company/+'

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
            c.subscribe([(f'smart_vms/cameras/zone/{ZONE}', 1), (company_topic, 1)])

    def on_subscribe(c, userdata, mid, reasons, properties):
        print('SUBACK:', mid, reasons)
        if ORGANIZATION and not any(reason.is_failure for reason in reasons):
            c.publish('smart_vms/cameras/getdevice',
                      json.dumps({'zone_id': ZONE, 'organization_id': ORGANIZATION}),
                      qos=1, retain=False)

    def on_message(c, userdata, msg):
        try:
            data = json.loads(msg.payload)
            organization = data.get('organization_id') or data.get('company_id')
            if '/company/' in msg.topic:
                organization = int(msg.topic.rsplit('/', 1)[1])
            rows = []
            for item in data.get('cameras', []):
                # Chỉ lưu thông tin cần học; bỏ URL RTSP chứa mật khẩu.
                row = {key: item.get(key) for key in ('id', 'code', 'zone_id', 'status', 'ai_modules')}
                row['organization_id'] = organization
                rows.append(row)
                if item.get('code') == CAMERA and item.get('zone_id') == ZONE:
                    if CAMERA_ID and item.get('id') != CAMERA_ID:
                        print('UUID khác .env:', item.get('id'))
                        continue
                    row['organization_id'] = organization or selected.get('organization_id') or ORGANIZATION or None
                    selected.update(row)
                    print('CAMERA:', json.dumps(selected, ensure_ascii=False))
                    (OUT / 'camera.json').write_text(json.dumps(selected, ensure_ascii=False, indent=2), encoding='utf-8')
            if msg.topic == f'smart_vms/cameras/zone/{ZONE}':
                (OUT / 'cameras.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
        except (ValueError, AttributeError, TypeError):
            print('Bỏ qua bản tin không hợp lệ:', msg.topic)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    try:
        client.connect(HOST, 443, keepalive=30)
        client.loop_start()
        time.sleep(SECONDS)  # MQTT vẫn nhận tin trong thread của loop_start().
        if not selected:
            raise RuntimeError('Không thấy camera đúng code/zone/UUID trong thời gian chờ.')
        print('Đối chiếu camera.json rồi điền UUID/organization vào .env nếu còn thiếu.')

    finally:
        client.disconnect()
        client.loop_stop()
        socket.getaddrinfo = original_dns


if __name__ == "__main__":
    main()
