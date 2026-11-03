"""03 - Gửi bbox MẪU lên giao diện VMS, không chạy model hoặc tạo event.
Luồng chạy: dựng payload -> print -> SEND=True mới kiểm camera và gửi bbox hữu hạn.
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

SEND = False  # Đổi True để gửi bbox mẫu thật.
SECONDS = 10
FPS = 5


def main():

    if not CAMERA or not CAMERA_ID or not ORGANIZATION:
        raise ValueError('Chạy case01 và điền CAMERA_CODE, CAMERA_ID, ORGANIZATION_ID trong .env trước.')

    if not 0 < FPS <= 15 or not 0 < SECONDS <= 300:
        raise ValueError('FPS phải trong (0,15], SECONDS trong (0,300].')
    topic = f'smart_vms/ai/bbox/{CAMERA}'
    payload = {'camera_id': CAMERA_ID, 'camera_code': CAMERA,
               'organization_id': ORGANIZATION, 'ai_modules': MODULE, 'timestamp': time.time(),
               'detections': [{'id': 'synthetic_test_box', 'class': 'person' if MODULE == 'FIGHT_DETECTION' else 'vehicle',
                               'confidence': 1.0, 'bbox': [0.1, 0.15, 0.45, 0.85]}],
               'hud': {'text': 'MQTT TEST BBOX - synthetic', 'display_ms': 500, 'color': '#f59e0b'}}
    print('TOPIC:', topic)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not SEND:
        print('Chỉ xem trước. Đổi SEND = True để gửi bbox mẫu.')
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

        print('Mở VMS, chọn đúng camera/module và bật Phát hiện AI.')
        end = time.monotonic() + SECONDS
        sent = 0
        while time.monotonic() < end:
            if MODULE not in camera.get('ai_modules', []):
                print('Module đã tắt; dừng gửi bbox.')
                break
            payload['timestamp'] = time.time()
            info = client.publish(topic, json.dumps(payload), qos=0, retain=False)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError('Không gửi được bbox.')
            sent += 1
            print('BBOX SENT:', sent, 'mid:', info.mid)  # QoS0 không có PUBACK.
            time.sleep(1 / FPS)
        print('Tổng bbox mẫu đã enqueue:', sent, '; tự kiểm tra khung trên VMS.')

    finally:
        client.disconnect()
        client.loop_stop()
        socket.getaddrinfo = original_dns


if __name__ == "__main__":
    main()
