"""06 - Nghe event camera và đầu ra SOP; không publish lệnh thiết bị.
Luồng chạy: sửa topic thiết bị test -> subscribe -> in bản tin -> tự đối chiếu history/dispatch log.
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

SECONDS = 120
# Lấy topic chính xác từ SOP/thiết bị TEST, ví dụ smart_vms/iot/ID_TEST/command.
SOP_TOPICS = []


def main():
    if not CAMERA or not CAMERA_ID:
        raise ValueError('Điền camera code/UUID trong .env.')
    if any('+' in topic or '#' in topic for topic in SOP_TOPICS):
        raise ValueError('Dùng topic của thiết bị TEST cụ thể trong SOP_TOPICS.')
    event_topic = 'smart_vms/ai_events/' + MODULE
    subscriptions = [(event_topic, 1)] + [(topic, 1) for topic in SOP_TOPICS]
    subscribed = threading.Event()
    counts = {'camera_events': 0, 'sop_commands': 0}

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
            c.subscribe(subscriptions)

    def on_subscribe(c, userdata, mid, reasons, properties):
        print('SUBACK:', mid, reasons)
        if not any(reason.is_failure for reason in reasons):
            subscribed.set()

    def on_message(c, userdata, msg):
        try:
            data = json.loads(msg.payload)
            if msg.topic == event_topic:
                if data.get('camera_id') != CAMERA_ID and data.get('camera_code') != CAMERA:
                    return
                counts['camera_events'] += 1
                print('EVENT TRÊN BROKER:', data.get('entity_id'), '; chưa phải ACK backend.')
            else:
                counts['sop_commands'] += 1
                print('SOP OUTPUT:', msg.topic, json.dumps(data, ensure_ascii=False))
                print('Đối chiếu incident_key/camera với lịch sử SOP và alert_dispatch_log.')
        except (ValueError, AttributeError, TypeError):
            print('Bỏ qua bản tin không phải JSON object:', msg.topic)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    try:
        client.connect(HOST, 443, keepalive=30)
        client.loop_start()
        if not subscribed.wait(10):
            raise RuntimeError('Chưa nhận SUBACK thành công.')
        print('Chạy case05 ở terminal khác sau khi bật SEND = True trong file05.')
        if not SOP_TOPICS:
            print('SOP_TOPICS đang rỗng: chỉ nghe event camera; SOP chỉ ghi log cần kiểm backend.')
        time.sleep(SECONDS)
        print('RESULT:', counts, '; chưa tự xác minh SOP đã thực thi thành công.')
        OUT.mkdir(exist_ok=True)
        (OUT / 'last_result.json').write_text(json.dumps(counts, indent=2), encoding='utf-8')

    finally:
        client.disconnect()
        client.loop_stop()
        socket.getaddrinfo = original_dns


if __name__ == "__main__":
    main()
