"""05 - Gửi đúng MỘT event MẪU, có thể tạo history/notification/SOP thật.
Luồng chạy: đọc snapshot case04 -> dựng/print event -> SEND=True kiểm camera -> publish một lần.
Chạy: python run.py. Sửa các biến ở đầu file; không dùng tham số --seconds/--send.
"""
import json
from pathlib import Path
import socket
import ssl
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
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

SEND = False  # Chỉ đổi True khi muốn tạo một sự kiện test thật.


def main():

    if not CAMERA or not CAMERA_ID or not ORGANIZATION:
        raise ValueError('Chạy case01 và điền CAMERA_CODE, CAMERA_ID, ORGANIZATION_ID trong .env trước.')

    # Dùng ảnh đã upload ở case04; không tự upload lại trong case này.
    snapshot_file = SUITE / '04_snapshot/output/snapshot.json'
    if not snapshot_file.exists():
        raise ValueError('Chưa có ảnh. Chạy case04 với SEND = True trước.')
    snapshot = json.loads(snapshot_file.read_text(encoding='utf-8'))
    if snapshot.get('camera_id') != CAMERA_ID or snapshot.get('module') != MODULE:
        raise ValueError('Ảnh của camera/module khác; chạy lại case04 theo đúng .env.')
    if not snapshot.get('object_key'):
        raise ValueError('Snapshot chưa có object_key hợp lệ.')

    # Event MẪU để test backend, không phải kết quả model phát hiện thật.
    event = {
        'ai_modules': MODULE, 'camera_id': CAMERA_ID, 'camera_code': CAMERA,
        'event_time': datetime.now(timezone(timedelta(hours=7))).isoformat(),
        'entity_id': 'mqtt_test_' + uuid.uuid4().hex,
        'confidence': 0.99, 'severity': 'HIGH',
    }
    if MODULE == 'FIGHT_DETECTION':
        event.update(submodule_code=MODULE, entity_type='PERSON',
                     payload={'type': 'FIGHT', 'person_count': 2, 'test_source': 'manual_backend_test'})
    elif MODULE == 'PLATE':
        event.update(entity_type='VEHICLE', payload={
            'direction': 'IN', 'confidence': 0.99,
            'vehicle': {'license_plate': {'text': 'TEST0001', 'status': 'DETECTED'}}})
    else:
        raise ValueError('Bộ test này dùng FIGHT_DETECTION hoặc PLATE.')

    event['snapshot_url'] = snapshot['object_key']

    OUT.mkdir(exist_ok=True)
    (OUT / 'last_event.json').write_text(json.dumps(event, ensure_ascii=False, indent=2), encoding='utf-8')
    print('EVENT MẪU:', json.dumps(event, ensure_ascii=False, indent=2))
    if not SEND:
        print('Chỉ xem trước. Đổi SEND = True ở đầu file để gửi thật.')
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

        topic = 'smart_vms/ai_events/' + MODULE
        info = client.publish(topic, json.dumps(event), qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError('Publish không được enqueue; không tự retry.')
        info.wait_for_publish(timeout=8)
        if not info.is_published():
            raise RuntimeError('Chưa rõ PUBACK; không tự gửi lại để tránh trùng.')
        print('PUBACK:', info.mid)
        print('Đối chiếu lịch sử VMS bằng camera, event_time và entity_id:', event['entity_id'])
        print('PUBACK chỉ là broker nhận tin; phải mở lịch sử/ảnh và kiểm SOP riêng.')
        (OUT / 'last_result.json').write_text(json.dumps({
            'status': 'broker_puback', 'mqtt_mid': info.mid, 'entity_id': event['entity_id'],
            'event_time': event['event_time'], 'backend_saved': 'manual_check_required'}, indent=2), encoding='utf-8')

    finally:
        client.disconnect()
        client.loop_stop()
        socket.getaddrinfo = original_dns


if __name__ == "__main__":
    main()
