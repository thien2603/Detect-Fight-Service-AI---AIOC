"""07 - Gửi một bản tin MẪU cố ý sai, kiểm log backend xem bị từ chối chưa.
Luồng chạy: chọn CASE -> dựng/print dữ liệu -> SEND=True kiểm camera -> publish đúng một lần.
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

SEND = False
CASE = 'invalid-type'
# Các lựa chọn: invalid-json, invalid-type, unknown-camera,
#               disabled-module, unsupported-module.


def main():

    if not CAMERA or not CAMERA_ID or not ORGANIZATION:
        raise ValueError('Chạy case01 và điền CAMERA_CODE, CAMERA_ID, ORGANIZATION_ID trong .env trước.')

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

    if CASE not in ('invalid-json', 'invalid-type', 'unknown-camera', 'disabled-module', 'unsupported-module'):
        raise ValueError('CASE không hợp lệ.')
    if CASE == 'invalid-type':
        if MODULE != 'FIGHT_DETECTION':
            raise ValueError('invalid-type này dùng cho FIGHT, không dùng PLATE.')
        event['payload']['type'] = 'INVALID_TEST_TYPE'
    elif CASE == 'unknown-camera':
        # Đổi cả hai để backend không fallback từ UUID sai về code camera thật.
        event['camera_id'] = str(uuid.uuid4())
        event['camera_code'] = 'UNKNOWN_TEST_' + uuid.uuid4().hex[:8]
    elif CASE == 'unsupported-module':
        event['ai_modules'] = 'UNSUPPORTED_TEST_MODULE'
        event['submodule_code'] = 'UNSUPPORTED_TEST_MODULE'
    topic = 'smart_vms/ai_events/' + event['ai_modules']
    body = '{invalid-json' if CASE == 'invalid-json' else json.dumps(event)
    print('CASE:', CASE, '; TOPIC:', topic, '; PAYLOAD:', body)
    if not SEND:
        print('Chỉ xem trước. Đổi SEND = True để gửi đúng một bản tin thử.')
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

        enabled = MODULE in camera.get('ai_modules', [])
        if CASE == 'disabled-module' and enabled:
            raise RuntimeError('Tự tắt module trên camera TEST, chờ đồng bộ rồi chạy lại.')
        if CASE != 'disabled-module' and not enabled:
            raise RuntimeError('Bật module trên camera TEST trước để chỉ kiểm đúng lỗi đang chọn.')
        info = client.publish(topic, body, qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError('Publish không được enqueue; không tự retry.')
        info.wait_for_publish(timeout=8)
        if not info.is_published():
            raise RuntimeError('Chưa rõ PUBACK; không tự retry.')
        print('PUBACK:', info.mid, '; kiểm log backend và lịch sử để xác nhận có bị từ chối không.')
        OUT.mkdir(exist_ok=True)
        (OUT / 'last_result.json').write_text(json.dumps({
            'case': CASE, 'mqtt_mid': info.mid, 'payload_sent': body,
            'expected': 'no_new_event', 'actual': 'manual_check_required'}, indent=2), encoding='utf-8')
        if CASE == 'disabled-module':
            print('Nhớ khôi phục module camera sau khi kiểm tra.')

    finally:
        client.disconnect()
        client.loop_stop()
        socket.getaddrinfo = original_dns


if __name__ == "__main__":
    main()
