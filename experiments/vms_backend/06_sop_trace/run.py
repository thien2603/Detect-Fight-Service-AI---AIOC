"""Nghe đầu ra SOP và event test; KHÔNG publish event hoặc lệnh thiết bị.
Luồng: đọc camera/CLI -> subscribe event module và các topic SOP chính xác
bạn lấy từ rule test -> chờ -> log entity_id/incident_key -> tổng hợp quan sát.
Topic SOP không tự chứng minh rule chạy xong; cần history SOP/alert_dispatch_log.
Mở case này trước, rồi chạy case05 --send ở terminal khác. Không tự tạo/sửa SOP.

File này độc lập: toàn bộ chương trình ở ngay đây, không gọi common.py/run(i).
"""

import argparse
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import socket
import ssl
import threading
import time
import uuid

from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import requests

# Mỗi case ghi output riêng; log tối đa 3 file x 2 MiB, JSON dùng tên cố định.
HERE = Path(__file__).resolve().parent
SUITE = HERE.parent
OUT = HERE / 'output'
LOGGER = logging.getLogger('vms_backend.' + HERE.name)


def safe(value):
    """Che user/password trong URL trước khi ghi log; không lưu RTSP thô."""
    if isinstance(value, dict):
        return {k: ('***' if k.lower() in {'password', 'username', 'authorization'} else safe(v)) for k,v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe(v) for v in value]
    if isinstance(value, str):
        return re.sub(r'(rtsps?://)[^/\s]*@', r'\1***@', value)
    return value


def log(kind, **fields):
    row = {'time': datetime.now(timezone.utc).isoformat(), 'kind': kind, **safe(fields)}
    line = json.dumps(row, ensure_ascii=False, allow_nan=False)
    print(line, flush=True)
    if LOGGER.handlers:
        LOGGER.info(line)


def write_json(name, data):
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / name
    temporary = target.with_suffix(target.suffix + '.tmp')
    temporary.write_text(json.dumps(safe(data), ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    temporary.replace(target)


def integer_env(name):
    value = os.getenv(name, '').strip()
    return int(value) if value else None


def parse_args():
    # Đọc .env cạnh README của suite; biến môi trường/CLI được ưu tiên hơn file.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument('--env-file', type=Path, default=SUITE / '.env')
    early, _ = pre.parse_known_args()
    load_dotenv(early.env_file, override=False)
    p = argparse.ArgumentParser(description=__doc__, parents=[pre])
    p.add_argument('--host', default=os.getenv('VMS_MQTT_HOST', 'aioc-stg.atin.vn'))
    p.add_argument('--broker-ip', default=os.getenv('VMS_BROKER_IP') or None)
    p.add_argument('--zone', type=int, default=integer_env('VMS_ZONE_ID'))
    p.add_argument('--organization', type=int, default=integer_env('VMS_ORGANIZATION_ID'))
    p.add_argument('--camera', default=os.getenv('VMS_CAMERA_CODE') or None)
    p.add_argument('--camera-id', default=os.getenv('VMS_CAMERA_ID') or None)
    p.add_argument('--module', choices=['FIGHT_DETECTION', 'PLATE'], default=os.getenv('VMS_MODULE', 'FIGHT_DETECTION'))
    p.add_argument('--timeout', type=float, default=20, help='Giới hạn chờ kết nối/cấu hình, giây')
    p.add_argument('--seconds', type=float, default=20, help='Thời gian quan sát/gửi bbox hữu hạn')
    p.add_argument('--send', action='store_true', help='Cho phép ghi thật: bbox/upload/event theo case; không tự retry event')
    add_case_arguments(p)
    args = p.parse_args()
    if args.zone is None or not args.camera:
        p.error('Điền VMS_ZONE_ID và VMS_CAMERA_CODE trong .env hoặc --zone/--camera')
    if args.zone < 0 or any(x in args.camera for x in '/#+'):
        p.error('Zone phải >=0; camera không chứa / # +')
    if args.organization is not None and args.organization <= 0:
        p.error('Organization phải là số dương khi được cung cấp')
    if not 0 < args.timeout <= 120 or not 0 < args.seconds <= 300:
        p.error('timeout thuộc (0,120]; seconds thuộc (0,300]')
    if args.camera_id:
        uuid.UUID(args.camera_id)
    if args.broker_ip:
        ipaddress.ip_address(args.broker_ip)
    # Cache của case01 chỉ hỗ trợ preview/organization; --send luôn kiểm camera qua MQTT.
    cached = SUITE / '01_camera_config/output/camera.json'
    if cached.exists():
        data = json.loads(cached.read_text(encoding='utf-8'))
        if data.get('code') == args.camera and data.get('zone_id') == args.zone and (not args.camera_id or data.get('id') == args.camera_id):
            args.camera_id = args.camera_id or data.get('id')
            args.organization = args.organization or data.get('organization_id')
    return args


def route_broker(args):
    # Chỉ đổi DNS trong process test, giữ hostname để TLS/SNI/HTTP kiểm chứng chỉ.
    if args.broker_ip:
        original = socket.getaddrinfo
        def resolve(host, port, *values, **options):
            return original(args.broker_ip if host == args.host else host, port, *values, **options)
        socket.getaddrinfo = resolve


def open_mqtt(args, subscriptions, handler):
    """CONNACK -> SUBSCRIBE -> SUBACK; callback lỗi không giết network thread."""
    user, password = os.getenv('VMS_MQTT_USERNAME'), os.getenv('VMS_MQTT_PASSWORD')
    if not user or not password:
        raise ValueError('Thiếu VMS_MQTT_USERNAME/VMS_MQTT_PASSWORD trong .env')
    online, subscribed = threading.Event(), threading.Event()
    errors = []
    # ID riêng cho mỗi lần thử, không đụng ID worker production.
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='tts_btest_' + uuid.uuid4().hex[:8] + '_01', transport='websockets')
    c.ws_set_options(path='/mqtt')
    c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    c.username_pw_set(user, password)
    c.reconnect_delay_set(1, 5)
    c.max_queued_messages_set(5)
    def on_connect(client, userdata, flags, reason, properties):
        log('on_connect', reason=str(reason))
        if reason.is_failure:
            errors.append(str(reason))
        else:
            rc, mid = client.subscribe(subscriptions)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                errors.append('subscribe_queue_failed')
        online.set()
    def on_subscribe(client, userdata, mid, reasons, properties):
        log('on_subscribe', mid=mid, granted=[str(r) for r in reasons])
        if any(r.is_failure for r in reasons):
            errors.append('subscription_rejected')
        subscribed.set()
    def on_message(client, userdata, message):
        try:
            handler(client, message)
        except Exception as exc:
            errors.append('callback_' + type(exc).__name__)
            log('callback_error', topic=message.topic, error_type=type(exc).__name__)
    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    c.on_message = on_message
    c.on_publish = lambda client,u,mid,reason,props: log('on_publish', mid=mid, reason=str(reason))
    c.on_disconnect = lambda client,u,flags,reason,props: log('on_disconnect', reason=str(reason))
    try:
        c.connect_async(args.host, 443, 30)
        c.loop_start()
        if not online.wait(args.timeout) or errors:
            raise RuntimeError('MQTT CONNACK timeout/rejected: ' + ','.join(errors))
        if not subscribed.wait(args.timeout) or errors:
            raise RuntimeError('MQTT SUBACK timeout/rejected: ' + ','.join(errors))
        return c
    except Exception:
        c.disconnect()
        c.loop_stop()
        raise


def close_mqtt(client):
    client.disconnect()
    client.loop_stop()


def preview_camera(args):
    if not args.camera_id:
        raise ValueError('Chạy case01 trước hoặc điền VMS_CAMERA_ID; không tự đoán UUID')
    return {'id':args.camera_id,'code':args.camera,'zone_id':args.zone,'organization_id':args.organization}


def main():
    args = parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    LOGGER.setLevel(logging.INFO)
    handler = RotatingFileHandler(OUT/'log.jsonl',maxBytes=2*1024*1024,backupCount=2,encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(message)s'))
    LOGGER.addHandler(handler)
    try:
        route_broker(args)
        return run_case(args)
    except Exception as exc:
        # Không ghi exception text mạng có thể chứa thông tin đăng nhập.
        reason = str(exc) if isinstance(exc,(ValueError,RuntimeError)) else type(exc).__name__
        log('test_error',error_type=type(exc).__name__,reason=reason)
        write_json('last_result.json',{'status':'error','error_type':type(exc).__name__,'reason':reason})
        raise SystemExit(1)
    finally:
        handler.close()
        LOGGER.removeHandler(handler)

def add_case_arguments(p):
    p.add_argument('--sop-topic',action='append',default=[],help='Topic đầu ra chính xác của thiết bị/loa TEST trong SOP; có thể truyền nhiều lần')
    p.add_argument('--incident-key',help='Nếu đã biết, lọc đúng incident_key này')
    p.add_argument('--backend-log-file',type=Path,help='Log backend đã xuất từ đúng host, tùy chọn')


def run_case(args):
    if args.send:raise ValueError('Case06 chỉ quan sát; dùng case05 để phát một event test')
    cam=preview_camera(args)
    event_topic='smart_vms/ai_events/'+args.module
    for topic in args.sop_topic:
        if any(x in topic for x in '+#') or not topic.startswith(('smart_vms/iot/','smart_vms/barrier/','kcn/speaker/device/','kcn/speaker/broadcast/')):
            raise ValueError('--sop-topic phải là topic đầu ra SOP TEST cụ thể, không wildcard')
    counts={'camera_event_messages':0,'sop_command_messages':0,'confirmed_correlation':0}
    def received(client,msg):
        data=json.loads(msg.payload)
        if not isinstance(data,dict):return
        if msg.topic==event_topic:
            if data.get('camera_id')!=cam['id'] and data.get('camera_code')!=cam['code']:return
            counts['camera_event_messages']+=1
            log('event_wire_observed',topic=msg.topic,entity_id=data.get('entity_id'),backend_saved='not_verified')
            return
        if args.incident_key and data.get('incident_key')!=args.incident_key:return
        counts['sop_command_messages']+=1
        matched=bool(args.incident_key and data.get('incident_key')==args.incident_key) or data.get('camera_code')==cam['code']
        counts['confirmed_correlation']+=int(matched)
        log('sop_output_observed',topic=msg.topic,retained=msg.retain,payload=data,
            correlation='camera_or_incident_match' if matched else 'needs_backend_log_verification')
    c=open_mqtt(args,[(event_topic,1)]+[(topic,1) for topic in args.sop_topic],received)
    try:
        log('instructions',note='Chạy case05 --send ở terminal khác; kiểm rule/SOP và dispatch log trên VMS backend',
            scoped_sop_topics=args.sop_topic,device_commands_published=0)
        if not args.sop_topic:log('no_sop_target',note='Chỉ nghe event camera; thêm --sop-topic lấy từ rule test để quan sát command')
        time.sleep(args.seconds)
        if args.backend_log_file:
            lines=args.backend_log_file.read_text(encoding='utf-8',errors='replace').splitlines()
            matching=[line for line in lines if args.camera in line or (args.incident_key and args.incident_key in line)]
            log('backend_log_matches',count=len(matching),last_lines=matching[-20:])
        result={'status':'observed',**counts,'sop_execution_verified':'manual_history_and_dispatch_log_required',
                'device_commands_published':0,'event_publish_calls':0}
        write_json('last_result.json',result)
        log('result',**result)
    finally:
        close_mqtt(c)


if __name__ == "__main__":
    main()
