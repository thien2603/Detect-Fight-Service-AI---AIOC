"""Quan sát polygon/lines và sửa vùng trên VMS; không sửa cấu hình qua MQTT.
Luồng: đọc cấu hình camera -> subscribe hai topic module -> nhận/kiểm payload;
so sánh chữ ký cấu hình -> báo thay đổi hoặc payload rỗng -> dừng sau --seconds.
Bạn tự sửa vùng test trên VMS trong lúc script quan sát. Không restart worker.

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


def publish_once(client, topic, payload, qos=1):
    """Một lần gọi publish; ACK không được coi là ACK backend/database/SOP."""
    body = payload if isinstance(payload, (str, bytes)) else json.dumps(payload, ensure_ascii=False, allow_nan=False)
    info = client.publish(topic, body, qos=qos, retain=False)
    log('publish', topic=topic, qos=qos, retain=False, mqtt_mid=info.mid, rc=int(info.rc), payload=payload)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError('Publish không được enqueue; không tự retry')
    if qos:
        info.wait_for_publish(timeout=8)
        if not info.is_published():
            raise RuntimeError('Không rõ kết quả PUBACK; không tự retry để tránh trùng')
        log('broker_puback', topic=topic, mqtt_mid=info.mid, backend_saved='not_verified', sop_executed='not_verified')
    return info.mid


def camera_summary(cam, organization=None):
    # Không ghi URL RTSP chứa mật khẩu ra file.
    return {'id':cam.get('id'), 'code':cam.get('code'), 'zone_id':cam.get('zone_id'),
            'organization_id':cam.get('organization_id') or organization, 'status':cam.get('status'),
            'ai_modules':cam.get('ai_modules',[]), 'has_rtsp':bool(cam.get('rtsp_url') or cam.get('restream_urls'))}


def get_camera(args, enabled=None, extra_filters=None, extra_handler=None):
    """Đọc đúng camera; chỉ xin config sau SUBACK, không sửa config của VMS."""
    zone_topic = f'smart_vms/cameras/zone/{args.zone}'
    filters = [(zone_topic,1)]
    if args.organization is not None:
        filters.append((f'smart_vms/cameras/company/{args.organization}',1))
    elif getattr(args, 'discover_organization', False):
        filters.append(('smart_vms/cameras/company/+',1))
    filters += extra_filters or []
    state, guard = {'camera':None, 'rows':[], 'error':None, 'retained':None}, threading.Lock()
    def message(client, msg):
        if not msg.topic.startswith('smart_vms/cameras/') or '/ai_zones/' in msg.topic:
            if extra_handler:
                extra_handler(client, msg)
            return
        data = json.loads(msg.payload) if msg.payload else {}
        if not isinstance(data,dict) or not isinstance(data.get('cameras'),list):
            return
        organization = data.get('organization_id') or data.get('company_id')
        if '/company/' in msg.topic:
            organization = int(msg.topic.rsplit('/',1)[1])
        rows = [camera_summary(x, organization) for x in data['cameras'] if isinstance(x,dict)]
        with guard:
            if msg.topic == zone_topic:
                state['rows'] = rows
            for cam in rows:
                if cam['code'] != args.camera or cam['zone_id'] != args.zone:
                    continue
                if args.camera_id and cam['id'] != args.camera_id:
                    log('camera_uuid_mismatch', code=cam['code'], expected=args.camera_id, received=cam['id'])
                    continue
                uuid.UUID(cam['id'])
                if cam['organization_id'] is not None and args.organization is not None and cam['organization_id'] != args.organization:
                    state['error'] = 'Organization cấu hình khác organization của camera nhận được'
                    continue
                if state['camera'] and not cam['organization_id']:
                    cam['organization_id'] = state['camera'].get('organization_id')
                state.update(camera=cam, retained=msg.retain)
                log('camera_config', topic=msg.topic, retained=msg.retain, camera=cam)
    c = open_mqtt(args, filters, message)
    requested = False
    try:
        until = time.monotonic() + args.timeout
        while time.monotonic() < until:
            with guard:
                cam = copy.deepcopy(state['camera'])
                error = state['error']
            if error:
                raise ValueError(error)
            if cam and args.organization is None and cam.get('organization_id'):
                args.organization = cam['organization_id']
            if args.organization is not None and not requested:
                publish_once(c, 'smart_vms/cameras/getdevice', {'zone_id':args.zone,'organization_id':args.organization})
                requested = True
            if cam:
                active = args.module in cam['ai_modules']
                want_org = getattr(args,'discover_organization',False) and not cam.get('organization_id')
                if not want_org and (enabled is None or active == enabled):
                    args.camera_id = cam['id']
                    cam['organization_id'] = cam.get('organization_id') or args.organization
                    write_json('camera.json',cam)
                    if not requested:
                        log('retained_only', note='Chưa biết organization; chưa gửi getdevice. Dùng case01 --discover-organization nếu được phép.')
                    log('config_freshness', note='MQTT getdevice không có correlation ID; retained/live không phải bảo đảm snapshot cấu hình tức thời.')
                    return c, cam, state
            time.sleep(.1)
        raise RuntimeError('Chưa nhận đúng camera/module/organization; kiểm tra zone, UUID, quyền và cấu hình VMS')
    except Exception:
        close_mqtt(c)
        raise


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
    pass


def run_case(args):
    config_client, cam, _ = get_camera(args)
    close_mqtt(config_client)
    base = f'smart_vms/cameras/{args.camera}/ai_zones/{args.module}'
    seen, counts = {}, {'messages':0,'changes':0,'empty':0,'invalid':0}
    def received(client,msg):
        try:
            field = 'lines' if msg.topic.endswith('/lines') else 'ai_zones'
            data = json.loads(msg.payload) if msg.payload else {field:[]}
            if not isinstance(data,dict) or not isinstance(data.get(field),list):
                raise ValueError('Invalid geometry object')
            if data.get('camera_id',cam['id']) != cam['id'] or data.get('camera_code',args.camera) != args.camera:
                raise ValueError('Wrong camera identity')
            for zone in data[field]:
                if not isinstance(zone,dict):raise ValueError('Zone must be object')
                points = (zone.get('line') or {}).get('points') if field=='lines' else zone.get('points')
                if not isinstance(points,list) or len(points)<(2 if field=='lines' else 3):
                    raise ValueError('Missing points')
                for point in points:
                    if not isinstance(point,dict) or any(isinstance(point.get(k),bool) or not isinstance(point.get(k),(int,float)) or not 0<=point[k]<=1 for k in ('x','y')):
                        raise ValueError('Coordinates must be within 0..1')
            signature = json.dumps(data[field],sort_keys=True,allow_nan=False)
            changed = msg.topic in seen and seen[msg.topic] != signature
            counts['messages'] += 1
            counts['changes'] += int(changed)
            counts['empty'] += int(not data[field])
            seen[msg.topic] = signature
            write_json('lines.json' if field=='lines' else 'polygons.json',data)
            log('geometry',topic=msg.topic,retained=msg.retain,changed=changed,empty=not data[field],payload=data)
        except (ValueError,TypeError,KeyError):
            counts['invalid']+=1
            log('geometry_rejected',topic=msg.topic,note='Giữ cấu hình tốt trước đó')
    c = open_mqtt(args,[(base,1),(base+'/lines',1)],received)
    try:
        log('instructions',note='Sửa vùng test trên VMS; theo dõi changed=true, không publish config giả')
        time.sleep(args.seconds)
        write_json('last_result.json',{'status':'observed','counts':counts,'hot_change_observed':counts['changes']>0})
        log('result',**counts)
    finally:
        close_mqtt(c)


if __name__ == "__main__":
    main()
