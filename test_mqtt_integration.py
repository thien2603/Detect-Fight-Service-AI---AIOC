"""Nghiệm thu C01/C02/C03 qua broker Docker CÔ LẬP trên loopback server.

Đọc RTSP camera được cấp từ IOC (chỉ subscribe). Mọi cấu hình giả, bật/tắt,
payload sai và ngắt broker đều nằm ở mosquitto test 127.0.0.1:18883, không gửi
config/event giả vào IOC. Worker chạy model thật nhưng KHÔNG bật --events.
Chạy trên Linux có Docker GPU: python test_mqtt_integration.py
"""
import json
from pathlib import Path
import socket
import ssl
import subprocess
import threading
import time
from dotenv import dotenv_values
import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parent
PURPOSE = 'fight-acceptance-mqtt'
TEST_CAMERA = 'ACCEPTANCE_CAMERA'
TEST_ID = '00000000-0000-4000-8000-000000000001'
ZONE_ID = '00000000-0000-4000-8000-000000000002'


def command(args, check=True):
    result = subprocess.run(args, capture_output=True, text=True, check=check)
    return result.stdout.strip()


def query_rtsp(env):
    # Broker thật chỉ đọc cấu hình; URL có credentials chỉ giữ trong RAM.
    original = socket.getaddrinfo
    def resolve(host, port, *args, **kwargs):
        return original(env['VMS_BROKER_IP'] if host == env['VMS_MQTT_HOST'] and env.get('VMS_BROKER_IP') else host,
                        port, *args, **kwargs)
    socket.getaddrinfo = resolve
    ready, selected = threading.Event(), []
    client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='tts_config_acceptance_01',transport='websockets')
    client.ws_set_options(path='/mqtt');client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.username_pw_set(env['VMS_MQTT_USERNAME'],env['VMS_MQTT_PASSWORD'])
    def connect(c,u,f,rc,p):
        c.subscribe('smart_vms/cameras/zone/'+env['VMS_ZONE_ID'],qos=1)
    def subscribed(c,u,mid,reasons,p):
        c.publish('smart_vms/cameras/getdevice',json.dumps({'zone_id':int(env['VMS_ZONE_ID']),
                                                        'organization_id':int(env['VMS_ORGANIZATION_ID'])}),qos=1)
    def message(c,u,m):
        try:
            data=json.loads(m.payload)
            for camera in data.get('cameras',[]):
                if camera.get('code')==env['VMS_CAMERA_CODE'] and camera.get('id')==env['VMS_CAMERA_ID']:
                    url=(camera.get('restream_urls') or {}).get('FIGHT_DETECTION') or camera.get('rtsp_url')
                    if url:selected.append(url);ready.set()
        except (ValueError,AttributeError):pass
    client.on_connect=connect;client.on_subscribe=subscribed;client.on_message=message
    try:
        client.connect(env['VMS_MQTT_HOST'],443,30);client.loop_start()
        if not ready.wait(30):raise RuntimeError('Could not read assigned RTSP config')
        return selected[-1]
    finally:
        client.disconnect();client.loop_stop();socket.getaddrinfo=original


def main():
    env=dotenv_values(ROOT/'.env')
    evidence=ROOT/'evidence/mqtt_integration';output=evidence/'worker_output'
    output.mkdir(parents=True,exist_ok=True)
    config=evidence/'mosquitto.conf'
    config.write_text('listener 1883 0.0.0.0\nallow_anonymous true\npersistence false\nlog_type error\n')
    camera={'id':TEST_ID,'code':TEST_CAMERA,'zone_id':9001,'organization_id':9001,
            'ai_modules':['FIGHT_DETECTION'],'rtsp_url':query_rtsp(env)}
    zone={'id':ZONE_ID,'name':'Acceptance polygon','is_active':True,'ai_modules':'FIGHT_DETECTION',
          'points':[{'x':0,'y':0},{'x':1,'y':0},{'x':1,'y':1},{'x':0,'y':1}]}
    line={'id':ZONE_ID,'ai_modules':'FIGHT_DETECTION','line':{'points':[{'x':0,'y':.5},{'x':1,'y':.5}]},
          'point':{'x':.5,'y':.8}}
    zone_topic=f'smart_vms/cameras/{TEST_CAMERA}/ai_zones/FIGHT_DETECTION'
    owned=[];report={'purpose':PURPOSE,'steps':[]};client=None
    counts={'requests':0,'connections':0,'bbox':0,'events':0}
    def record(name, **fields):
        row={'step':name,'passed':True,'time':time.time(),**fields}
        report['steps'].append(row);print(json.dumps(row),flush=True)
    def health():
        return json.loads((output/'health.json').read_text())
    def wait_health(predicate,timeout=45):
        until=time.monotonic()+timeout
        while time.monotonic()<until:
            try:
                h=health()
                if predicate(h):return h
            except (OSError,ValueError,KeyError):pass
            time.sleep(1)
        raise RuntimeError('Worker health predicate timed out')
    def publish_config():
        payload=json.dumps({'cameras':[camera]})
        for topic in ('smart_vms/cameras/zone/9001','smart_vms/cameras/company/9001'):
            client.publish(topic,payload,qos=1,retain=True)
    def publish_geometry(zones=None,lines=None):
        if zones is not None:client.publish(zone_topic,json.dumps({'camera_code':TEST_CAMERA,'camera_id':TEST_ID,'ai_zones':zones}),qos=1,retain=True)
        if lines is not None:client.publish(zone_topic+'/lines',json.dumps({'camera_code':TEST_CAMERA,'camera_id':TEST_ID,'lines':lines}),qos=1,retain=True)
    try:
        broker_id=command(['docker','run','-d','--name','fight-acceptance-broker','--label',f'purpose={PURPOSE}',
                           '-p','127.0.0.1:18883:1883','-v',f'{config}:/mosquitto/config/mosquitto.conf:ro',
                           '--log-opt','max-size=2m','--log-opt','max-file=1','eclipse-mosquitto:2.0.22'])
        owned.append(broker_id)
        client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='tts_backend_acceptance_01')
        client.reconnect_delay_set(1,5)
        def connect(c,u,f,rc,p):
            counts['connections']+=1
            c.subscribe([('smart_vms/cameras/getdevice',1),(f'smart_vms/ai/bbox/{TEST_CAMERA}',0),('smart_vms/ai_events/#',1)])
            publish_config();publish_geometry([zone],[line])
        def message(c,u,m):
            if m.topic=='smart_vms/cameras/getdevice':counts['requests']+=1;publish_config()
            elif m.topic.startswith('smart_vms/ai/bbox/'):counts['bbox']+=1
            elif m.topic.startswith('smart_vms/ai_events/'):counts['events']+=1
        client.on_connect=connect;client.on_message=message
        for attempt in range(20):
            try:client.connect('127.0.0.1',18883,10);break
            except OSError:time.sleep(.5)
        client.loop_start();time.sleep(1)
        weights=env.get('FIGHT_WEIGHTS_DIR',str(ROOT/'weights'))
        worker_id=command(['docker','run','-d','--name','fight-acceptance-worker','--label',f'purpose={PURPOSE}',
                           '--gpus','all','--network','host','--env-file',str(ROOT/'.env'),
                           '-e','VMS_BROKER_IP=','-e','VMS_MQTT_CLIENT_ID=tts_thien_acceptance_01',
                           '-v',f'{weights}:/app/weights:ro','-v',f'{output}:/app/output',
                           '--log-opt','max-size=2m','--log-opt','max-file=1','fight-service:local',
                           '--mqtt-transport','tcp','--broker-host','127.0.0.1','--broker-port','18883',
                           '--zone','9001','--organization','9001','--camera',TEST_CAMERA,'--camera-id',TEST_ID,'--seconds','300'])
        owned.append(worker_id)
        initial=wait_health(lambda h:h['mqtt_connected'] and h['enabled'] and h['bbox_sent']>10 and h['geometry']['polygon_count']==1 and h['geometry']['line_count']==1,90)
        started=json.loads(command(['docker','inspect',worker_id]))[0]['State']['StartedAt']
        record('initial_retained_and_real_model',device=initial['device'],geometry=initial['geometry']['version'],bbox_sent=initial['bbox_sent'])
        before=initial['geometry']['version']
        zone['points'][1]['x']=.7;zone['points'][2]['x']=.7
        publish_geometry([zone],None)
        updated=wait_health(lambda h:h['geometry']['version']>before)
        record('hot_polygon_update',version=updated['geometry']['version'])
        publish_geometry([],[])
        cleared=wait_health(lambda h:h['geometry']['polygon_count']==0 and h['geometry']['line_count']==0 and h['enabled'])
        record('empty_geometry_cleared_and_camera_reconciled',enabled=cleared['enabled'])
        client.publish(zone_topic,b'',qos=1,retain=True)
        wait_health(lambda h:h['enabled'] and h['mqtt_connected'])
        client.publish(zone_topic,'not-json',qos=1,retain=False)
        time.sleep(3)
        assert health()['mqtt_connected']
        record('tombstone_and_invalid_json_no_crash')
        camera['ai_modules']=[];publish_config();publish_geometry([],[])
        disabled=wait_health(lambda h:not h['enabled'])
        sent=disabled['bbox_sent'];detected=disabled['detections_completed']
        time.sleep(12)
        paused=health()
        assert paused['bbox_sent']==sent and paused['detections_completed']==detected, (sent,paused['bbox_sent'])
        record('module_off_stops_inference_and_publish',bbox_sent=sent,detections=detected)
        camera['ai_modules']=['FIGHT_DETECTION'];publish_config();publish_geometry([zone],[line])
        resumed=wait_health(lambda h:h['enabled'] and h['bbox_sent']>sent+10)
        record('module_on_resumes',bbox_sent=resumed['bbox_sent'])
        before_requests=counts['requests']
        command(['docker','stop','-t','3',broker_id])
        wait_health(lambda h:not h['mqtt_connected'],25)
        command(['docker','start',broker_id])
        after=wait_health(lambda h:h['mqtt_connected'] and h['bbox_sent']>resumed['bbox_sent']+10,65)
        assert counts['requests']>before_requests,counts
        inspect=json.loads(command(['docker','inspect',worker_id]))[0]
        assert inspect['State']['StartedAt']==started and inspect['RestartCount']==0
        assert counts['events']==0
        record('broker_restart_reconnect_resubscribe_request_without_worker_restart',requests=counts['requests'],connections=counts['connections'],restart_count=inspect['RestartCount'])
        report.update(passed=True,counts=counts,worker_started_at=started,events_enabled=False)
    except Exception as exc:
        report.update(passed=False,error_type=type(exc).__name__,error=str(exc)[:200])
        raise
    finally:
        if client:client.disconnect();client.loop_stop()
        (evidence/'result.json').write_text(json.dumps(report,indent=2))
        for container_id in reversed(owned):
            data=json.loads(command(['docker','inspect',container_id]))[0]
            if data['Config']['Labels'].get('purpose')==PURPOSE:
                command(['docker','stop','-t','10',container_id],check=False)
                command(['docker','rm',container_id],check=False)


if __name__=='__main__':
    main()
