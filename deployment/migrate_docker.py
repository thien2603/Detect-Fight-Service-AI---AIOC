"""Chuyển systemd worker sang Docker; rollback nếu không có bbox mới/health tốt.

Chạy từ venv có python-dotenv: python deployment/migrate_docker.py --old-service ioc-fight-bbox
Đọc cấu hình riêng .env. Không in mật khẩu/RTSP hoặc ghi dữ liệu nội bộ vào Git.
"""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import time
from dotenv import dotenv_values

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--old-service',required=True)
    args=parser.parse_args()
    env=dotenv_values(ROOT/'.env')
    output=Path(env.get('FIGHT_OUTPUT_DIR',str(ROOT/'output')))
    evidence=ROOT/'evidence';evidence.mkdir(exist_ok=True)
    if (output/'health.json').exists():
        (evidence/'before_docker_health.json').write_text((output/'health.json').read_text())
    def run(cmd):return subprocess.run(cmd,cwd=ROOT,check=True,capture_output=True,text=True)
    run(['docker','compose','config','--quiet'])
    started=time.time()
    run(['systemctl','--user','stop',args.old_service])
    try:
        run(['docker','compose','up','-d','--no-build'])
        success=None
        for _ in range(35):
            time.sleep(3)
            try:
                h=json.loads((output/'health.json').read_text())
                stamp=datetime.datetime.fromisoformat(h['time']).timestamp()
                if stamp>=started and time.time()-stamp<20 and h.get('client_id')==env.get('VMS_MQTT_CLIENT_ID','tts_thien_01') and h['mqtt_connected'] and h['device']=='cuda' and h.get('send_hz',0)>5:
                    success=h;break
            except (OSError,ValueError,KeyError):pass
        if success is None:
            raise RuntimeError('Docker did not produce fresh connected GPU health/bbox within timeout')
        run(['systemctl','--user','disable',args.old_service])
        status=json.loads(run(['docker','inspect','ioc-fight-service']).stdout)[0]
        record={'deployed':True,'switched_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'container_id':status['Id'],'image':status['Image'],'old_service_disabled':True,'health':success}
        (evidence/'docker_deployment.json').write_text(json.dumps(record,indent=2))
        print(json.dumps({'deployed':True,'container_id':status['Id'][:12],'device':success['device'],
                          'mqtt_connected':success['mqtt_connected'],'send_hz':success['send_hz'],
                          'client_id':success['client_id'],'polygons':success['geometry']['polygon_count'],
                          'events_enabled':success['events_enabled'],'publish_errors':success['publish_errors']}),flush=True)
    except Exception:
        subprocess.run(['docker','compose','stop'],cwd=ROOT,capture_output=True)
        subprocess.run(['systemctl','--user','start',args.old_service])
        raise


if __name__=='__main__':
    main()
