"""Kiểm tra hành vi ghi mạng của bộ test; không truy cập broker hoặc Storage.

Các UUID/zone dưới đây là dữ liệu giả dành riêng cho unittest. Chạy từ folder
suite: python -m unittest -v test_safety. Không phải kết quả nghiệm thu VMS.
"""
import argparse
from contextlib import redirect_stdout
from datetime import datetime
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent
TEMP = ROOT/'output/unittest'
TEMP.mkdir(parents=True,exist_ok=True)
CAMERA_ID = '00000000-0000-4000-8000-000000000001'


def load(folder):
    spec = importlib.util.spec_from_file_location('test_' + folder, ROOT/folder/'run.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def arguments(**changes):
    values = dict(camera='UNIT_CAMERA',camera_id=CAMERA_ID,zone=9001,organization=9001,
                  module='FIGHT_DETECTION',host='unit.invalid',broker_ip=None,
                  timeout=1,seconds=.01,send=False,hz=5,gap=.1,
                  object_key='anomaly/unit_test.jpg',snapshot_record=Path('not_used.json'),
                  person_count=2,plate='TEST0001',case='invalid-json',sop_topic=[],
                  incident_key=None,backend_log_file=None)
    values.update(changes)
    return argparse.Namespace(**values)


def camera(args):
    return {'id':args.camera_id,'code':args.camera,'zone_id':args.zone,
            'organization_id':args.organization,'ai_modules':[args.module]}


class SafetyTests(unittest.TestCase):
    def invoke(self, module, args):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory, patch.object(module,'OUT',Path(directory)), redirect_stdout(io.StringIO()):
            return module.run_case(args)

    def test_write_cases_preview_never_connect_or_publish(self):
        for name in ['03_bbox','05_event_history','07_rejection','08_duplicate']:
            with self.subTest(case=name):
                module=load(name)
                with patch.object(module,'open_mqtt') as open_client, patch.object(module,'get_camera') as get_cam, patch.object(module,'publish_once') as publish:
                    self.invoke(module,arguments())
                    open_client.assert_not_called()
                    get_cam.assert_not_called()
                    publish.assert_not_called()

    def test_snapshot_preview_reads_image_without_http_or_mqtt(self):
        module=load('04_snapshot')
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path=Path(directory)/'unit.jpg'
            path.write_bytes(b'\xff\xd8\xff' + b'unit-test')
            with patch.object(module,'get_camera') as get_cam, patch.object(module.requests,'Session') as session:
                self.invoke(module,arguments(image=path))
                get_cam.assert_not_called()
                session.assert_not_called()

    def test_snapshot_same_camera_but_wrong_module_is_rejected(self):
        module=load('05_event_history')
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path=Path(directory)/'snapshot.json'
            path.write_text(json.dumps({'camera_id':CAMERA_ID,'module':'PLATE','object_key':'plate/unit.jpg'}))
            args=arguments(object_key=None,snapshot_record=path)
            with self.assertRaisesRegex(ValueError,'module khác'):
                module.build_event(args,camera(args))

    def test_snapshot_wrong_camera_is_rejected(self):
        module=load('05_event_history')
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path=Path(directory)/'snapshot.json'
            path.write_text(json.dumps({'camera_id':'00000000-0000-4000-8000-000000000002','module':'FIGHT_DETECTION','object_key':'anomaly/unit.jpg'}))
            args=arguments(object_key=None,snapshot_record=path)
            with self.assertRaisesRegex(ValueError,'camera khác'):
                module.build_event(args,camera(args))

    def test_unknown_camera_changes_id_and_code_and_preserves_baseline(self):
        module=load('07_rejection');args=arguments(case='unknown-camera')
        original=module.build_event(args,camera(args))
        bad=module.rejection_payload(args,original)
        self.assertNotEqual(bad['camera_id'],original['camera_id'])
        self.assertNotEqual(bad['camera_code'],original['camera_code'])
        self.assertEqual(original['camera_id'],CAMERA_ID)

    def test_invalid_type_cannot_be_claimed_for_plate(self):
        module=load('07_rejection');args=arguments(module='PLATE',case='invalid-type',object_key='plate/unit.jpg')
        with self.assertRaisesRegex(ValueError,'chỉ áp mẫu FIGHT'):
            module.rejection_payload(args,module.build_event(args,camera(args)))

    def test_qos1_unknown_outcome_has_one_publish_and_no_retry(self):
        module=load('05_event_history')
        client=Mock();info=Mock(rc=0,mid=7)
        info.is_published.return_value=False
        client.publish.return_value=info
        with redirect_stdout(io.StringIO()),self.assertRaisesRegex(RuntimeError,'không tự retry'):
            module.publish_once(client,'smart_vms/ai_events/FIGHT_DETECTION',{'test':'unit'})
        self.assertEqual(client.publish.call_count,1)
        self.assertFalse(client.publish.call_args.kwargs['retain'])

    def test_sop_trace_rejects_send_and_never_publishes(self):
        module=load('06_sop_trace')
        with patch.object(module,'open_mqtt') as connect, patch.object(module,'publish_once',create=True) as publish:
            with self.assertRaisesRegex(ValueError,'chỉ quan sát'):
                self.invoke(module,arguments(send=True))
            connect.assert_not_called();publish.assert_not_called()

    def test_sop_trace_counts_observation_without_claiming_execution(self):
        module=load('06_sop_trace');args=arguments()
        with patch.object(module,'open_mqtt',return_value=Mock()), patch.object(module,'close_mqtt'), patch.object(module,'publish_once',create=True) as publish:
            with tempfile.TemporaryDirectory(dir=TEMP) as directory, patch.object(module,'OUT',Path(directory)), redirect_stdout(io.StringIO()):
                module.run_case(args)
                result=json.loads((Path(directory)/'last_result.json').read_text())
                self.assertEqual(result['event_publish_calls'],0)
                self.assertEqual(result['device_commands_published'],0)
                self.assertIn('required',result['sop_execution_verified'])
            publish.assert_not_called()

    def test_duplicate_case_exactly_two_identical_event_publishes(self):
        module=load('08_duplicate');args=arguments(send=True)
        with patch.object(module,'get_camera',return_value=(Mock(),camera(args),{})), patch.object(module,'close_mqtt'), patch.object(module,'publish_once',side_effect=[8,9]) as publish:
            self.invoke(module,args)
            self.assertEqual(publish.call_count,2)
            self.assertEqual(publish.call_args_list[0].args[2],publish.call_args_list[1].args[2])
            self.assertEqual(datetime.fromisoformat(publish.call_args_list[0].args[2]['event_time']).utcoffset().total_seconds(),7*3600)

    def test_event_case_exactly_one_event_publish(self):
        module=load('05_event_history');args=arguments(send=True)
        with patch.object(module,'get_camera',return_value=(Mock(),camera(args),{})), patch.object(module,'close_mqtt'), patch.object(module,'publish_once',return_value=9) as publish:
            self.invoke(module,args)
            publish.assert_called_once()
            self.assertEqual(publish.call_args.args[1],'smart_vms/ai_events/FIGHT_DETECTION')

    def test_rtsp_credentials_are_redacted(self):
        module=load('01_camera_config')
        self.assertEqual(module.safe('rtsp://user:pass@unit.invalid:8554/live'),'rtsp://***@unit.invalid:8554/live')
        self.assertEqual(module.safe({'password':'private'}),{'password':'***'})

    def test_snapshot_send_uploads_once_and_verifies_download(self):
        module=load('04_snapshot');content=b'\xff\xd8\xff'+b'unit-test'
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path=Path(directory)/'unit.jpg';path.write_bytes(content)
            args=arguments(send=True,image=path)
            session=Mock();session.post.return_value=Mock(status_code=200)
            session.post.return_value.json.return_value={'success':True,'object_key':'anomaly/unit.jpg'}
            session.get.return_value=Mock(status_code=200,content=content)
            with patch.object(module,'get_camera',return_value=(Mock(),camera(args),{})),patch.object(module,'close_mqtt'),patch.object(module.requests,'Session') as factory:
                factory.return_value.__enter__.return_value=session
                with patch.object(module,'OUT',Path(directory)/'out'),redirect_stdout(io.StringIO()):
                    module.run_case(args)
                    record=json.loads((module.OUT/'snapshot.json').read_text())
                    self.assertTrue(record['download_verified'])
                    self.assertEqual(record['camera_id'],CAMERA_ID)
                session.post.assert_called_once();session.get.assert_called_once()

    def test_snapshot_key_is_preserved_when_download_fails_no_second_upload(self):
        module=load('04_snapshot')
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path=Path(directory)/'unit.png';path.write_bytes(b'\x89PNG\r\n\x1a\nunit-test')
            args=arguments(send=True,image=path)
            session=Mock();session.post.return_value=Mock(status_code=200)
            session.post.return_value.json.return_value={'success':True,'object_key':'anomaly/unit.png'}
            session.get.side_effect=module.requests.Timeout('unit')
            with patch.object(module,'get_camera',return_value=(Mock(),camera(args),{})),patch.object(module,'close_mqtt'),patch.object(module.requests,'Session') as factory:
                factory.return_value.__enter__.return_value=session
                with patch.object(module,'OUT',Path(directory)/'out'),redirect_stdout(io.StringIO()):
                    with self.assertRaises(module.requests.Timeout):module.run_case(args)
                    record=json.loads((module.OUT/'snapshot.json').read_text())
                    self.assertFalse(record['download_verified'])
                    self.assertEqual(record['object_key'],'anomaly/unit.png')
                session.post.assert_called_once()


if __name__ == '__main__':
    unittest.main()
