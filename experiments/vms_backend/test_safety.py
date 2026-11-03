"""Kiểm tra file đơn giản bằng MQTT/HTTP giả lập; không gửi VMS thật."""
from contextlib import contextmanager, redirect_stdout
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent
TEMP = ROOT / 'output/unittest'
TEMP.mkdir(parents=True, exist_ok=True)
SETTINGS = {
    'VMS_MQTT_HOST': 'unit.invalid', 'VMS_MQTT_USERNAME': 'unit', 'VMS_MQTT_PASSWORD': 'unit',
    'VMS_BROKER_IP': '', 'VMS_CAMERA_CODE': 'UNIT_CAMERA',
    'VMS_CAMERA_ID': '00000000-0000-4000-8000-000000000001',
    'VMS_ZONE_ID': '9001', 'VMS_ORGANIZATION_ID': '9001', 'VMS_MODULE': 'FIGHT_DETECTION',
}
CAMERA = {'id': SETTINGS['VMS_CAMERA_ID'], 'code': 'UNIT_CAMERA', 'zone_id': 9001,
          'ai_modules': ['FIGHT_DETECTION']}


@contextmanager
def program(folder):
    with tempfile.TemporaryDirectory(dir=TEMP) as directory:
        spec = importlib.util.spec_from_file_location('test_' + folder, ROOT / folder / 'run.py')
        module = importlib.util.module_from_spec(spec)
        with patch('dotenv.dotenv_values', return_value=SETTINGS):
            spec.loader.exec_module(module)
        module.SUITE = Path(directory)
        module.OUT = Path(directory) / 'result'
        if hasattr(module, 'SECONDS'): module.SECONDS = 0.001
        image = Path(directory) / 'snapshot.jpg'
        image.write_bytes(bytes.fromhex('ffd8ff') + b'unit-image')
        if hasattr(module, 'IMAGE'): module.IMAGE = image
        snapshot = Path(directory) / '04_snapshot/output/snapshot.json'
        snapshot.parent.mkdir(parents=True)
        snapshot.write_text(json.dumps({'camera_id': CAMERA['id'], 'module': 'FIGHT_DETECTION',
                                        'object_key': 'anomaly/unit.jpg'}))
        with redirect_stdout(io.StringIO()): yield module


def broker(module, messages=None, published=True, camera=None):
    """Giả CONNACK/SUBACK/metadata; publish chỉ được ghi nhớ trong RAM."""
    client = Mock()
    mids = []
    def publish(topic, payload, **options):
        mids.append(len(mids) + 1)
        info = Mock(rc=0, mid=mids[-1])
        info.is_published.return_value = published
        return info
    client.publish.side_effect = publish
    def subscribe(*args, **kwargs):
        client.on_subscribe(client, None, 1, [SimpleNamespace(is_failure=False)], None)
        return 0, 1
    client.subscribe.side_effect = subscribe
    if messages is None:
        messages = [('smart_vms/cameras/company/9001', {'organization_id': 9001, 'cameras': [camera or CAMERA]})]
    def start():
        client.on_connect(client, None, None, 0, None)
        for topic, data in messages:
            payload = data if isinstance(data, bytes) else json.dumps(data).encode()
            client.on_message(client, None, SimpleNamespace(topic=topic, payload=payload, retain=False))
    client.loop_start.side_effect = start
    return client


def events(client):
    return [call for call in client.publish.call_args_list if call.args[0].startswith('smart_vms/ai_events/')]


class SafetyTests(unittest.TestCase):
    def test_write_cases_default_to_preview_without_network(self):
        for folder in ['03_bbox', '04_snapshot', '05_event_history', '07_rejection', '08_duplicate']:
            with self.subTest(folder=folder), program(folder) as module:
                self.assertFalse(module.SEND)
                with patch.object(module.mqtt, 'Client') as client, patch('requests.post') as post:
                    module.main(); client.assert_not_called(); post.assert_not_called()

    def test_geometry_reports_change_empty_and_invalid_without_publish(self):
        with program('02_geometry') as module:
            topic = 'smart_vms/cameras/UNIT_CAMERA/ai_zones/FIGHT_DETECTION'
            data = {'camera_id': CAMERA['id'], 'camera_code': 'UNIT_CAMERA', 'ai_zones': [
                {'points': [{'x': 0, 'y': 0}, {'x': 1, 'y': 0}, {'x': 0, 'y': 1}]}]}
            changed = copy.deepcopy(data); changed['ai_zones'][0]['points'][1]['x'] = .7
            wrong = copy.deepcopy(data); wrong['camera_code'] = 'OTHER_CAMERA'
            invalid = copy.deepcopy(data); invalid['ai_zones'][0]['points'][0]['x'] = 2
            client = broker(module, [(topic, data), (topic, changed), (topic, b''), (topic, wrong), (topic, invalid), (topic, b'not-json')])
            with patch.object(module.mqtt, 'Client', return_value=client): module.main()
            self.assertEqual(json.loads((module.OUT / 'last_result.json').read_text()),
                             {'messages': 3, 'changes': 2, 'empty': 1, 'invalid': 3})
            client.publish.assert_not_called()

    def test_event_published_once_qos1_no_retain(self):
        with program('05_event_history') as module:
            module.SEND = True; client = broker(module)
            with patch.object(module.mqtt, 'Client', return_value=client): module.main()
            sent = events(client)
            self.assertEqual(len(sent), 1)
            self.assertEqual(sent[0].kwargs, {'qos': 1, 'retain': False})
            self.assertEqual(json.loads(sent[0].args[1])['payload']['type'], 'FIGHT')

    def test_duplicate_exactly_two_identical_bodies(self):
        with program('08_duplicate') as module:
            module.SEND = True; client = broker(module)
            with patch.object(module.mqtt, 'Client', return_value=client), patch.object(module.time, 'sleep'): module.main()
            sent = events(client)
            self.assertEqual(len(sent), 2)
            self.assertEqual(sent[0].args[1], sent[1].args[1])

    def test_unknown_ack_not_retried(self):
        with program('05_event_history') as module:
            module.SEND = True; client = broker(module, published=False)
            with patch.object(module.mqtt, 'Client', return_value=client), self.assertRaises(RuntimeError): module.main()
            self.assertEqual(len(events(client)), 1)
            client.disconnect.assert_called_once()

    def test_unknown_camera_replaces_uuid_and_code(self):
        with program('07_rejection') as module:
            module.SEND = True; module.CASE = 'unknown-camera'; client = broker(module)
            with patch.object(module.mqtt, 'Client', return_value=client): module.main()
            event = json.loads(events(client)[0].args[1])
            self.assertNotEqual(event['camera_id'], CAMERA['id'])
            self.assertNotEqual(event['camera_code'], CAMERA['code'])

    def test_disabled_case_blocks_when_module_enabled(self):
        with program('07_rejection') as module:
            module.SEND = True; module.CASE = 'disabled-module'; client = broker(module)
            with patch.object(module.mqtt, 'Client', return_value=client), self.assertRaises(RuntimeError): module.main()
            self.assertEqual(len(events(client)), 0)

    def test_sop_never_publishes(self):
        with program('06_sop_trace') as module:
            client = broker(module, messages=[])
            with patch.object(module.mqtt, 'Client', return_value=client): module.main()
            client.publish.assert_not_called()

    def test_missing_module_blocks_event(self):
        with program('05_event_history') as module:
            module.SEND = True; client = broker(module, camera={**CAMERA, 'ai_modules': []})
            with patch.object(module.mqtt, 'Client', return_value=client), self.assertRaises(RuntimeError): module.main()
            self.assertEqual(len(events(client)), 0)

    def test_wrong_snapshot_blocks_before_network(self):
        with program('05_event_history') as module:
            (module.SUITE / '04_snapshot/output/snapshot.json').write_text(json.dumps(
                {'camera_id': 'wrong', 'module': module.MODULE, 'object_key': 'anomaly/unit.jpg'}))
            module.SEND = True
            with patch.object(module.mqtt, 'Client') as client, self.assertRaises(ValueError): module.main()
            client.assert_not_called()

    def test_upload_one_post_one_get(self):
        with program('04_snapshot') as module:
            module.SEND = True; client = broker(module); response = Mock(status_code=200)
            response.json.return_value = {'success': True, 'object_key': 'anomaly/unit.jpg'}
            with patch.object(module.mqtt, 'Client', return_value=client), patch.object(module.requests, 'post', return_value=response) as post, patch.object(module.requests, 'get', return_value=Mock(content=module.IMAGE.read_bytes())) as get:
                module.main(); post.assert_called_once(); get.assert_called_once()
            self.assertTrue(json.loads((module.OUT / 'snapshot.json').read_text())['download_verified'])

    def test_upload_key_survives_get_failure(self):
        with program('04_snapshot') as module:
            module.SEND = True; client = broker(module); response = Mock(status_code=200)
            response.json.return_value = {'success': True, 'object_key': 'anomaly/unit.jpg'}
            with patch.object(module.mqtt, 'Client', return_value=client), patch.object(module.requests, 'post', return_value=response) as post, patch.object(module.requests, 'get', side_effect=module.requests.Timeout('unit')):
                with self.assertRaises(module.requests.Timeout): module.main()
                post.assert_called_once()
            record = json.loads((module.OUT / 'snapshot.json').read_text())
            self.assertEqual(record['object_key'], 'anomaly/unit.jpg')
            self.assertFalse(record['download_verified'])


if __name__ == '__main__':
    unittest.main()
