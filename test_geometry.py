"""C03: vùng/vạch retained, cập nhật nóng, tombstone, dữ liệu sai và chọn người.

Các payload ở đây chỉ dùng trong unittest, không publish lên broker IOC.
Chạy: python -m unittest -v test_geometry
"""
import copy
import json
import unittest
from run import AiZonesConfig

CAMERA_ID = '00000000-0000-4000-8000-000000000001'
ZONE_ID = '00000000-0000-4000-8000-000000000002'


def polygon(points=None):
    return {'id': ZONE_ID, 'name': 'Test zone', 'ai_modules': 'FIGHT_DETECTION',
            'is_active': True, 'points': points or [{'x': 0, 'y': 0}, {'x': .5, 'y': 0},
                                                   {'x': .5, 'y': 1}, {'x': 0, 'y': 1}]}


class GeometryTests(unittest.TestCase):
    def setUp(self):
        self.config = AiZonesConfig('TEST_CAMERA', CAMERA_ID)

    def payload(self, zones):
        return json.dumps({'camera_code': 'TEST_CAMERA', 'camera_id': CAMERA_ID, 'ai_zones': zones})

    def test_hot_update_replaces_region_and_identical_payload_is_idempotent(self):
        first = self.payload([polygon()])
        self.assertTrue(self.config.update(first, 'polygons'))
        self.assertFalse(self.config.update(first, 'polygons'))
        self.assertEqual(self.config.version, 1)
        second = self.payload([polygon([{'x': .6, 'y': 0}, {'x': 1, 'y': 0}, {'x': 1, 'y': 1}])])
        self.assertTrue(self.config.update(second, 'polygons'))
        self.assertEqual(self.config.version, 2)
        self.assertEqual(len(self.config.polygons), 1)

    def test_empty_array_and_tombstone_clear_without_crash(self):
        self.config.update(self.payload([polygon()]), 'polygons')
        self.assertTrue(self.config.update(self.payload([]), 'polygons'))
        self.assertEqual(self.config.polygons, [])
        self.config.update(self.payload([polygon()]), 'polygons')
        self.assertTrue(self.config.update(b'', 'polygons'))
        self.assertEqual(self.config.polygons, [])

    def test_invalid_payload_preserves_last_good_configuration(self):
        self.config.update(self.payload([polygon()]), 'polygons')
        before = copy.deepcopy(self.config.status())
        bad_id = polygon()
        bad_id['id'] = 123
        bad_direction = polygon()
        bad_direction['direction_point'] = {'x': float('nan'), 'y': 0}
        for payload in ('invalid json', '[]', '{}', self.payload([bad_id]), self.payload([bad_direction]), self.payload([polygon([{'x': 2, 'y': 0}])]),
                        self.payload([polygon([{'x': 0, 'y': 0}, {'x': .5, 'y': .5}, {'x': 1, 'y': 1}])])):
            with self.assertRaises((ValueError, TypeError)):
                self.config.update(payload, 'polygons')
            self.assertEqual(self.config.status(), before)

    def test_wrong_camera_and_uuid_rejected(self):
        for changes in ({'camera_code': 'OTHER'}, {'camera_id': ZONE_ID}):
            doc = json.loads(self.payload([polygon()]))
            doc.update(changes)
            with self.assertRaises(ValueError):
                self.config.update(json.dumps(doc), 'polygons')

    def test_lines_separate_and_can_be_cleared(self):
        line = {'id': ZONE_ID, 'line': {'points': [{'x': 0, 'y': .5}, {'x': 1, 'y': .5}]},
                'point': {'x': .5, 'y': .8}, 'ai_modules': 'FIGHT_DETECTION'}
        self.config.update(json.dumps({'lines': [line]}), 'lines')
        self.assertEqual(len(self.config.lines), 1)
        self.assertEqual(self.config.lines[0]['direction_point'], line['point'])
        self.assertEqual(self.config.polygons, [])
        self.config.update(b'', 'lines')
        self.assertEqual(self.config.lines, [])
        with self.assertRaises(ValueError):
            self.config.update(json.dumps({'lines': [{'line': [1, 2]}]}), 'lines')

    def test_inactive_and_other_module_not_used(self):
        a, b = polygon(), polygon()
        a['is_active'] = False
        b['ai_modules'] = 'PLATE'
        self.config.update(self.payload([a, b]), 'polygons')
        self.assertEqual(self.config.polygons, [])

    def test_bottom_center_on_boundary_counts_and_outside_excluded(self):
        detections = [{'bbox': [.1, .1, .3, .9]}, {'bbox': [.4, .1, .6, 1]}, {'bbox': [.8, .1, 1, .9]}]
        self.assertEqual(self.config.event_people(detections), (3, None))
        self.config.update(self.payload([polygon()]), 'polygons')
        count, zone = self.config.event_people(detections)
        self.assertEqual(count, 2)
        self.assertEqual(zone['id'], ZONE_ID)

    def test_concave_polygon_and_closed_points(self):
        points = [(0,0), (1,0), (1,.3), (.3,.3), (.3,1), (0,1)]
        self.assertTrue(self.config.contains(points, .2, .8))
        self.assertFalse(self.config.contains(points, .8, .8))
        p = polygon()
        p['points'].append(p['points'][0])
        self.config.update(self.payload([p]), 'polygons')
        self.assertEqual(len(self.config.polygons[0]['points']), 4)


if __name__ == '__main__':
    unittest.main()
