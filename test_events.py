"""Kiểm tra xác nhận theo thời gian và chống lặp; không publish sự kiện thử.

Chạy: .venv/bin/python -m unittest -v test_events
Các chuỗi điểm dưới đây chỉ nằm trong unittest, không đi vào luồng MQTT thật.
"""
import unittest
from run import FightGate


class FightGateTests(unittest.TestCase):
    def test_duration_not_number_of_outputs(self):
        gate = FightGate(confirm_seconds=1)
        self.assertFalse(gate.update(.99, 10, 100, 2))
        self.assertFalse(gate.update(.99, 10.2, 100.2, 2))
        self.assertFalse(gate.update(.99, 10.4, 100.4, 2))
        self.assertTrue(gate.update(.99, 11, 101, 2))

    def test_one_event_until_low_reset_and_cooldown(self):
        gate = FightGate(confirm_seconds=1, reset_seconds=2, cooldown=10)
        for stamp in (0, .5):
            self.assertFalse(gate.update(.99, stamp, 100 + stamp, 2))
        self.assertTrue(gate.update(.99, 1, 101, 2))
        for stamp in (1.5, 2, 2.5, 3):
            self.assertFalse(gate.update(.99, stamp, 100 + stamp, 3))
        for stamp in (3.5, 4, 4.5, 5, 5.5):
            self.assertFalse(gate.update(.1, stamp, 100 + stamp, 3))
        self.assertFalse(gate.active)
        for stamp in (6, 6.5, 7, 8, 9, 10):
            self.assertFalse(gate.update(.99, stamp, 100 + stamp, 2))
        self.assertTrue(gate.update(.99, 11, 111, 2))

    def test_gap_does_not_confirm_and_duplicate_stamp_ignored(self):
        gate = FightGate()
        self.assertFalse(gate.update(.99, 0, 100, 2))
        self.assertFalse(gate.update(.99, 4, 104, 2))
        self.assertFalse(gate.update(.99, 4, 104, 2))
        self.assertFalse(gate.update(.99, 4.5, 104.5, 2))
        self.assertTrue(gate.update(.99, 5, 105, 2))

    def test_need_two_real_people_and_continuous_high(self):
        gate = FightGate()
        for stamp in (0, .5, 1):
            self.assertFalse(gate.update(.99, stamp, 100 + stamp, 1))
        self.assertFalse(gate.update(.99, 1.5, 101.5, 2))
        self.assertFalse(gate.update(.8, 2, 102, 2))
        self.assertFalse(gate.update(.99, 2.5, 102.5, 2))
        self.assertFalse(gate.update(.99, 3, 103, 2))
        self.assertTrue(gate.update(.99, 3.5, 103.5, 2))

    def test_restart_preserves_active_incident(self):
        gate = FightGate(active=True, last_event_wall=100)
        for stamp in range(5):
            self.assertFalse(gate.update(.99, stamp, 200 + stamp, 2))
        for stamp in range(5, 8):
            self.assertFalse(gate.update(.1, stamp, 200 + stamp, 2))
        self.assertFalse(gate.active)
        self.assertFalse(gate.update(.99, 8, 208, 2))
        self.assertTrue(gate.update(.99, 9, 209, 2))

    def test_invalid_scores_rejected(self):
        for score in (float('nan'), float('inf'), -1, 2):
            with self.assertRaises(ValueError):
                FightGate().update(score, 1, 100, 2)


if __name__ == '__main__':
    unittest.main()
