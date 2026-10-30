"""Kiểm tra xoay vòng log và chặn ghi lúc thiếu đĩa; không nạp model/chạy MQTT.

Chạy trong môi trường deployment: .venv/bin/python -m unittest -v test_storage
Tất cả file thử nằm trong thư mục tạm, không đụng dữ liệu của service.
"""

import json
import logging
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from run import BudgetLogHandler, StorageGuard, SnapshotRetention, LogRetention


class StorageTests(unittest.TestCase):
    def test_rotation_batches_cleanup_and_keeps_latest(self):
        with tempfile.TemporaryDirectory() as directory:
            guard = StorageGuard(directory, reserve_bytes=0)
            handler = BudgetLogHandler(Path(directory) / "log.jsonl", guard, max_bytes=1024, total_bytes=3072)
            handler.setFormatter(logging.Formatter("%(message)s"))
            try:
                for i in range(150):
                    row = json.dumps({"sequence": i, "payload": "x" * 100})
                    handler.handle(logging.LogRecord("test", logging.INFO, "", 0, row, (), None))
                    if i % 5 == 0:
                        handler.prune()
                handler.prune()
                handler.flush()
                files = list(Path(directory).glob("log.jsonl*"))
                self.assertLessEqual(sum(f.stat().st_size for f in files), 3 * 1024)
                for file in files:
                    for line in file.read_text().splitlines():
                        json.loads(line)
                latest = [json.loads(line) for line in (Path(directory) / "log.jsonl").read_text().splitlines()]
                self.assertEqual(latest[-1]["sequence"], 149)
            finally:
                handler.close()

    def test_log_json_delete_oldest_half_and_keep_current_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy = LogRetention(root, max_bytes=200)
            (root / "log.jsonl").write_bytes(b"a" * 20)
            for name in ["health.json", "latest_payload.json", "effective_bbox.schema.json"]:
                (root / name).write_text("{}")
            (root / "weights.pt").write_bytes(b"x" * 1000)
            outside = root / "outside"
            outside.mkdir()
            (outside / "protected.json").write_bytes(b"x" * 1000)
            (root / "link.json").symlink_to(outside / "protected.json")
            for i in range(1, 6):
                archive = root / f"log.jsonl.{i}"
                archive.write_bytes(b"x" * 40)
                os.utime(archive, (i, i))
            result = policy.sweep()
            self.assertEqual(result["before_bytes"], 226)
            self.assertEqual(result["after_bytes"], 106)
            self.assertEqual(result["deleted_files"], 3)
            for i in range(1, 4):
                self.assertFalse((root / f"log.jsonl.{i}").exists())
            self.assertTrue((root / "log.jsonl.5").exists())
            self.assertEqual((root / "log.jsonl").stat().st_size, 20)
            self.assertEqual((outside / "protected.json").stat().st_size, 1000)
            self.assertTrue((root / "weights.pt").exists())
            for name in ["health.json", "latest_payload.json", "effective_bbox.schema.json"]:
                self.assertEqual((root / name).read_text(), "{}")
            self.assertEqual(policy.sweep()["deleted_files"], 0)
            for i in range(6, 9):
                archive = root / f"log.jsonl.{i}"
                archive.write_bytes(b"x" * 40)
                os.utime(archive, (i, i))
            self.assertEqual(policy.sweep()["after_bytes"], 106)

    def test_oversized_active_log_is_archived_before_cleanup_and_logging_continues(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "log.jsonl"
            path.write_bytes(b"x" * 250)
            handler = BudgetLogHandler(path, StorageGuard(directory, reserve_bytes=0), max_bytes=100, total_bytes=200)
            handler.setFormatter(logging.Formatter("%(message)s"))
            try:
                result = handler.prune()
                self.assertEqual(result["deleted_bytes"], 250)
                self.assertLessEqual(result["after_bytes"], 125)
                handler.handle(logging.LogRecord("test", logging.INFO, "", 0, '{"alive":true}', (), None))
                handler.flush()
                self.assertEqual(json.loads(path.read_text()), {"alive": True})
            finally:
                handler.close()

    def test_low_space_does_not_append_logs_or_overwrite_health(self):
        with tempfile.TemporaryDirectory() as directory:
            guard = StorageGuard(directory, reserve_bytes=2048)
            health = Path(directory) / "health.json"
            health.write_text('{"state":"before"}')
            handler = BudgetLogHandler(Path(directory) / "log.jsonl", guard)
            try:
                with patch("run.shutil.disk_usage", return_value=SimpleNamespace(free=2047)):
                    self.assertFalse(guard.write_json("health.json", {"state": "after"}))
                    handler.handle(logging.LogRecord("test", logging.INFO, "", 0, "not written", (), None))
                self.assertEqual(json.loads(health.read_text()), {"state": "before"})
                self.assertFalse((Path(directory) / "log.jsonl").exists())
                self.assertGreaterEqual(guard.denied_writes, 2)
            finally:
                handler.close()

    def test_status_updates_replace_one_file(self):
        with tempfile.TemporaryDirectory() as directory:
            guard = StorageGuard(directory, reserve_bytes=0)
            for i in range(30):
                self.assertTrue(guard.write_json("health.json", {"sequence": i}))
            self.assertEqual([p.name for p in Path(directory).iterdir()], ["health.json"])
            self.assertEqual(json.loads((Path(directory) / "health.json").read_text())["sequence"], 29)

    def test_snapshots_delete_oldest_half_and_repeat_after_refill(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshots = Path(directory) / "snapshots"
            policy = SnapshotRetention(snapshots, max_bytes=200)
            for i in range(7):
                image = snapshots / f"frame_{i}.jpg"
                image.write_bytes(b"x" * 32)
                os.utime(image, (i + 1, i + 1))
            result = policy.sweep()
            self.assertEqual(result["before_bytes"], 224)
            self.assertEqual(result["after_bytes"], 96)
            self.assertEqual(result["deleted_files"], 4)
            self.assertEqual(sorted(p.name for p in snapshots.iterdir()), ["frame_4.jpg", "frame_5.jpg", "frame_6.jpg"])
            self.assertEqual(policy.sweep()["deleted_files"], 0)
            for i in range(7, 11):
                image = snapshots / f"frame_{i}.jpg"
                image.write_bytes(b"x" * 32)
                os.utime(image, (i + 1, i + 1))
            self.assertEqual(policy.sweep()["after_bytes"], 96)
            self.assertEqual(sorted(p.name for p in snapshots.iterdir()), ["frame_10.jpg", "frame_8.jpg", "frame_9.jpg"])

    def test_snapshots_preserve_data_at_limit_and_outside_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshots = Path(directory) / "snapshots"
            policy = SnapshotRetention(snapshots, max_bytes=200)
            (snapshots / "at_limit.jpg").write_bytes(b"x" * 200)
            outside = Path(directory) / "outside"
            outside.mkdir()
            protected = outside / "protected.jpg"
            protected.write_bytes(b"x" * 1000)
            (snapshots / "external").symlink_to(outside, target_is_directory=True)
            (snapshots / "external.jpg").symlink_to(protected)
            result = policy.sweep()
            self.assertEqual(result["before_bytes"], 200)
            self.assertEqual(result["deleted_files"], 0)
            self.assertEqual(protected.stat().st_size, 1000)
            (snapshots / "overflow.jpg").write_bytes(b"x" * 100)
            policy.sweep()
            self.assertEqual(protected.stat().st_size, 1000)


if __name__ == "__main__":
    unittest.main()
