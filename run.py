"""Chạy YOLO + VD-MIL toàn ảnh; gửi bbox/điểm và tùy chọn sự kiện thật lên IOC.

Luồng chạy từ trên xuống:
1. Đọc tham số, schema và tạo log xoay vòng để không đầy ổ đĩa.
2. Nạp YOLO11n và VD-MIL; xác nhận GPU hoạt động trước khi chạy nền.
3. Kết nối MQTT TLS/WebSocket, subscribe cấu hình zone cấu hình và topic bbox.
4. Capture đọc frame mới nhất từ RTSP, tự mở lại khi mất luồng.
5. Sampler lấy 8 frame/giây riêng, tránh YOLO làm chậm cửa sổ video.
6. Hai worker chạy YOLO bbox người và VD-MIL điểm của toàn clip.
7. Capture đánh thức YOLO theo frame; YOLO đánh thức publisher theo kết quả mới.
8. Nếu --events: xác nhận điểm cao đủ thời gian, chốt một vụ, upload JPEG trong
   RAM rồi publish sự kiện QoS1; worker riêng không chặn bbox. Điểm thấp đủ lâu
   mới tái kích và phải hết cooldown. Ghi health, nhận SIGTERM để dừng.

Mặc định không gửi sự kiện; --events có thể tạo history/notification/SOP thật.
QoS0 bbox không có PUBACK; PUBACK sự kiện cũng chưa chứng minh backend đã lưu.
Model/phần vendor được đóng gói cùng thư mục để server không cần tải model.
"""

import argparse
from collections import deque
from datetime import datetime, timezone, timedelta
import json
import ipaddress
import logging
from logging.handlers import RotatingFileHandler
import math
import os
import queue
from pathlib import Path
import signal
import shutil
import socket
import ssl
import sys
import threading
import time
import uuid

# Giảm log native OpenCV; không ghi URL RTSP chứa thông tin đăng nhập.
os.environ.setdefault("OPENCV_LOG_LEVEL", "OFF")
os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"
ROOT = Path(__file__).resolve().parent
sys.path[:0] = [str(ROOT / "vendor/movinet_runtime"), str(ROOT / "vendor/violence-detection-mil")]

import cv2
import numpy as np
import paho.mqtt.client as mqtt
import torch
import requests
from dotenv import load_dotenv
from ultralytics import YOLO
from jsonschema import Draft202012Validator, FormatChecker
from movinet_classifier import MovinetClassifier

MODULE = "FIGHT_DETECTION"
LOG_MAX_BYTES = 20 * 1024 * 1024
LOG_TOTAL_MAX_BYTES = 200 * 1024 * 1024
MIN_FREE_BYTES = 2 * 1024 * 1024 * 1024
SNAPSHOT_MAX_BYTES = 200 * 1024 * 1024


class AiZonesConfig:
    """Nhận cấu hình vùng/vạch của một camera, không cắt ảnh đầu vào VD-MIL.

    Luồng: kiểm tra đúng camera → parse JSON/tombstone → kiểm tra điểm 0..1 →
    thay toàn bộ danh sách tương ứng. Payload sai giữ cấu hình tốt trước đó.
    Lines được nhận/lưu để đáp ứng hợp đồng; FIGHT không áp luật đếm IN/OUT.
    """

    def __init__(self, camera_code, camera_id=None):
        self.camera_code, self.camera_id = camera_code, camera_id
        self.polygons, self.lines = [], []
        self.version = 0
        self.received = False

    @staticmethod
    def points(values, minimum):
        if not isinstance(values, list):
            raise ValueError("points must be a list")
        result = []
        for point in values:
            if not isinstance(point, dict):
                raise ValueError("point must contain x/y")
            xy = tuple(point.get(key) for key in ("x", "y"))
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                   or not 0 <= v <= 1 for v in xy):
                raise ValueError("point coordinates must be finite numbers within 0..1")
            if not result or xy != result[-1]:
                result.append(xy)
        if len(result) > 1 and result[0] == result[-1]:
            result.pop()
        if len(set(result)) < minimum:
            raise ValueError("not enough distinct points")
        return result

    def update(self, payload, kind):
        field = "lines" if kind == "lines" else "ai_zones"
        def reject_constant(value):
            raise ValueError("geometry JSON must not contain NaN/Infinity")
        data = json.loads(payload, parse_constant=reject_constant) if payload else {field: []}
        if not isinstance(data, dict) or not isinstance(data.get(field), list):
            raise ValueError("invalid geometry payload")
        if data.get("camera_code", self.camera_code) != self.camera_code:
            raise ValueError("geometry belongs to another camera code")
        if self.camera_id and data.get("camera_id", self.camera_id) != self.camera_id:
            raise ValueError("geometry belongs to another camera UUID")
        parsed = []
        for zone in data[field]:
            if not isinstance(zone, dict):
                raise ValueError("zone must be an object")
            if zone.get("is_active", True) is False:
                continue
            module = zone.get("ai_modules", MODULE)
            modules = module if isinstance(module, list) else [module]
            if MODULE not in modules:
                continue
            is_line = kind == "lines" or zone.get("shape_type") == "line_crossing"
            # Line-crossing được nhận từ topic /lines riêng, không dùng làm polygon.
            if kind != "lines" and is_line:
                continue
            line_data = zone.get("line") or {}
            if is_line and not isinstance(line_data, dict):
                raise ValueError("line must be an object")
            values = line_data.get("points") if is_line else zone.get("points")
            points = self.points(values, 2 if is_line else 3)
            if is_line and len(points) != 2:
                raise ValueError("line must have exactly two points")
            if not is_line:
                area = abs(sum(points[i][0] * points[(i+1) % len(points)][1]
                               - points[(i+1) % len(points)][0] * points[i][1] for i in range(len(points)))) / 2
                if area < 1e-8:
                    raise ValueError("polygon must have nonzero area")
            zone_id = zone.get("id")
            if zone_id is not None:
                if not isinstance(zone_id, str):
                    raise ValueError("zone id must be a UUID string")
                uuid.UUID(zone_id)
            parsed.append({"id": zone_id, "name": str(zone.get("name", zone.get("code", ""))),
                           "points": points, "direction_point": zone.get("direction_point", zone.get("point"))})
        old = self.lines if kind == "lines" else self.polygons
        changed = parsed != old
        if kind == "lines":
            self.lines = parsed
        else:
            self.polygons = parsed
        self.received = True
        if changed:
            self.version += 1
        return changed

    @staticmethod
    def contains(points, x, y):
        # Ray casting, coi điểm trên biên là nằm trong vùng theo dõi.
        inside = False
        for a, b in zip(points, points[1:] + points[:1]):
            cross = (x-a[0]) * (b[1]-a[1]) - (y-a[1]) * (b[0]-a[0])
            if abs(cross) < 1e-9 and min(a[0],b[0])-1e-9 <= x <= max(a[0],b[0])+1e-9 and min(a[1],b[1])-1e-9 <= y <= max(a[1],b[1])+1e-9:
                return True
            if (a[1] > y) != (b[1] > y):
                if x < (b[0]-a[0]) * (y-a[1]) / (b[1]-a[1]) + a[0]:
                    inside = not inside
        return inside

    def event_people(self, detections):
        # Bbox/clip vẫn toàn ảnh; khi có polygon, chỉ cho cảnh báo nếu đủ người
        # trong cùng một polygon. Đây không phải định vị hành vi đánh nhau.
        if not self.polygons:
            return len(detections), None
        counts = [(sum(self.contains(zone["points"], (d["bbox"][0]+d["bbox"][2])/2, d["bbox"][3])
                       for d in detections), zone) for zone in self.polygons]
        return max(counts, key=lambda item: item[0])

    def status(self):
        return {"received": self.received, "version": self.version,
                "polygon_count": len(self.polygons), "line_count": len(self.lines),
                "polygons": self.polygons, "lines": self.lines}


class FightGate:
    """Xác nhận theo giây; một vụ một lần, không nối thời gian qua mất dữ liệu."""

    def __init__(self, threshold=.9, confirm_seconds=1., reset_seconds=2., cooldown=60.,
                 active=False, last_event_wall=0.):
        self.threshold, self.confirm_seconds = threshold, confirm_seconds
        self.reset_seconds, self.cooldown = reset_seconds, cooldown
        self.active, self.last_event_wall = active, last_event_wall
        self.high_since = self.low_since = self.last_stamp = None

    def update(self, score, stamp, wall, people):
        if not math.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("fight score must be finite and within [0,1]")
        # Score phải thuộc cửa sổ mới. Khoảng ngắt >1s hủy đếm liên tục,
        # nhưng không mở lại một vụ đã cảnh báo khi chưa có bằng chứng kết thúc.
        if self.last_stamp is not None:
            if stamp <= self.last_stamp:
                return False
            if stamp - self.last_stamp > 1.:
                self.high_since = self.low_since = None
        self.last_stamp = stamp
        if score < .5:
            self.high_since = None
            if self.low_since is None:
                self.low_since = stamp
            if stamp - self.low_since >= self.reset_seconds:
                self.active = False
        else:
            self.low_since = None
            if score < self.threshold or people < 2:
                self.high_since = None
            elif not self.active:
                if self.high_since is None:
                    self.high_since = stamp
                if stamp - self.high_since >= self.confirm_seconds and wall - self.last_event_wall >= self.cooldown:
                    self.active, self.last_event_wall = True, wall
                    return True
        return False


class StorageGuard:
    """Chặn ghi dữ liệu mới khi dung lượng trống chạm mức dự phòng.

    Không xóa snapshot/video hoặc dữ liệu của chương trình khác. Bản bbox này
    không tạo snapshot/video; guard áp dụng cho log và JSON do chính nó ghi.
    """

    def __init__(self, directory, reserve_bytes=MIN_FREE_BYTES):
        self.directory = Path(directory)
        self.reserve_bytes = reserve_bytes
        self.denied_writes = 0

    def can_write(self, size=0):
        try:
            allowed = shutil.disk_usage(self.directory).free >= self.reserve_bytes + size
        except OSError:
            allowed = False
        if not allowed:
            self.denied_writes += 1
        return allowed

    def write_json(self, name, data):
        # Tên file cố định và os.replace: giữ đúng một phiên bản, không tích lũy.
        content = json.dumps(data, indent=2, allow_nan=False).encode("utf-8")
        if not self.can_write(len(content)):
            return False
        temporary = self.directory / (name + ".tmp")
        try:
            temporary.write_bytes(content)
            temporary.replace(self.directory / name)
            return True
        except OSError:
            self.denied_writes += 1
            return False


class LogRetention:
    """Tổng log/JSON >200 MiB: xóa file cũ nhất đến khi còn <=50%.

    Chỉ quét file trực tiếp trong output, không đụng snapshots hoặc thư mục khác.
    File log đang mở và các file trạng thái hiện tại luôn được bảo vệ.
    """

    PROTECTED = {"log.jsonl", "health.json", "latest_payload.json", "effective_bbox.schema.json",
                 "event_state.json", "latest_event.json", "zones.json"}

    def __init__(self, directory, max_bytes=LOG_TOTAL_MAX_BYTES):
        self.directory = Path(directory).absolute()
        self.max_bytes = max_bytes

    def sweep(self, target_bytes=None):
        if self.directory.is_symlink():
            raise ValueError("log directory must not be a symlink")
        root = self.directory.resolve()
        files, errors = [], 0
        for path in root.iterdir():
            is_log = (path.suffix in {".json", ".jsonl", ".log"}
                      or (path.name.startswith("log.jsonl.") and path.name[10:].isdigit()))
            if not is_log or path.is_symlink() or not path.is_file():
                continue
            try:
                info = path.stat()
                files.append((info.st_mtime_ns, str(path), info.st_size))
            except OSError:
                errors += 1
        total = sum(entry[2] for entry in files)
        target = target_bytes if target_bytes is not None else (total // 2 if total > self.max_bytes else total)
        remaining, deleted_files, deleted_bytes = total, 0, 0
        if total > target:
            for _, name, size in sorted(files):
                if remaining <= target:
                    break
                path = Path(name)
                if path.name in self.PROTECTED:
                    continue
                try:
                    path.resolve().relative_to(root)
                    if path.is_symlink() or not path.is_file():
                        continue
                    path.unlink()
                    remaining -= size
                    deleted_files += 1
                    deleted_bytes += size
                except (OSError, ValueError):
                    errors += 1
        return {"before_bytes": total, "after_bytes": remaining,
                "deleted_files": deleted_files, "deleted_bytes": deleted_bytes,
                "target_bytes": target, "limit_bytes": self.max_bytes, "errors": errors}


class BudgetLogHandler(RotatingFileHandler):
    """Chia log 20 MiB/file; dọn theo đợt 50% khi tổng log/JSON vượt 200 MiB."""

    def __init__(self, filename, storage, max_bytes=LOG_MAX_BYTES, total_bytes=LOG_TOTAL_MAX_BYTES):
        super().__init__(filename, maxBytes=max_bytes, backupCount=0, encoding="utf-8", delay=True)
        self.storage = storage
        self.retention = LogRetention(Path(filename).parent, max_bytes=total_bytes)

    def emit(self, record):
        if self.storage.can_write(len((self.format(record) + "\n").encode("utf-8"))):
            super().emit(record)

    def doRollover(self):
        # Archive có tên duy nhất; không xóa lẻ từng file như backupCount trước đây.
        if self.stream:
            self.stream.close()
            self.stream = None
        path = Path(self.baseFilename)
        if path.exists() and path.stat().st_size:
            path.replace(path.with_name(path.name + "." + str(time.time_ns())))
        if not self.delay:
            self.stream = self._open()

    def prune(self):
        # Chung khóa với emit/rollover để không xóa hoặc rename file lúc đang ghi.
        self.acquire()
        try:
            if self.stream:
                self.stream.flush()
            result = self.retention.sweep()
            # Xử lý cả log đang mở quá lớn từ phiên cũ: đóng/archive rồi mới dọn.
            if result["before_bytes"] > result["limit_bytes"] and result["after_bytes"] > result["target_bytes"]:
                self.doRollover()
                extra = self.retention.sweep(target_bytes=result["target_bytes"])
                result.update(after_bytes=extra["after_bytes"],
                              deleted_files=result["deleted_files"] + extra["deleted_files"],
                              deleted_bytes=result["deleted_bytes"] + extra["deleted_bytes"],
                              errors=result["errors"] + extra["errors"])
            return result
        finally:
            self.release()


class SnapshotRetention:
    """Vượt 200 MiB: xóa file cũ nhất đến khi còn tối đa một nửa dung lượng.

    Chỉ quản lý output/snapshots của deployment này. Không đi theo symlink,
    không đụng weights/vendor/mã nguồn hoặc snapshot của ứng dụng khác.
    Xóa theo file nguyên vẹn nên lượng còn lại có thể ít hơn một nửa.
    """

    def __init__(self, directory, max_bytes=SNAPSHOT_MAX_BYTES):
        self.directory = Path(directory).absolute()
        if self.directory.is_symlink():
            raise ValueError("snapshot directory must not be a symlink")
        self.directory.mkdir(exist_ok=True)
        self.max_bytes = max_bytes

    def sweep(self):
        if self.directory.is_symlink():
            raise ValueError("snapshot directory changed to a symlink")
        root = self.directory.resolve()
        files = []
        errors = []
        for directory, subdirectories, names in os.walk(root, followlinks=False, onerror=errors.append):
            subdirectories[:] = [name for name in subdirectories if not (Path(directory) / name).is_symlink()]
            for name in names:
                path = Path(directory) / name
                try:
                    if path.is_symlink() or not path.is_file():
                        continue
                    info = path.stat()
                    files.append((info.st_mtime_ns, str(path), info.st_size))
                except OSError as exc:
                    errors.append(exc)
        total = sum(entry[2] for entry in files)
        remaining, deleted_files, deleted_bytes = total, 0, 0
        if total > self.max_bytes:
            target = total // 2
            for _, name, size in sorted(files):
                if remaining <= target:
                    break
                path = Path(name)
                try:
                    # Kiểm tra lại trước khi xóa, kể cả thư mục cha vừa đổi symlink.
                    path.resolve().relative_to(root)
                    if path.is_symlink() or not path.is_file():
                        continue
                    path.unlink()
                    remaining -= size
                    deleted_files += 1
                    deleted_bytes += size
                except (OSError, ValueError) as exc:
                    errors.append(exc)
        return {"before_bytes": total, "after_bytes": remaining,
                "deleted_files": deleted_files, "deleted_bytes": deleted_bytes,
                "errors": len(errors), "limit_bytes": self.max_bytes}


def main():
    load_dotenv(ROOT / ".env")  # Thông tin site/camera/broker chỉ nằm trong .env riêng.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=0, help="0: chạy liên tục; số dương: chạy thử")
    parser.add_argument("--camera", default=os.getenv("VMS_CAMERA_CODE"))
    parser.add_argument("--zone", type=int, default=os.getenv("VMS_ZONE_ID"), help="Zone lấy cấu hình từ broker")
    parser.add_argument("--organization", type=int, default=os.getenv("VMS_ORGANIZATION_ID"), help="Organization của camera trên IOC")
    parser.add_argument("--camera-id", default=os.getenv("VMS_CAMERA_ID"), help="UUID kỳ vọng; chặn nhầm camera trùng code ở IOC khác")
    parser.add_argument("--client-id", default=os.getenv("VMS_MQTT_CLIENT_ID", "tts_thien_01"),
                        help="Client ID riêng dạng tts_<ten>_01; chỉ chạy một instance cho mỗi ID")
    parser.add_argument("--detector-fps", type=float, default=30, help="Trần YOLO; chạy theo frame mới, sampler VD-MIL vẫn 8 FPS")
    parser.add_argument("--publish-fps", type=float, default=30, help="Trần gửi, không lặp bbox frame cũ; thực tế phụ thuộc FPS nguồn")
    parser.add_argument("--max-bbox-age-ms", type=float, default=200, help="Bỏ bbox quá cũ tính từ lúc đọc frame")
    parser.add_argument("--decoder-threads", type=int, default=1,
                        help="Thread decoder RTSP; 1 tránh giữ nhiều frame, 0 dùng tự động như bản cũ")
    parser.add_argument("--events", action="store_true", help="Cho phép upload ảnh và tạo sự kiện thật trên staging")
    parser.add_argument("--fight-threshold", type=float, default=.9)
    parser.add_argument("--confirm-seconds", type=float, default=1.)
    parser.add_argument("--reset-seconds", type=float, default=2.)
    parser.add_argument("--cooldown", type=float, default=60.)
    parser.add_argument("--event-limit", type=int, default=0, help="0: liên tục; số dương: trần sự kiện mỗi phiên chạy thử")
    parser.add_argument("--broker-host", default=os.getenv("VMS_MQTT_HOST"))
    parser.add_argument("--broker-port", type=int, default=443)
    parser.add_argument("--broker-ip", default=os.getenv("VMS_BROKER_IP") or None, help="IP kết nối riêng cho process; vẫn giữ broker-host cho TLS/Host WebSocket")
    parser.add_argument("--mqtt-transport", choices=["wss", "tcp"], default="wss",
                        help="wss: TLS443/domain; tcp: cổng broker nội bộ đã được xác nhận")
    args = parser.parse_args()
    if not args.camera or not args.broker_host or args.zone is None or args.organization is None:
        parser.error("Cần cấu hình camera, zone, organization, broker-host qua CLI hoặc .env")
    if args.seconds < 0 or any(c in args.camera for c in "/#+"):
        parser.error("seconds phải >= 0; camera không chứa / # +")
    if not args.client_id.startswith("tts_") or args.client_id.startswith("vms_staging_backend"):
        parser.error("client-id phải có prefix tts_ và không dùng prefix backend")
    if not (0 < args.detector_fps <= 30 and 0 < args.publish_fps <= 30 and 0 < args.max_bbox_age_ms <= 500):
        parser.error("detector/publish FPS phải thuộc (0,30]; max-bbox-age-ms thuộc (0,500]")
    if not 0 <= args.decoder_threads <= 64:
        parser.error("decoder-threads phải thuộc [0,64]")
    if not (.5 < args.fight_threshold <= 1 and args.confirm_seconds > 0 and args.reset_seconds > 0
            and args.cooldown >= 0 and args.event_limit >= 0):
        parser.error("Ngưỡng fight thuộc (.5,1], confirm/reset >0, cooldown/event-limit >=0")
    if args.broker_ip:
        try:
            ipaddress.ip_address(args.broker_ip)
        except ValueError:
            parser.error("broker-ip phải là IP hợp lệ")
        # Chỉ định tuyến domain broker trong process này. Tên TLS/SNI và HTTP
        # Host vẫn là domain broker được cấu hình; không tắt xác thực chứng chỉ, không đổi DNS OS.
        original_getaddrinfo = socket.getaddrinfo

        def broker_getaddrinfo(host, port, *values, **options):
            address = args.broker_ip if host == args.broker_host else host
            return original_getaddrinfo(address, port, *values, **options)

        socket.getaddrinfo = broker_getaddrinfo

    # Log chia file 20 MiB; tổng log/JSON >200 MiB sẽ dọn nửa cũ nhất mỗi 30s.
    output = ROOT / "output"
    output.mkdir(exist_ok=True)
    storage = StorageGuard(output)
    logger = logging.getLogger("ioc_bbox")
    logger.setLevel(logging.INFO)
    handler = BudgetLogHandler(output / "log.jsonl", storage)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    log_retention_status = handler.prune()

    def log(kind, **fields):
        row = {"time": datetime.now(timezone.utc).isoformat(), "kind": kind, **fields}
        line = json.dumps(row, allow_nan=False)
        logger.info(line)
        # Payload đầy đủ chỉ ghi file; journal giữ trạng thái/tổng hợp để dễ đọc.
        if kind not in {"bbox_send", "bbox_received_from_broker", "score"}:
            print(line, flush=True)

    if log_retention_status["deleted_files"] or log_retention_status["errors"]:
        log("log_cleanup", **log_retention_status)

    # Dọn ngay khi khởi động và mỗi 30s, không phụ thuộc camera/MQTT có kết nối.
    retention = SnapshotRetention(output / "snapshots")
    retention_status = retention.sweep()
    if retention_status["deleted_files"] or retention_status["errors"]:
        log("snapshot_cleanup", **retention_status)

    # Schema gốc là profile cục bộ; đặt đúng organization của lần chạy này.
    schema = json.loads((ROOT / "bbox.schema.json").read_text())
    schema["properties"]["organization_id"]["const"] = args.organization
    storage.write_json("effective_bbox.schema.json", schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    event_validator = Draft202012Validator(json.loads((ROOT / "event.schema.json").read_text()),
                                          format_checker=FormatChecker())

    def validate(payload):
        validator.validate(payload)
        for detection in payload["detections"]:
            x1, y1, x2, y2 = detection["bbox"]
            if x1 >= x2 or y1 >= y2:
                raise ValueError("bbox phải có x1<x2, y1<y2")
        json.dumps(payload, allow_nan=False)

    # lock bảo vệ trạng thái dùng chung giữa callback MQTT và các worker.
    lock = threading.Lock()
    stop = threading.Event()
    result_ready = threading.Event()  # YOLO đánh thức publisher ngay khi có frame mới.
    frame_ready = threading.Event()  # Capture đánh thức YOLO, tránh chờ theo chu kỳ riêng.
    connected = threading.Event()
    started = time.monotonic()
    state = {"camera": None, "url": None, "enabled": False, "revision": 0,
             "frame": None, "seq": 0, "captured": 0., "captured_wall": 0., "detections": None,
             "score": None, "score_stamp": 0., "fatal": None, "config_received": False,
             "decoder_threads": None, "source_fps": None, "module_enabled": False,
             "geometry_refresh_pending": False}
    geometry = AiZonesConfig(args.camera, args.camera_id)
    samples = deque(maxlen=8)
    counters = {"bbox_sent": 0, "broker_echo": 0, "invalid_received": 0,
                "scores": 0, "rtsp_reconnects": 0, "publish_errors": 0, "detections_completed": 0}
    pending_echo = {}  # timestamp của chính publisher này, phân biệt publisher khác cùng topic.
    # Trạng thái cố định giúp restart không cảnh báo lại ngay cùng vụ.
    event_ledger = {"active": True, "last_event_wall": 0.}
    if (output / "event_state.json").exists():
        try:
            event_ledger.update(json.loads((output / "event_state.json").read_text()))
        except (ValueError, OSError):
            log("event_state_invalid")  # Chờ điểm thấp trước khi mở một vụ mới.
    fight_gate = FightGate(args.fight_threshold, args.confirm_seconds, args.reset_seconds,
                           args.cooldown, bool(event_ledger["active"]), float(event_ledger["last_event_wall"]))
    event_jobs = queue.Queue(maxsize=2)  # Tối đa hai JPEG/frame pending, không tăng RAM vô hạn.
    counters.update(events_queued=0, events_pubacked=0, event_errors=0)
    last_activity = {"send": 0., "echo": 0., "detection": 0.}
    topic = "smart_vms/ai/bbox/" + args.camera
    client_id = args.client_id
    zones_topic = f"smart_vms/cameras/{args.camera}/ai_zones/{MODULE}"
    lines_topic = zones_topic + "/lines"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                         transport="websockets" if args.mqtt_transport == "wss" else "tcp", protocol=mqtt.MQTTv311)
    if args.mqtt_transport == "wss":
        client.ws_set_options(path="/mqtt")
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    username, password = os.getenv("VMS_MQTT_USERNAME"), os.getenv("VMS_MQTT_PASSWORD")
    if not username or not password:
        parser.error("Thiếu VMS_MQTT_USERNAME / VMS_MQTT_PASSWORD trong .env")
    client.username_pw_set(username, password)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.max_queued_messages_set(10)

    def request_config():
        client.publish("smart_vms/cameras/getdevice", json.dumps({"zone_id": args.zone, "organization_id": args.organization}), qos=1, retain=False)

    def on_connect(c, userdata, flags, reason, properties):
        log("mqtt_connect", reason=str(reason), client_id=client_id)
        if reason == 0:
            connected.set()
            c.subscribe([(f"smart_vms/cameras/zone/{args.zone}", 1),
                         (f"smart_vms/cameras/company/{args.organization}", 1), (topic, 0),
                         (zones_topic, 1), (lines_topic, 1)])

    def on_subscribe(c, userdata, mid, reasons, properties):
        if any(r.is_failure for r in reasons):
            log("mqtt_subscribe_failed")
        else:
            request_config()  # Gửi yêu cầu sau SUBACK, tránh bỏ lỡ trả lời cấu hình.

    def on_disconnect(c, userdata, flags, reason, properties):
        connected.clear()
        log("mqtt_disconnect", reason=str(reason))

    def on_connect_fail(c, userdata):
        # Lỗi mạng/TLS xảy ra trước CONNACK cũng phải xuất hiện trong log dịch vụ.
        connected.clear()
        log("mqtt_connect_failed", broker_host=args.broker_host, broker_port=args.broker_port,
            transport=args.mqtt_transport)

    def on_message(c, userdata, message):
        if message.topic in (zones_topic, lines_topic):
            try:
                kind = "lines" if message.topic == lines_topic else "polygons"
                with lock:
                    changed = geometry.update(message.payload, kind)
                    empty = not geometry.polygons if kind == "polygons" else not geometry.lines
                    # Vùng rỗng có thể do tắt module hoặc chưa cấu hình vùng. Tạm
                    # dừng, xin camera config mới để phân biệt; không đoán trạng thái.
                    if changed or (empty and kind == "polygons"):
                        state["revision"] += 1
                        state.update(frame=None, detections=None, score=None, captured=0., score_stamp=0.)
                        samples.clear()
                    if empty and kind == "polygons":
                        state.update(geometry_refresh_pending=True, enabled=False)
                    status = geometry.status()
                storage.write_json("zones.json", status)
                log("geometry_updated", topic=message.topic, changed=changed, empty=empty,
                    retained=message.retain, **status)
                if empty and kind == "polygons":
                    request_config()
            except (ValueError, TypeError, KeyError) as exc:
                log("geometry_invalid", topic=message.topic, error_type=type(exc).__name__)
            return
        if message.topic == topic:
            try:
                payload = json.loads(message.payload)
                validate(payload)
                with lock:
                    sent_monotonic = pending_echo.pop(payload["timestamp"], None)
                    own = sent_monotonic is not None
                    if own:
                        counters["broker_echo"] += 1
                        last_activity["echo"] = time.monotonic()
                # Đây là tin broker chuyển lại, không phải ACK của backend/DB.
                log("bbox_received_from_broker", topic=message.topic, correlated=own,
                    qos=message.qos, retained=message.retain, schema_valid=True, payload=payload,
                    broker_roundtrip_ms=round((time.monotonic() - sent_monotonic) * 1000, 2) if own else None)
            except Exception as exc:
                with lock:
                    counters["invalid_received"] += 1
                log("bbox_receive_invalid", error_type=type(exc).__name__)
            return
        try:
            data = json.loads(message.payload) if message.payload else {"cameras": []}
            if not isinstance(data, dict) or not isinstance(data.get("cameras"), list):
                return
            camera = next((cam for cam in data["cameras"] if isinstance(cam, dict)
                           and cam.get("code") == args.camera and cam.get("zone_id") == args.zone
                           and cam.get("organization_id", args.organization) == args.organization
                           and (not args.camera_id or cam.get("id") == args.camera_id)), None)
            url = ((camera.get("restream_urls") or {}).get(MODULE) or camera.get("rtsp_url")) if camera else None
            enabled = bool(camera and url and MODULE in camera.get("ai_modules", []))
            with lock:
                previous = (state["url"], state["enabled"], (state["camera"] or {}).get("id"))
                current = (url, enabled, (camera or {}).get("id"))
                if previous != current:
                    state["revision"] += 1
                    state.update(frame=None, detections=None, score=None, captured=0., score_stamp=0.)
                    samples.clear()
                state.update(camera=camera, url=url, enabled=enabled, module_enabled=enabled,
                             geometry_refresh_pending=False, config_received=True)
                geometry.camera_id = (camera or {}).get("id", args.camera_id)
            log("camera_config", camera=args.camera, enabled=enabled,
                camera_id=(camera or {}).get("id"), changed=previous != current)
        except Exception as exc:
            log("invalid_config", error_type=type(exc).__name__)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    client.on_connect_fail = on_connect_fail
    client.on_message = on_message

    def capture():
        while not stop.is_set():
            with lock:
                url, enabled, revision = state["url"], state["enabled"], state["revision"]
            if not enabled:
                stop.wait(.2)
                continue
            params = [cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 10000, cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000]
            # Đây là tham số lúc OPEN. Frame-threading nhiều luồng có thể giữ lại
            # nhiều frame trước khi read() trả ảnh; FPS vẫn đủ nhưng ảnh đã cũ.
            if args.decoder_threads:
                params += [cv2.CAP_PROP_N_THREADS, args.decoder_threads]
            cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG, params)
            if cap.isOpened():
                decoder_threads = int(cap.get(cv2.CAP_PROP_N_THREADS))
                source_fps = float(cap.get(cv2.CAP_PROP_FPS))
                with lock:
                    state.update(decoder_threads=decoder_threads, source_fps=source_fps)
                log("rtsp_opened", decoder_threads=decoder_threads, source_fps=source_fps)
            try:
                while not stop.is_set():
                    with lock:
                        if revision != state["revision"] or not state["enabled"]:
                            break
                    ok, frame = cap.read()
                    if not ok:
                        break
                    stamp = time.monotonic()
                    captured_wall = time.time()
                    with lock:
                        if revision == state["revision"]:
                            state.update(frame=frame, captured=stamp, captured_wall=captured_wall, seq=state["seq"] + 1)
                            frame_ready.set()
            finally:
                cap.release()
            with lock:
                counters["rtsp_reconnects"] += 1
            log("rtsp_reconnect")
            stop.wait(2)

    def sampler():
        # Nhịp lấy mẫu độc lập inference: không lặp frame cũ, không nối clip qua mất luồng.
        last_seq = -1
        while not stop.is_set():
            tick = time.monotonic()
            with lock:
                frame, seq, stamp, revision = state["frame"], state["seq"], state["captured"], state["revision"]
                enabled = state["enabled"]
            if enabled and frame is not None and seq != last_seq and tick - stamp < .4:
                small = cv2.resize(frame, (172, 172), interpolation=cv2.INTER_AREA)
                with lock:
                    if revision == state["revision"]:
                        if samples and stamp - samples[-1][1] > .4:
                            samples.clear()
                        samples.append((seq, stamp, small))
                        last_seq = seq
            stop.wait(max(0, .125 - (time.monotonic() - tick)))

    # Dùng CUDA thật nếu có; không áp dụng bản vá NMS Windows lên Linux.
    torch.set_num_threads(4)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    detector = YOLO(str(ROOT / "weights/yolo11n.pt"))
    classifier = MovinetClassifier(str(ROOT / "weights"),
                                   [str(ROOT / "weights/model_20.pt")], device=device)
    for net in classifier.classifier:
        net.to(device)
    log("models_ready", device=device, gpu=torch.cuda.get_device_name(0) if device == "cuda" else None,
        events_enabled=args.events, sample_fps=8, clip_frames=8, stride=4,
        detector_fps=args.detector_fps, publish_hz=args.publish_fps)

    def detect():
        last_seq = -1
        next_detect_at = 0.
        with torch.inference_mode():
            while not stop.is_set():
                delay = next_detect_at - time.monotonic()
                if delay > 0:
                    stop.wait(delay)
                frame_ready.wait(timeout=.1)
                frame_ready.clear()
                if stop.is_set():
                    break
                tick = time.monotonic()
                with lock:
                    frame, seq, stamp, revision = state["frame"], state["seq"], state["captured"], state["revision"]
                    enabled = state["enabled"]
                    captured_wall = state["captured_wall"]
                if not enabled or frame is None or seq == last_seq or tick - stamp > .4:
                    continue
                last_seq = seq
                infer_started = time.monotonic()
                next_detect_at = infer_started + 1 / args.detector_fps
                result = detector.predict(frame, classes=[0], conf=.35, imgsz=640,
                                          device=0 if device == "cuda" else "cpu", verbose=False,
                                          save=False, save_txt=False, save_crop=False)[0]
                h, w = frame.shape[:2]
                detections = []
                for i, box in enumerate(result.boxes):
                    xy = np.clip(box.xyxy[0].cpu().numpy() / np.array([w, h, w, h]), 0, 1)
                    if xy[0] < xy[2] and xy[1] < xy[3]:
                        detections.append({"id": f"detection_{i}", "class": "person",
                                           "confidence": float(box.conf[0]), "bbox": xy.tolist()})
                ready_at = time.monotonic()
                with lock:
                    if revision == state["revision"]:
                        state["detections"] = {"stamp": stamp, "seq": seq, "boxes": detections,
                                               "frame": frame,  # Ảnh thật cùng frame bbox; chỉ giữ latest trong RAM.
                                               "ready_at": ready_at, "captured_wall": captured_wall,
                                               "capture_to_inference_ms": (infer_started - stamp) * 1000,
                                               "inference_ms": (ready_at - infer_started) * 1000}
                        counters["detections_completed"] += 1
                        last_activity["detection"] = ready_at
                        result_ready.set()

    def classify():
        last_stamp = 0.
        gate_revision = -1
        with torch.inference_mode():
            while not stop.is_set():
                with lock:
                    clip, revision, enabled = list(samples), state["revision"], state["enabled"]
                # stride 4 tại 8 FPS tương đương khoảng 0.5 giây giữa hai điểm.
                if not enabled or len(clip) < 8 or clip[-1][1] - last_stamp < .45 or time.monotonic() - clip[-1][1] > .4:
                    stop.wait(.02)
                    continue
                last_stamp = clip[-1][1]
                score = float(classifier([[entry[2] for entry in clip]])[0, 0])
                if not math.isfinite(score):
                    raise ValueError("VD-MIL trả về điểm không hữu hạn")
                with lock:
                    if revision == state["revision"]:
                        state.update(score=score, score_stamp=last_stamp)
                        counters["scores"] += 1
                    event_camera, event_detection = state["camera"], state["detections"]
                    event_valid = revision == state["revision"] and state["enabled"]
                log("score", score=score, clip_span_seconds=round(clip[-1][1] - clip[0][1], 3))
                # Chỉ xét sự kiện khi có bbox YOLO thật còn mới cùng cửa sổ clip.
                if args.events and event_valid and event_detection and abs(event_detection["stamp"] - last_stamp) < .4:
                    if gate_revision != revision:
                        fight_gate.high_since = fight_gate.low_since = fight_gate.last_stamp = None
                        gate_revision = revision
                    with lock:
                        people, event_zone = geometry.event_people(event_detection["boxes"])
                    before_active = fight_gate.active
                    triggered = fight_gate.update(score, last_stamp, time.time(), people)
                    if before_active != fight_gate.active or triggered:
                        saved = storage.write_json("event_state.json", {
                            "active": fight_gate.active, "last_event_wall": fight_gate.last_event_wall})
                        if not saved and triggered:
                            log("event_skipped", reason="cannot_persist_dedup_state")
                            continue
                    if triggered and connected.is_set() and (not args.event_limit or counters["events_queued"] < args.event_limit):
                        event = {"ai_modules": MODULE, "submodule_code": MODULE,
                                 "camera_id": event_camera["id"], "camera_code": event_camera["code"],
                                 "event_time": datetime.fromtimestamp(event_detection["captured_wall"], timezone(timedelta(hours=7))).isoformat(),
                                 "entity_type": "PERSON", "entity_id": "incident_" + uuid.uuid4().hex,
                                 "confidence": score, "severity": "HIGH",
                                 "payload": {"type": "FIGHT", "person_count": people,
                                             "person_count_scope": "visible_people_in_ai_zone" if event_zone else "visible_people_in_full_frame",
                                             "threshold": args.fight_threshold, "confirm_seconds": args.confirm_seconds}}
                        if event_zone:
                            event["payload"]["zone_name"] = event_zone["name"]
                            if event_zone["id"]:
                                event["payload"]["zone_id"] = event_zone["id"]
                        try:
                            event_jobs.put_nowait((event, event_detection["frame"].copy(), revision, time.monotonic()))
                            with lock:
                                counters["events_queued"] += 1
                            log("fight_confirmed", entity_id=event["entity_id"], score=score, person_count=people)
                        except queue.Full:
                            log("event_skipped", reason="queue_full")

    def event_worker():
        # HTTP và chờ PUBACK chạy ngoài luồng bbox. Không lưu ảnh xuống đĩa.
        # Không tự phát lại khi kết quả publish không rõ: backend không dedup.
        with requests.Session() as session:
            while not stop.is_set():
                try:
                    event, frame, revision, queued_at = event_jobs.get(timeout=.2)
                except queue.Empty:
                    continue
                phase = "before_upload"
                try:
                    with lock:
                        valid = state["enabled"] and revision == state["revision"]
                    if not valid or not connected.is_set() or time.monotonic() - queued_at > 5:
                        raise RuntimeError("event_expired_or_camera_disabled")
                    ok, jpg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
                    if not ok or jpg.nbytes > 10 * 1024 * 1024:
                        raise ValueError("invalid_snapshot")
                    phase = "upload"
                    response = session.post(f"https://{args.broker_host}/storage/upload/file",
                                            files={"file": ("snapshot.jpg", jpg.tobytes(), "image/jpeg")},
                                            data={"camera_id": event["camera_id"], "category": "anomaly"}, timeout=(10, 30))
                    response.raise_for_status()
                    data = response.json()
                    key = data.get("object_key")
                    if data.get("success") is not True or not isinstance(key, str) or not key.startswith("anomaly/"):
                        raise ValueError("storage_response_invalid")
                    event["snapshot_url"] = key
                    log("snapshot_uploaded", camera=event["camera_code"], entity_id=event["entity_id"], object_key=key,
                        bytes=int(jpg.nbytes), http_status=response.status_code)
                    # Kiểm tra cấu hình lần cuối sau HTTP; không gửi event khi đã tắt module.
                    with lock:
                        valid = state["enabled"] and revision == state["revision"]
                    if not valid or not connected.is_set() or stop.is_set():
                        raise RuntimeError("disabled_or_offline_after_upload")
                    phase = "publish"
                    event_topic = "smart_vms/ai_events/" + MODULE
                    event_validator.validate(event)  # Kiểm tra profile cục bộ trước khi gửi lên broker.
                    body = json.dumps(event, allow_nan=False)
                    info = client.publish(event_topic, body, qos=1, retain=False)
                    log("event_publish", topic=event_topic, camera=event["camera_code"], object_key=key,
                        mqtt_mid=info.mid, publish_rc=int(info.rc), payload=event)
                    info.wait_for_publish(timeout=15)
                    if info.rc != mqtt.MQTT_ERR_SUCCESS or not info.is_published():
                        raise RuntimeError("publish_outcome_unknown_no_application_retry")
                    with lock:
                        counters["events_pubacked"] += 1
                    storage.write_json("latest_event.json", {"status": "broker_puback", "event": event,
                                                             "mqtt_mid": info.mid, "verified_in_vms": False})
                    log("event_puback", topic=event_topic, camera=event["camera_code"], object_key=key,
                        mqtt_mid=info.mid, entity_id=event["entity_id"], backend_saved="not_yet_verified")
                except Exception as exc:
                    with lock:
                        counters["event_errors"] += 1
                    log("event_failed", phase=phase, entity_id=event["entity_id"], error_type=type(exc).__name__,
                        reason=str(exc)[:160] if isinstance(exc, (ValueError, RuntimeError)) else None)
                finally:
                    event_jobs.task_done()

    def guarded(function):
        try:
            function()
        except Exception as exc:
            # systemd tự restart nếu bất cứ worker thiết yếu nào chết.
            log("worker_failed", worker=function.__name__, error_type=type(exc).__name__)
            with lock:
                state["fatal"] = function.__name__ + ":" + type(exc).__name__
            stop.set()

    def shutdown(signum, frame):
        stop.set()
        result_ready.set()
        frame_ready.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    threads = []
    last_health = 0.
    last_retention = time.monotonic()
    last_request = 0.
    previous_sent = 0
    previous_detections = 0
    previous_frames = 0
    next_publish_at = 0.
    last_sent_frame = None
    latest_payload = None
    log("starting", pid=os.getpid(), hostname=socket.gethostname(), camera=args.camera, topic=topic,
        qos=0, retain=False, events_enabled=args.events, seconds=args.seconds,
        fight_threshold=args.fight_threshold, confirm_seconds=args.confirm_seconds, cooldown=args.cooldown,
        broker_host=args.broker_host, broker_port=args.broker_port, transport=args.mqtt_transport,
        broker_connect_ip=args.broker_ip,
        zone_id=args.zone, organization_id=args.organization, expected_camera_id=args.camera_id)
    client.connect_async(args.broker_host, args.broker_port, 30)
    client.loop_start()
    try:
        for function in (capture, sampler, detect, classify, event_worker):
            thread = threading.Thread(target=guarded, args=(function,), name=function.__name__, daemon=True)
            thread.start()
            threads.append(thread)
        enabled_since = None
        while not stop.is_set() and (args.seconds == 0 or time.monotonic() - started < args.seconds):
            tick = time.monotonic()
            if tick - last_retention >= 30:
                log_retention_status = handler.prune()
                if log_retention_status["deleted_files"] or log_retention_status["errors"]:
                    log("log_cleanup", **log_retention_status)
                retention_status = retention.sweep()
                if retention_status["deleted_files"] or retention_status["errors"]:
                    log("snapshot_cleanup", **retention_status)
                last_retention = time.monotonic()
            with lock:
                camera, enabled, revision = state["camera"], state["enabled"], state["revision"]
                detection, captured = state["detections"], state["captured"]
                score, score_stamp = state["score"], state["score_stamp"]
            enabled_since = (enabled_since or tick) if enabled else None
            if connected.is_set() and tick - last_request > 60:
                request_config()
                last_request = tick
            if (connected.is_set() and enabled and detection and tick >= next_publish_at
                    and (revision, detection["seq"]) != last_sent_frame and tick - captured < 1
                    and tick - detection["stamp"] < args.max_bbox_age_ms / 1000):
                score_fresh = score is not None and tick - score_stamp < 2
                payload = {"camera_id": camera["id"], "camera_code": camera["code"],
                           "organization_id": args.organization, "ai_modules": MODULE, "timestamp": time.time(),
                           "detections": detection["boxes"], "hud": {"text": f"VD-MIL score {score:.3f}" if score_fresh else "VD-MIL warming up",
                                                                  "display_ms": 500, "color": "#f59e0b"}}
                validate(payload)
                with lock:
                    # Kiểm tra lại revision để tránh gửi cấu hình cũ sau hot-toggle.
                    if revision != state["revision"] or not state["enabled"]:
                        continue
                    published_at = time.monotonic()
                    pending_echo[payload["timestamp"]] = published_at
                    while len(pending_echo) > 512:
                        pending_echo.pop(next(iter(pending_echo)))
                info = client.publish(topic, json.dumps(payload, allow_nan=False), qos=0, retain=False)
                next_publish_at = published_at + 1 / args.publish_fps
                last_sent_frame = (revision, detection["seq"])
                with lock:
                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        counters["bbox_sent"] += 1
                        last_activity["send"] = tick
                    else:
                        counters["publish_errors"] += 1
                latest_payload = payload
                log("bbox_send", topic=topic, qos=0, retain=False, schema_valid=True, payload=payload,
                    violence_score=score if score_fresh else None, publish_rc=int(info.rc), mqtt_mid=info.mid,
                    result_age_ms=round((published_at - detection["stamp"]) * 1000, 1),
                    frame_seq=detection["seq"], frame_captured_at=detection["captured_wall"],
                    capture_to_inference_ms=round(detection["capture_to_inference_ms"], 2),
                    inference_ms=round(detection["inference_ms"], 2),
                    publisher_wait_ms=round((published_at - detection["ready_at"]) * 1000, 2))
            if tick - last_health >= 10:
                with lock:
                    health = {**counters, "time": datetime.now(timezone.utc).isoformat(), "pid": os.getpid(),
                              "mqtt_connected": connected.is_set(), "config_received": state["config_received"],
                              "camera": args.camera, "enabled": enabled, "device": device, "events_enabled": args.events,
                              "event_config": {"threshold": args.fight_threshold, "confirm_seconds": args.confirm_seconds,
                                               "reset_seconds": args.reset_seconds, "cooldown": args.cooldown,
                                               "limit": args.event_limit, "active_incident": fight_gate.active},
                              "organization_id": args.organization, "zone_id": args.zone,
                              "broker_host": args.broker_host, "broker_port": args.broker_port,
                              "broker_connect_ip": args.broker_ip,
                              "client_id": client_id, "geometry": geometry.status(),
                              "detector_target_fps": args.detector_fps, "publish_target_fps": args.publish_fps,
                              "decoder_threads": state["decoder_threads"], "source_fps": state["source_fps"],
                              "detection_hz": round((counters["detections_completed"] - previous_detections) / (tick - last_health), 2) if last_health else 0,
                              "capture_hz": round((state["seq"] - previous_frames) / (tick - last_health), 2) if last_health else 0,
                              "score": score, "score_age_seconds": round(tick - score_stamp, 2) if score_stamp else None,
                              "frame_age_seconds": round(tick - captured, 2) if captured else None,
                              "last_send_age_seconds": round(tick - last_activity["send"], 2) if last_activity["send"] else None,
                              "last_echo_age_seconds": round(tick - last_activity["echo"], 2) if last_activity["echo"] else None,
                              "send_hz": round((counters["bbox_sent"] - previous_sent) / (tick - last_health), 2) if last_health else 0}
                    previous_sent = counters["bbox_sent"]
                    previous_detections = counters["detections_completed"]
                    previous_frames = state["seq"]
                # Khi thiếu dung lượng, dừng ghi xuống ổ đĩa nhưng vẫn xử lý RAM/MQTT.
                health["storage"] = {"snapshot_saving": False, "video_saving": False,
                                     "log_limit_mib": 200, "min_free_mib": 2048,
                                     "log_json": log_retention_status,
                                     "snapshots": retention_status,
                                     "denied_writes": storage.denied_writes,
                                     "disk_free_mib": round(shutil.disk_usage(output).free / 1024**2, 1)}
                storage.write_json("health.json", health)
                if latest_payload:
                    storage.write_json("latest_payload.json", latest_payload)
                log("health", **health)
                last_health = tick
            # Quá 120s không có cấu hình, hoặc camera bật mà 90s không có bbox mới:
            # thoát lỗi để systemd khởi động lại. Module bị tắt thì chờ, không restart.
            with lock:
                no_config = not state["config_received"] and tick - started > 120
                stalled = enabled_since is not None and tick - max(enabled_since, last_activity["detection"]) > 90
            if no_config or stalled:
                raise RuntimeError("camera config timeout" if no_config else "no fresh detections for 90s")
            # Đã đủ thời gian giữa hai lần publish: đợi kết quả mới thay vì ngủ
            # cứng 200ms. Chưa tới hạn: chờ ngắn rồi lấy kết quả mới nhất.
            remaining = next_publish_at - time.monotonic()
            if remaining > 0:
                stop.wait(min(remaining, .1))
            else:
                result_ready.wait(timeout=.1)
                result_ready.clear()
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=15)
        client.disconnect()
        client.loop_stop()
        log("stopped", events_enabled=args.events, **counters)
    if state["fatal"]:
        raise RuntimeError(state["fatal"])


if __name__ == "__main__":
    main()
