# 8 bài test backend - bản đơn giản

Mỗi folder có run.py độc lập. Không argparse, LOGGER, logging handler,
class riêng, common.py hay run(i). Dùng print() và vài file JSON tên cố định;
không tích lũy log. Bbox/event là dữ liệu MẪU học backend, không phải detection.

## Cấu hình và chạy

```bash
python -m pip install -r requirements.txt
cp .env.example .env
```

Giữ tài khoản/camera/zone/organization trong .env ở thư mục này. Case01 giúp
xem UUID/organization để điền nếu thiếu. Không đoán org từ URL RTSP.
VMS_BROKER_IP tùy chọn đi qua IP nội bộ; vẫn kiểm TLS theo VMS_MQTT_HOST.
Quyền đọc RTSP/MQTT chưa chứng minh quyền ghi/xem SOP; dùng camera/SOP test
được cấp. Script không tự sửa cấu hình VMS hoặc dừng worker khác.

Sửa các biến ở đầu run.py, rồi chạy lệnh tương ứng từ thư mục suite:

| Bài | Biến chỉnh | Lệnh |
|---|---|---|
|01 Camera|SECONDS|python 01_camera_config/run.py|
|02 Geometry|SECONDS|python 02_geometry/run.py|
|03 Bbox mẫu|SEND, SECONDS, FPS|python 03_bbox/run.py|
|04 Snapshot|SEND, IMAGE|python 04_snapshot/run.py|
|05 Một event|SEND|python 05_event_history/run.py|
|06 SOP listener|SECONDS, SOP_TOPICS|python 06_sop_trace/run.py|
|07 Từ chối|SEND, CASE|python 07_rejection/run.py|
|08 Trùng|SEND, GAP|python 08_duplicate/run.py|

Không dùng --seconds, --send, --case nữa. SEND=False xem trước; đổi True
khi muốn ghi thật. Không commit .env. Client ID mỗi lần chạy được tạo riêng.

## Thử lần lượt

1. Case01 đọc zone/company; chỉ lưu code, UUID, zone, organization, module,
   status; không lưu URL chứa mật khẩu. Nếu chưa biết org thì đọc company/+
   để tìm đúng camera trong phạm vi được cấp. Điền UUID/org vào .env nếu thiếu.
2. Case02 chạy SECONDS=300 rồi tự sửa polygon trên VMS và Lưu. Tìm changed=true.
   Bản đầu changed=false. So sánh trực tiếp list/dict, không tạo chữ ký JSON.
   output/polygons.json và lines.json giữ cấu hình hợp lệ gần nhất. Payload
   rỗng xóa danh sách; dữ liệu sai không ghi đè file tốt. Chọn đúng code/UUID/
   module trong .env từ case01; case02 chỉ nghe hai topic vùng/vạch.
3. Case03 SEND=True: VMS -> đúng camera/module -> bật Phát hiện AI. Khung mẫu
   và HUD MQTT TEST BBOX - synthetic, QoS0; không tạo sự kiện lịch sử.
4. Case04: VLC -> Video -> Take Snapshot; chép JPG/PNG lên MÁY CHẠY PYTHON,
   đặt tại snapshot.jpg cạnh README này hoặc sửa IMAGE. SEND=True upload một
   lần, GET lại so SHA256. Key lưu ở 04_snapshot/output/snapshot.json cho
   case05/08. GET lỗi vẫn giữ key; không tự upload lại. Đổi camera/module thì
   chạy lại case04. Ảnh phải <=10MB.
5. Case05 SEND=True tạo đúng một event MẪU, entity_id mqtt_test_..., thời gian
   +07:00, ảnh từ case04. Có thể kích SOP. Đối chiếu event_time/camera/entity_id
   trong lịch sử và mở ảnh. PUBACK chỉ xác nhận broker, không phải backend/SOP.
6. Case06: chuẩn bị SOP test khớp điều kiện, ưu tiên nhánh ghi log/thiết bị giả
   lập. Sửa SOP_TOPICS bằng topic thiết bị TEST lấy từ rule, ví dụ:

   ```python
   SOP_TOPICS = ["smart_vms/iot/ID_THIET_BI_TEST/command"]
   ```

   Terminal A chạy case06; sau SUBACK, terminal B chạy case05 có SEND=True.
   Topic dạng khác: smart_vms/barrier/ID/command, kcn/speaker/device/ID,
   kcn/speaker/broadcast/ZONE. Case06 chỉ nghe, không publish OPEN/loa/barrier.
   Đối chiếu incident_key/camera và lịch sử SOP/alert_dispatch_log. Nhận command
   chưa chứng minh thiết bị đã làm xong. Không có command cũng chưa chứng minh
   SOP không chạy vì SOP có thể chỉ ghi log/HTTP. SOP_TOPICS=[] chỉ nghe event.
7. Case07 chọn CASE: invalid-json, invalid-type (chỉ FIGHT), unknown-camera,
   unsupported-module. SEND=True gửi một bản tin. Với disabled-module, tự tắt
   module CAMERA TEST trên VMS, chờ đồng bộ rồi chạy; nhớ bật lại sau test.
   unknown-camera đổi cả UUID/code để tránh fallback. Case07 không cần ảnh.
   Phải đối chiếu log backend/lịch sử; PUBACK không chứng minh đã reject.
8. Case08 SEND=True gửi đúng hai message cùng body/entity_id/event_time cách
   GAP giây. Đếm event ID, notification, lần SOP. Theo tài liệu FIGHT/ANOMALY
   không dedup backend, PLATE có luật riêng. Chỉ thử camera/SOP test, không loop.

Case03/04/05/07/08 kiểm camera/module từ company topic trước khi ghi thật.
Không retry ứng dụng khi kết quả publish không rõ. .env và output không commit.
Không dùng JSONL logger nữa; output cũ vẫn giữ nếu tồn tại. Cleanup local
không xóa ảnh đã upload trên Storage VMS.

Trên MÁY CHẠY VMS BACKEND, dùng đúng container và quyền được cấp:

```bash
docker logs --since 5m -f aioc-vms-staging-backend
```

Đây không mặc định là server AI. Bộ test không tự tạo/sửa SOP, không có
SSH/DB credential backend hoặc API SOP đoán sẵn.

## Kiểm tra chương trình không ghi nhầm

```bash
python -m unittest -v test_safety
```

Các unit test dùng MQTT/HTTP giả lập, không tạo event thật. Nguồn hợp đồng:
docs/AI_MODULES_MQTT.md ở project nguồn: §4 Storage, §5.3 PLATE, §5.7 ANOMALY,
§5.8 bbox, §6.8 SOP, §7 notification/dedup.
