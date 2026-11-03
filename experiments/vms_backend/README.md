# Test MQTT -> VMS backend -> SOP, từng case độc lập

Mỗi folder có run.py chứa toàn bộ chương trình và comment luồng chạy; không
import common.py hoặc gọi run(i). Không cần model, PyTorch hay GPU. Bbox/event
là DỮ LIỆU MẪU để học backend, không phải detection và không đánh giá model.

Case03/04/05/07/08 mặc định xem trước; thêm --send mới ghi dữ liệu thật.
Case01/02/06 chỉ quan sát. Request getdevice không tạo sự kiện. Từng publish
event được thực hiện một lần, không retry ứng dụng khi ACK không rõ.

## Chuẩn bị và phạm vi

Chạy các lệnh tại thư mục này:

```bash
python -m pip install -r requirements.txt
cp .env.example .env
```

Điền broker, tài khoản, zone, code, UUID, module được cấp vào .env. PowerShell
dùng Copy-Item thay cp nếu cần. Python3.10+. VMS_BROKER_IP tùy chọn để đi qua
IP nội bộ đã kiểm chứng; không tắt TLS. Không đoán organization từ URL RTSP.

Theo tài liệu ban đầu, atin_tts chỉ có quyền zone51. Xem được MQTT/RTSP zone279
chưa chứng minh quyền ghi/xem lịch sử/SOP zone279. Chỉ --send trên camera và
SOP test được cấp. Script không tự sửa vùng/module/SOP hoặc dừng worker khác.
Dừng publisher cùng camera nếu cần quan sát bbox test riêng.

## 01 - Camera/config/organization

```bash
python 01_camera_config/run.py --discover-organization
```

Đọc retained zone và company/+ để tìm đúng camera theo code/UUID/zone. Khi
biết organization, gửi getdevice sau SUBACK. Cache ở
01_camera_config/output/camera.json; danh sách ở cameras.json, không lưu URL
credentials. Bỏ --discover-organization nếu đã biết org. Chỉ dùng khám phá
company trong phạm vi được cấp. Nếu chưa có UUID, để trống và lấy từ code/zone.
Case sau dùng cache cho preview, khi --send vẫn kiểm camera qua MQTT.
Getdevice không có correlation ID: không coi retained là bằng chứng mới tuyệt đối.

## 02 - Vùng/vạch cập nhật nóng

```bash
python 02_geometry/run.py --seconds 120
```

Tự sửa vùng test trên VMS trong120giây, Lưu và xem geometry changed=true.
Payload rỗng ghi empty=true; payload sai giữ file tốt trước đó. Script KHÔNG
publish config giả/xóa vùng. Xem output/polygons.json và lines.json. Rỗng
không đủ kết luận module tắt; camera config mới là nguồn trạng thái module.

## 03 - Bbox mẫu trên giao diện

```bash
python 03_bbox/run.py --seconds 10 --hz 5
python 03_bbox/run.py --seconds 10 --hz 5 --send
```

Mở VMS -> Giám sát trực tiếp -> camera đúng -> module đúng -> bật Phát hiện AI.
Khung cố định, HUD MQTT TEST BBOX - synthetic, QoS0, retainFalse. Cần biết
organization đúng và camera bật module. Broker echo không xác nhận UI hiển thị;
bạn tự đối chiếu giao diện. Case này không publish sự kiện lịch sử.

## 04 - Upload snapshot và tải lại

VLC -> Video -> Take Snapshot. Ảnh JPG/PNG <=10MB phải có trên MÁY CHẠY PYTHON;
Python trên server SSH không đọc trực tiếp ổ C của laptop.

```bash
python 04_snapshot/run.py --image /duong/dan/snapshot.jpg
python 04_snapshot/run.py --image /duong/dan/snapshot.jpg --send
```

Upload một lần, lấy object_key, GET ảnh lại qua snapshot API, so SHA256.
Xem output/snapshot.json và retrieved_snapshot.jpg/png. Case05/07/08 tự đọc
key từ đây. Key được lưu ngay sau upload, dù GET sau đó lỗi; không cần upload
lặp chỉ để lấy lại key. Đổi camera phải upload lại đúng camera.

## 05 - Một event mẫu vào lịch sử

```bash
python 05_event_history/run.py
python 05_event_history/run.py --send
```

FIGHT dùng --person-count 2 (giá trị MẪU). PLATE: đặt module/camera tương ứng,
thêm --plate TEST0001. Event có entity_id mqtt_test_..., event_time+07:00,
snapshot_url=object_key, QoS1/retainFalse. Có thể tạo history/notification/SOP
thật. Đối chiếu output/last_event.json, last_result.json với lịch sử và ảnh
VMS. PUBACK KHÔNG phải backend_saved; không gọi event mẫu là model phát hiện.

## 06 - Theo dõi SOP, không publish lệnh thiết bị

Chuẩn bị SOP test đang bật, điều kiện khớp camera/module; bắt đầu bằng nhánh
ghi log hoặc thiết bị giả lập. Không suy ra SOP đã chạy chỉ vì có event.
Lấy topic đích cụ thể từ rule/thiết bị TEST; không đoán device UUID.

Terminal A, thay ID_THIET_BI_TEST bằng ID được cấp:

```bash
python 06_sop_trace/run.py --seconds 120 --sop-topic smart_vms/iot/ID_THIET_BI_TEST/command
```

Terminal B, sau SUBACK của A:

```bash
python 05_event_history/run.py --send
```

Các dạng topic khác: smart_vms/barrier/ID/command, kcn/speaker/device/ID,
kcn/speaker/broadcast/ZONE. Có thể thêm nhiều --sop-topic; yêu cầu topic cụ thể,
không wildcard. --incident-key KEY lọc incident đã biết. Case06 KHÔNG publish
event hoặc OPEN/loa/barrier. Nếu không có topic (SOP chỉ ghi log/HTTP), chạy
không --sop-topic rồi tra lịch sử SOP và alert_dispatch_log được cấp quyền.
0 command không đủ kết luận SOP không chạy; topic đúng cũng chưa xác nhận
thiết bị đã thực thi. Event echo chỉ là tin trên broker.

Trên MÁY CHẠY VMS BACKEND, dùng đúng container thực tế:

```bash
docker logs --since 5m aioc-vms-staging-backend > backend_test.log 2>&1
```

Nếu đã copy log về máy chạy Python:

```bash
python 06_sop_trace/run.py --seconds 10 --backend-log-file /duong/dan/backend_test.log
```

Không có SSH/DB credential hoặc endpoint SOP được đoán trong bộ test.

## 07 - Từng case từ chối

```bash
python 07_rejection/run.py --case invalid-json
python 07_rejection/run.py --case invalid-json --send
python 07_rejection/run.py --case invalid-type --send
python 07_rejection/run.py --case unknown-camera --send
python 07_rejection/run.py --case unsupported-module --send
```

invalid-type chỉ cho mẫu FIGHT; PLATE không validate payload.type theo cách
đó. unknown-camera đổi cả UUID và code để tránh fallback về camera thật.
Mỗi lần chỉ gửi một message, không tự retry.

Tự tắt module CAMERA TEST trên VMS, chờ đồng bộ (tài liệu nêu có thể tới1phút):

```bash
python 07_rejection/run.py --case disabled-module --send
```

Chỉ gửi nếu camera config nhận được cho thấy module không bật. Sau test tự
bật lại trạng thái ban đầu. Mỗi case phải đối chiếu log và số event trước/sau;
PUBACK không chứng minh backend đã reject. Trạng thái thay đổi giữa test hoặc
backend khác hợp đồng có thể cho kết quả khác: ghi thực tế, không tự đánh pass.

## 08 - Hai bản tin giống hệt để hiểu dedup

Chỉ dùng camera/SOP test:

```bash
python 08_duplicate/run.py --gap 0.25
python 08_duplicate/run.py --gap 0.25 --send
```

Cùng body, entity_id và event_time, đúng2publish. Xem hai mqtt_mid rồi đếm
event ID, notification, số lần SOP. Theo tài liệu, FIGHT/ANOMALY không dedup
backend, PLATE có logic theo camera/biển/thời gian. Không thêm loop hàng loạt.

## Kết quả và kiểm tra offline

Mỗi case có output/log.jsonl và JSON tên cố định. Log tối đa3file x2MiB/case;
không lưu video dài hoặc RTSP credentials; output/.env không commit. Cleanup
local không xóa ảnh đã upload trên Storage VMS.

```bash
python -m unittest -v test_safety
python 05_event_history/run.py --help
```

Đã kiểm tra14unittest ở local: preview không gửi mạng, một event đúng1publish,
duplicate đúng2publish cùng body, SOP không publish command, ảnh sai camera/
module bị chặn, upload một lần và giữ object_key khi GET lỗi. Kiểm tra đọc
config/geometry và kết nối listener đã chạy với broker thật; chưa dùng suite
này để gửi bbox/upload/event/SOP thật. Những kết quả đó cần bạn nghiệm thu
theo từng bước trên UI và log backend.

Nguồn hợp đồng: docs/AI_MODULES_MQTT.md ở project nguồn, §1.1 ACK, §4 Storage,
§5.3 PLATE, §5.7 ANOMALY, §5.8 bbox, §6.8 SOP, §7 notification trùng.
