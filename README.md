# FIGHT_DETECTION → AIOC/VMS qua MQTT

YOLO11n phát hiện người; VD-MIL/MoViNet A0 chấm điểm toàn clip. Giữ nguyên
toàn ảnh làm baseline. `run.py` chứa chương trình chính, không gọi `common.py`.

## Luồng dữ liệu

1. Kết nối WSS TLS `<broker-host>:443/mqtt`, subscribe camera zone/company;
   sau SUBACK gửi `getdevice`. Mất MQTT thì tự reconnect/subscribe/request lại.
2. Chỉ xử lý camera bật `FIGHT_DETECTION`; lấy RTSP từ cấu hình MQTT.
3. Decoder 1 thread, latest frame → YOLO người; sampler 8 FPS/8 frame → VD-MIL.
4. Bbox chuẩn hóa 0–1 + HUD điểm: `smart_vms/ai/bbox/{camera_code}`, QoS0,
   retain=false; trần 30Hz, nguồn thử nghiệm khoảng 15FPS nên thực gửi khoảng 15Hz.
5. Với `--events`: điểm ≥0.9 liên tục ≥1 giây, ít nhất 2 người detection thật
   → một vụ. Upload JPEG trong RAM lên `/storage/upload/file`, category=anomaly.
6. Dùng `object_key` làm `snapshot_url`; validate `event.schema.json`, publish
   QoS1 lên `smart_vms/ai_events/FIGHT_DETECTION`, ghi log PUBACK.
7. Điểm <0.5 liên tục 2 giây mới mở lại vụ; cooldown 60 giây. Trạng thái chống
   lặp lưu cố định qua restart. Không retry ứng dụng khi kết quả publish mơ hồ.
8. Kiểm tra riêng lịch sử VMS và ảnh để xác nhận backend đã lưu: PUBACK chỉ là
   xác nhận broker, không phải ACK nghiệp vụ.

`person_count` là số người YOLO nhìn thấy trong toàn ảnh, **không phải** số người
tham gia đánh nhau. Payload ghi rõ `person_count_scope`. VD-MIL score không phải
xác suất đã hiệu chỉnh hoặc độ chính xác. Bbox là người, điểm fight là toàn clip.

## Chạy

Python 3.12/Linux là môi trường đã kiểm chứng trên server. Cài PyTorch và
torchvision tương thích CPU/CUDA từ hướng dẫn chính thức, sau đó:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python download_weights.py
cp .env.example .env
# Điền broker, tài khoản và camera/zone/organization được cấp vào .env.
python run.py --seconds 60
```

Mặc định chỉ bbox/điểm. Bật sự kiện thật:

```bash
python run.py --seconds 0 \
  --events --fight-threshold 0.9 --confirm-seconds 1 --reset-seconds 2 --cooldown 60
```

Không chạy thêm publisher cùng camera nếu dịch vụ server đang hoạt động.
Nếu hạ tầng cần đích TCP riêng, dùng `--broker-ip` hoặc VMS_BROKER_IP;
TLS/SNI/HTTP Host vẫn là domain broker và vẫn kiểm tra chứng chỉ.
`--event-limit 1` giới hạn một sự kiện mỗi phiên kiểm tra; 0 là liên tục.

Môi trường server đã kiểm chứng: torch 2.14.1+cu132, torchvision 0.29.1+cu132,
OpenCV 5.0.0.93; các dependency còn lại trong requirements. Cần cài torch /
torchvision phù hợp với driver thực tế, không trộn hai phiên bản không tương thích.

## Dịch vụ chạy liên tục

`deployment/ioc-fight-bbox.service` là template cho checkout tại
`~/projects/Detect-Fight-Service-AI---AIOC`. Chỉnh WorkingDirectory/ExecStart
theo đường dẫn thực tế, điền `.env` trong thư mục chứa run.py rồi cài unit cho
user. Không chạy hai publisher cùng camera. Bật --events có thể tạo thông báo
và SOP thật tùy cấu hình backend.

## Log và dung lượng

- `output/log.jsonl`: `fight_confirmed`, `snapshot_uploaded`, `event_publish`,
  `event_puback`, `event_failed`, cùng bbox_send/broker echo.
- `latest_event.json`: payload sự kiện mới nhất và trạng thái broker_puback.
  `verified_in_vms=false` cho tới khi kiểm chứng bên ngoài; ứng dụng không giả
  định PUBACK là đã vào lịch sử.
- `health.json`: MQTT, FPS, số event queue/PUBACK/lỗi, cấu hình xác nhận/cooldown.
- Không lưu JPEG hoặc video cục bộ. Log/JSON >200MiB → xóa file cũ nhất đến
  ≤50% dung lượng trước dọn, kiểm tra 30s. Giữ trạng thái dedup hiện tại.
- Snapshot directory có cùng chính sách 200MiB; thiếu 2GiB trống thì chặn ghi
  mới. Ảnh đã upload nằm trên Storage của VMS, không thuộc cleanup local.

## Kiểm tra

```bash
python -m unittest -v test_events test_storage
python measure_latency.py --window 60 --instrumented
```

13 tests đã pass trên môi trường Linux cho cả bản triển khai và bản public dùng .env.
Bộ storage tests có symlink và thao tác file tạm; Windows sandbox của phiên này
không chạy qua đầy đủ bộ đó. 6 tests gate đã pass ở local. Test không gửi event
giả lên staging. `browser_latency_probe.js` là công cụ tùy chọn, cần điền mật
khẩu MQTT khi chạy; các phép đo không đồng bộ đồng hồ không phải latency tuyệt đối.

## Nghiệm thu

Đã kiểm tra chuỗi upload ảnh → MQTT → lịch sử VMS và mở snapshot thành công.
Dữ liệu camera, định danh và log minh chứng giữ riêng ngoài repo public.
Xem `EVENT_VERIFICATION.md` để biết cách kiểm tra lại.

Chưa có đánh giá Precision/Recall/F1 trên dữ liệu có nhãn, chưa hoàn tất nhận
polygon/lines MQTT (C03), và bản hiện tại triển khai bằng systemd/venv, chưa
nghiệm thu Docker. Độ lệch bbox/video vẫn cần đo timeline chung trên IOC.

## Nguồn và giấy phép

- VD-MIL của Roger Figueroa Quintero:
  https://github.com/roggerfq/violence-detection-mil — CC BY-NC 4.0 theo header
  mã nguồn upstream; giữ attribution trong vendor. Chỉ dùng trong phạm vi được cấp phép.
- MoViNet runtime: https://github.com/Atze00/MoViNet-pytorch — giữ LICENSE upstream.
- YOLO/Ultralytics: https://github.com/ultralytics/ultralytics — tuân thủ giấy phép
  upstream. Repo này không cấp lại giấy phép cho mã/trọng số bên thứ ba.
- Trọng số tải riêng có kiểm SHA256; không commit .env, ảnh camera, log hoặc video.
