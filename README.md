# FIGHT_DETECTION → AIOC/VMS qua MQTT

YOLO11n phát hiện người; VD-MIL/MoViNet A0 chấm điểm toàn clip. Giữ nguyên
toàn ảnh làm baseline. `run.py` chứa chương trình chính, không gọi `common.py`.

## Luồng dữ liệu

1. Kết nối WSS TLS `<broker-host>:443/mqtt`, subscribe camera zone/company;
   sau SUBACK gửi `getdevice`. Mất MQTT thì tự reconnect/subscribe/request lại.
2. Chỉ xử lý camera bật `FIGHT_DETECTION`; lấy RTSP từ cấu hình MQTT.
   Subscribe thêm `ai_zones/FIGHT_DETECTION` và `/lines` cho camera đó;
   thay vùng/vạch theo retained cập nhật, không restart. Payload rỗng xóa vùng;
   xin lại camera config để phân biệt chưa có vùng với module đã tắt.
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

`person_count` là số người YOLO nhìn thấy trong toàn ảnh hoặc cùng polygon cấu
hình, **không phải** số người tham gia đánh nhau. Payload ghi rõ `person_count_scope`. VD-MIL score không phải
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
Container đã kiểm chứng dùng torch2.10.0+cu128/torchvision0.25.0+cu128; số liệu
đánh giá400clip được đo trong chính container đó.

## Dịch vụ chạy liên tục

### Docker GPU

```bash
cp .env.example .env
# Điền cấu hình được cấp; client ID riêng, ví dụ tts_thien_01.
python download_weights.py
mkdir -p output
docker compose build
docker compose up -d
docker compose ps
docker compose logs --tail 30 fight
```

`docker-compose.yml` mount weights chỉ đọc và output để giữ trạng thái qua
restart. Tài khoản nằm trong `.env`/env_file, không COPY vào image. User1000,
log Docker tối đa2file×5MiB, log/JSON ứng dụng vẫn theo policy200MiB dọn50%.
Nếu user host khác1000, điều chỉnh user/ownership output phù hợp trước chạy.

Đã build/run GPU thật, thử inference YOLO+VD-MIL và nghiệm thu worker nhận
config từ VMS, gửi bbox và sự kiện. Không bật đồng thời bản systemd cùng camera.
`deployment/migrate_docker.py` hỗ trợ chuyển từ unit cũ và rollback khi worker
mới không tạo được health/bbox. Image mặc định dựa trên PyTorch CUDA public;
`FIGHT_BASE_IMAGE` cho phép chọn base CUDA tương thích đã có ở máy triển khai.

### Systemd/venv (phương án thay thế)

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

Bộ 8 bài thử backend độc lập, từ cấu hình camera đến bbox, snapshot, lịch sử,
SOP và dedup: [experiments/vms_backend/README.md](experiments/vms_backend/README.md).
Mỗi case có `run.py` riêng, không cần GPU/model; preview mặc định trước khi
gửi dữ liệu mẫu bằng `--send`. Không dùng event mẫu để đánh giá detection.

```bash
python -m unittest -v test_geometry test_events test_storage
python measure_latency.py --window 60 --instrumented
```

21 tests đã pass trên môi trường Linux và trong container: 8 geometry, 6 gate,
7 storage. `test_mqtt_integration.py` còn chạy model thật với broker loopback
cô lập: retained, sửa/xóa vùng, tombstone/JSON sai, bật/tắt, restart broker,
reconnect/resubscribe/request lại; không restart worker và không phát event giả.
Bộ storage tests có symlink và thao tác file tạm; Windows sandbox của phiên này
không chạy qua đầy đủ bộ đó. 6 tests gate đã pass ở local. Test không gửi event
giả lên staging. `browser_latency_probe.js` là công cụ tùy chọn, cần điền mật
khẩu MQTT khi chạy; các phép đo không đồng bộ đồng hồ không phải latency tuyệt đối.

## Nghiệm thu

Đã kiểm tra chuỗi upload ảnh → MQTT → lịch sử VMS và mở snapshot thành công.
Dữ liệu camera, định danh và log minh chứng giữ riêng ngoài repo public.
Xem `EVENT_VERIFICATION.md` để biết cách kiểm tra lại.

Đã nghiệm thu bật/tắt module và sửa polygon trực tiếp trên VMS: container
dừng/tiếp tục inference và bbox đúng trạng thái, cập nhật vùng mà không restart.
Vùng rỗng, lines rỗng và reconnect được kiểm tra thêm trên broker test riêng.

Đã đánh giá 400 clip val có nhãn: chấm điểm clip Accuracy88%, Precision91.30%,
Recall84%, F1 87.50%. Luật cảnh báo 1s+≥2người có Precision95.89%, Recall35%.
Xem [EVALUATION.md](EVALUATION.md) để biết protocol, confusion matrix và phạm vi
diễn giải; không gọi88% là độ đúng cảnh báo cuối trên mọi camera.
Độ lệch bbox/video vẫn cần timeline chung phía IOC để đồng bộ đúng từng frame.

Với polygon cấu hình, `person_count` là số bbox thật có tâm cạnh dưới nằm trong
cùng vùng; VD-MIL vẫn xử lý toàn ảnh. Không suy ra model đã định vị đúng hành vi
trong vùng. Lines được nhận/lưu nhưng FIGHT không dùng để đếm IN/OUT.

## Nguồn và giấy phép

- VD-MIL của Roger Figueroa Quintero:
  https://github.com/roggerfq/violence-detection-mil — CC BY-NC 4.0 theo header
  mã nguồn upstream; giữ attribution trong vendor. Chỉ dùng trong phạm vi được cấp phép.
- MoViNet runtime: https://github.com/Atze00/MoViNet-pytorch — giữ LICENSE upstream.
- YOLO/Ultralytics: https://github.com/ultralytics/ultralytics — tuân thủ giấy phép
  upstream. Repo này không cấp lại giấy phép cho mã/trọng số bên thứ ba.
- Trọng số tải riêng có kiểm SHA256; không commit .env, ảnh camera, log hoặc video.
