# Hoàn tất tiêu chí tuần 2

Mục tiêu kiểm tra: 06:00 ngày 31/10/2026 (Asia/Saigon).
Chỉ đánh dấu hoàn tất khi có kiểm thử hoặc bằng chứng thực tế.

- [x] C03: subscribe vùng/vạch theo camera/module, cập nhật nóng, chịu payload rỗng.
- [x] C01/cấu hình: client_id đúng mẫu, .env trên server, không lộ bí mật trong Git.
- [x] Docker: build image, GPU/model thật, triển khai một publisher, kiểm tra health.
- [x] C01/C02: nghiệm thu MQTT reconnect/resubscribe và bật/tắt module.
- [x] Độ đúng: tập có nhãn, dự đoán thực, confusion matrix/Precision/Recall/F1.
- [x] Demo MP4: model/video, bbox và lịch sử/ảnh trên VMS; 135,44 giây, H.264 1080p.
- [x] Báo cáo PDF: cập nhật kết quả được kiểm chứng; giữ kiểu Latin Modern Roman.
- [x] Bộ nộp: PDF/MP4, hướng dẫn, bằng chứng và folder Drive riêng tên không dấu.

Không đưa camera/RTSP credentials, object_key nội bộ hoặc ảnh camera lên repo public.

Bằng chứng: 21 unit tests; integration broker loopback tất cả bước đạt;
bật/tắt và sửa vùng trên VMS thật không restart container; 400/400 video có
nhãn đã được chấm điểm (không lỗi giải mã). Kết quả chi tiết: EVALUATION.md.

## Đối chiếu yêu cầu

| Tiêu chí | Bằng chứng / cách kiểm tra |
|---|---|
| C01 - WSS, client ID, reconnect | TLS xác thực; ID riêng `tts_thien_01`; integration restart broker vẫn cùng worker và subscribe/request lại. |
| C02 - đúng camera/module | Lọc organization/zone/camera; nghiệm thu bật/tắt trực tiếp trên VMS: dừng/tiếp tục inference và bbox. |
| C03 - vùng/vạch, hot update, rỗng | Subscribe hai topic polygon và lines; kiểm UUID; sửa vùng thật trên VMS; unit/integration có tombstone, rỗng, JSON sai. |
| C04 - video phù hợp | Pipeline đọc RTSP có hành vi đánh nhau thật; demo giám sát trực tiếp và snapshot của cùng nguồn. |
| C05 - Storage/object_key | JPEG RAM, upload HTTP thành công; snapshot_url là object_key; ảnh gốc mở được trong VMS, không base64 MQTT. |
| C06 - sự kiện/schema/thời gian | Validate JSON Schema; topic FIGHT_DETECTION; event_time +07:00; đối chiếu sự kiện và ảnh trên Web VMS. |
| C07 - bbox | QoS0, retain=false, bbox 0..1, chỉ kết quả YOLO thật; hiển thị VMS khoảng15Hz. |
| C08 - một vụ/cooldown | Gate xác nhận, latch bền qua restart; reset khi điểm thấp đủ2s; cooldown60s và các ngưỡng cấu hình được. |
| C09 - detection thật | YOLO boxes của frame thật còn mới, VD-MIL từ clip thật; không tracking/interpolation để phát sự kiện. |
| Riêng FIGHT | submodule_code=FIGHT_DETECTION, payload.type=FIGHT, person_count; ngưỡng thời gian cấu hình. |
| Phi chức năng | .env riêng; payload validation; chống lặp; log publish/ảnh/PUBACK; Docker GPU; giới hạn dữ liệu. |
| Đánh giá định lượng | 400 clip có nhãn; hai confusion matrix và Precision/Recall/F1; tách điểm clip khỏi luật sự kiện. |
| Hồ sơ | PDF3trang có link GitHub, bảng số liệu và ảnh VMS; demo MP4 có bbox, lịch sử và ảnh. |

PDF/demo chứa bằng chứng camera được giữ trong bộ nộp riêng, không commit vào
GitHub public. Bộ nộp giữ bản đã nộp trước đó, bổ sung kết quả nghiệm thu mới.
