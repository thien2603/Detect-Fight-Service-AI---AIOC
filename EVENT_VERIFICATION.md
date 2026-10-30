# Kiểm tra luồng tích hợp

Bản triển khai đã được xác minh qua các bước sau:

1. Model thật xác nhận đủ ngưỡng thời gian và số người detection tối thiểu.
2. Storage API nhận JPEG và trả HTTP200/object_key.
3. Edge gửi đúng topic FIGHT_DETECTION, QoS1 và nhận PUBACK.
4. Mở Web VMS → Phát hiện đánh nhau → Lịch sử → Làm mới: thấy sự kiện.
5. Mở Chi tiết: kiểm tra camera/thời gian và snapshot hiển thị; object_key ảnh
   khớp giá trị upload. Không thay đổi trạng thái xử lý khi chỉ nghiệm thu.

Bằng chứng cụ thể, ảnh camera, định danh, object_key và log nội bộ được giữ ngoài
repository công khai. Đây là kiểm tra tích hợp, chưa phải đánh giá độ đúng model.

Ứng dụng giảm sự kiện trùng bằng latch/cooldown và lưu trạng thái qua restart.
Backend chưa có cơ chế idempotency/ACK nghiệp vụ riêng; MQTT QoS1 vẫn có khả
năng redelivery. Không tự retry khi kết quả publish không rõ.
