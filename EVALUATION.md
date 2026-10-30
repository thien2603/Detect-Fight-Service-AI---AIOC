# Đánh giá trên video có nhãn

Đã chạy cả 400 video split `val`: 200 Fight và 200 NonFight. Không có video bị
loại do lỗi giải mã. Nhãn lấy từ thư mục dataset; không sinh nhãn bằng model.

Nguồn mirror: https://huggingface.co/datasets/A1mal/RWF-2000-Dataset

Revision cố định: `643fe417947d4265fcfaef407332468709728aa8`.
Script `download_eval_dataset.py` chỉ tải split val, kiểm size/SHA256 và lưu
manifest nguồn. Video không được commit hoặc phân phối lại trong repository.

## Phương pháp

- Cùng trọng số YOLO11n và VD-MIL/MoViNet A0 như module chạy thật.
- Toàn ảnh, lấy mẫu 8FPS, 8frame/clip, stride4, threshold0.9 được dùng trước
  phép đánh giá này. Không chọn lại threshold theo kết quả tập val.
- Nhãn clip dự đoán dương nếu max score qua các cửa sổ >=0.9.
- Đánh giá thứ hai áp thêm luật >=1s liên tục và >=2 người YOLO, gate inactive
  khi bắt đầu mỗi clip độc lập, cooldown0 giữa các clip đánh giá.
- Offline hoàn toàn trong container `--network none`, GPU; thời gian336.36s.
- Runtime đã đo: PyTorch2.10.0+cu128, torchvision0.25.0+cu128, OpenCV5.0.0.93.

| Phép đo trên 400 clip | Chấm điểm clip | Luật 1s + ít nhất 2 người |
|---|---:|---:|
| TP |168|70|
| FP |16|3|
| TN |184|197|
| FN |32|130|
| Accuracy |88.00%|66.75%|
| Precision |91.30%|95.89%|
| Recall |84.00%|35.00%|
| F1 |87.50%|51.28%|

Tính Precision=TP/(TP+FP), Recall=TP/(TP+FN), F1=2PR/(P+R).
Luật xác nhận giảm số báo dương nhầm, đồng thời bỏ sót nhiều hơn trên các clip
ngắn. Hai cột là hai phép đánh giá khác nhau; không dùng88% để mô tả độ đúng
của cảnh báo cuối cùng trên camera triển khai.

## Phạm vi diễn giải

Đây là kết quả với chính giao thức inference đang dùng, không tuyên bố tái lập
giao thức/benchmark bài báo upstream. Tập val đến từ mirror; chưa kiểm toán độc
lập việc các video này có tách khỏi dữ liệu huấn luyện upstream của checkpoint
hay không. Kết quả không thay thế đánh giá trên camera triển khai với nhãn thời
gian đầy đủ. Hai đoạn camera đã đánh dấu được giữ riêng để đối chiếu hành vi model.

`evaluate.py` sinh `predictions.csv`, `window_scores.jsonl`, `metrics.json` và
hash trọng số. Có thể chạy lại bằng:

```bash
python download_eval_dataset.py
python evaluate.py
```

Trọng số đã kiểm SHA256 trong `download_weights.py`; dataset có manifest riêng.
