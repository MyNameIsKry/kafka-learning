# Ráp Kafka với ONNX inference

Tài liệu này mô tả cách chạy và tích hợp Kafka với Go inference consumer trong repo. Model hiện tại là **mock ONNX để kiểm tra wiring**, chưa phải GATv2 đã train và không dùng để báo cáo chất lượng phát hiện gian lận.

## Luồng xử lý

```text
PaySim CSV
   │ producer (Go)
   ▼
transactions.raw ── inference consumer (Go + ONNX Runtime)
                         ├── mọi prediction ──▶ transactions.scored
                         ├── anomaly=true ────▶ transactions.anomalies
                         └── payload/schema lỗi ▶ transactions.dlq
```

Kafka topics được tạo bởi `make kafka-up`. Raw và prediction Mongo sinks, cùng alert consumer, là các tiến trình riêng; chạy chúng nếu cần lưu/quan sát kết quả.

## Chạy local

Các lệnh dưới đây chạy từ thư mục gốc repo. Mỗi lệnh consumer chạy liên tục, nên mở một terminal riêng cho từng tiến trình.

1. Khởi động Kafka, Kafka UI, MongoDB và tạo topics:

   ```sh
   make kafka-up
   ```

2. Xuất mock model và kiểm tra Python exporter + Go/ONNX Runtime:

   ```sh
   make onnx-test
   ```

   File mặc định: `gatv2-pipeline/artifacts/mock/anomaly_score.onnx` (được gitignore). Có thể chỉ xuất model bằng `make mock-onnx-export`.

3. Mở các terminal riêng và chạy consumer cần dùng:

   ```sh
   make consumer
   make prediction-sink
   make alert-consumer
   ```

   `make raw-sink` là tùy chọn nếu cần lưu raw event vào MongoDB.

4. Phát dataset vào Kafka từ một terminal khác:

   ```sh
   make producer CSV=gatv2-pipeline/data/PS_20174392719_1491204439457_log.csv
   ```

   Giới hạn số event khi thử nhanh bằng `MAX_EVENTS=100`. Dùng `make help` để xem các target khác.

## Kafka contracts

### Input: `transactions.raw`

Producer gửi JSON theo `RawTransactionEvent` (`schemaVersion: 2`). Ví dụ rút gọn:

```json
{
  "schemaVersion": 2,
  "transactionId": "paysim-v1-1",
  "sourceRowIndex": 1,
  "step": 1,
  "type": "PAYMENT",
  "amount": 9839.64,
  "nameOrig": "C1231006815",
  "oldbalanceOrg": 170136.0,
  "newbalanceOrig": 160296.36,
  "nameDest": "M1979787155",
  "oldbalanceDest": 0.0,
  "newbalanceDest": 0.0
}
```

Contract đầy đủ và validation nằm ở `server/internal/events/events.go`. Kafka key là `transactionId`. Raw event **không chứa `isFraud` hoặc `isFlaggedFraud`**; label không được đưa vào inference.

### Model input: ONNX

Go adapter tạo tensor `features` kiểu `float32`, shape `[batch, 7]`, theo đúng thứ tự:

| Index | Giá trị |
| --- | --- |
| 0 | `log1p(amount)` |
| 1 | `step / 744` |
| 2 | one-hot `type == CASH_IN` |
| 3 | one-hot `type == CASH_OUT` |
| 4 | one-hot `type == DEBIT` |
| 5 | one-hot `type == PAYMENT` |
| 6 | one-hot `type == TRANSFER` |

ONNX graph phải nhận input tên `features` và trả output tên `scores`, kiểu `float32`, shape `[batch, 1]`. Mỗi score phải hữu hạn; Go đánh dấu `isAnomaly = score >= threshold`. Mapping này hiện được cài trong `server/internal/inference/onnx_engine.go`.

### Output: `transactions.scored` và `transactions.anomalies`

Prediction JSON theo `TransactionPredictionEvent` (`schemaVersion: 1`):

```json
{
  "schemaVersion": 1,
  "transactionId": "paysim-v1-1",
  "step": 1,
  "anomalyScore": 0.12,
  "isAnomaly": false,
  "modelVersion": "mock-v1",
  "processedAt": "2026-10-15T10:00:00Z",
  "reasonCodes": []
}
```

Mọi giao dịch dự đoán thành công được publish vào `transactions.scored`. Chỉ event có `isAnomaly: true` được publish thêm vào `transactions.anomalies`. `transactions.dlq` nhận lỗi payload/schema không hợp lệ sau khi được đóng gói thành `DLQEvent`. Inference lỗi tạm thời được retry; offset chỉ commit sau khi xử lý thành công.

## Cấu hình

Make targets chấp nhận các biến sau:

| Biến | Mặc định | Tác dụng |
| --- | --- | --- |
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers, phân cách bằng dấu phẩy |
| `MODEL_PATH` | `gatv2-pipeline/artifacts/mock/anomaly_score.onnx` | ONNX file Go consumer load |
| `ANOMALY_THRESHOLD` | `0.8` | Ngưỡng để đặt `isAnomaly` |
| `MODEL_VERSION` | `mock-v1` | Version ghi trong prediction |
| `GROUP_ID` | tùy target | Consumer group override |
| `MAX_RETRIES` | `3` | Số lần retry inference/publish lỗi |
| `METRICS_PORT` | `0` | Port Prometheus; `0` tắt metrics |

Ví dụ đổi broker và threshold:

```sh
make consumer BOOTSTRAP_SERVERS=localhost:9092 ANOMALY_THRESHOLD=0.75 MODEL_VERSION=mock-v1
```

Go ONNX binding cần CGO và native ONNX Runtime shared library. `make consumer` dùng Nix shell `.#paysim`, nơi cung cấp Go, ONNX Runtime và `ONNXRUNTIME_SHARED_LIBRARY_PATH`. Nếu chạy binary ngoài Nix, cần cài ONNX Runtime tương thích và đặt biến này trỏ tới thư viện native trước khi chạy.

## Khi thay mock bằng model GATv2

1. Xuất checkpoint đã train thành ONNX và ghi lại input/output names, dtypes, shapes, preprocessing, class/score semantics cùng model version.
2. Đảm bảo `features` đúng thứ tự/chuẩn hóa mà model đã train. Nếu GATv2 cần graph nodes/edges hoặc nhiều tensor, cập nhật Go adapter và event/graph builder theo contract đó; không chỉ thay file ONNX.
3. Đặt artifact vào đường dẫn muốn dùng và chạy `make onnx-test MODEL_PATH=/path/to/model.onnx`.
4. Chạy consumer với cùng đường dẫn và ngưỡng đã được chọn bằng validation:

   ```sh
   make consumer MODEL_PATH=/path/to/model.onnx ANOMALY_THRESHOLD=0.5 MODEL_VERSION=gatv2-v1
   ```

Ngưỡng `0.5` ở ví dụ chỉ minh họa cách truyền cấu hình, không phải ngưỡng khuyến nghị. Mock hiện dùng graph feed-forward với trọng số cố định; score và anomaly của nó không có ý nghĩa đánh giá ML.
