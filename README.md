# Financial Anomaly Detection

Hệ thống phát hiện giao dịch tài chính bất thường trên PaySim sử dụng Kafka,
MongoDB, GATv2 và React Dashboard.

## Trạng thái triển khai

- Phase 6: MongoDB API and live React dashboard
- Kafka KRaft, Kafka UI và MongoDB chạy bằng Docker Compose.
- Các topic PaySim được tạo với một partition cho MVP.
- MongoDB sẵn sàng; collections được tạo tự động khi sink khởi động.
- DTO Python, test chống leakage, PaySim replay producer, mock inference consumer,
  raw sink, prediction sink và alert consumer đã được triển khai.
- Model GATv2 thật, metrics vận hành và observability nâng cao sẽ được triển khai ở các phase tiếp theo.

## Khởi động infrastructure

```bash
make kafka-up
```

Các service:

- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8090`
- MongoDB: `mongodb://localhost:27017`

## Kafka topics

```text
transactions.raw
transactions.scored
transactions.anomalies
transactions.dlq
```

Tạo lại topics nếu cần:

```bash
make topics
```

## PaySim replay producer

Cài dependency Python trước khi chạy producer:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Validate CSV và tạo ground truth offline mà không kết nối Kafka:

```bash
python3 -m producer.main \
  --csv data/PS_20174392719_1491204439457_log.csv \
  --ground-truth-path data/ground_truth.csv \
  --dry-run
```

Replay vào Kafka local, ví dụ 100 events/giây:

```bash
python3 -m producer.main \
  --csv data/PS_20174392719_1491204439457_log.csv \
  --ground-truth-path data/ground_truth.csv \
  --events-per-second 100
```

Hoặc dùng Makefile:

```bash
make producer CSV=data/PS_20174392719_1491204439457_log.csv
```

Producer publish raw event có đủ bốn balance nhưng không publish
`isFraud`/`isFlaggedFraud`. Labels được ghi riêng vào ground-truth CSV.

Producer xác thực toàn bộ CSV trước khi publish, sử dụng external sort theo
`(step, sourceRowIndex)` để không giữ toàn bộ PaySim trong RAM. Khi delivery
failure, producer dừng mặc định để không bỏ qua event giữa chuỗi. Chỉ dùng
`--continue-on-error` khi muốn chủ động bỏ qua event lỗi.

## Inference consumer với mock predictor

Chạy consumer sau khi infrastructure đã khởi động:

```bash
python3 -m consumer.main \
  --batch-size 32 \
  --max-wait-ms 250 \
  --mock-score 0.1 \
  --mock-threshold 0.8
```

Để kiểm tra nhánh anomaly trong wiring test:

```bash
python3 -m consumer.main --mock-score 0.95 --mock-threshold 0.8
```

Consumer chỉ commit input offset sau khi validation/DLQ, inference và các
output publish cần thiết được Kafka acknowledge. Schema lỗi được publish vào
`transactions.dlq`; lỗi inference/output sẽ retry với backoff và dừng mà
không commit sau khi hết retry. Consumer có bounded dedupe cache trong RAM
(`--dedupe-cache-size`); graph-state/dedupe durable sau restart sẽ được xử lý
khi tích hợp inference engine thật.

## Phase 5 — MongoDB sinks và alert consumer

Mỗi nhánh dùng Kafka consumer group riêng và manual commit:

```bash
# transactions.raw -> financial_anomaly.transactions
make raw-sink

# transactions.scored -> financial_anomaly.transaction_predictions
make prediction-sink

# transactions.anomalies -> financial_anomaly.transaction_alerts
make alert-consumer
```

Có thể cấu hình trực tiếp:

```bash
python3 -m sink.main --kind prediction \
  --mongo-uri mongodb://localhost:27017 \
  --mongo-db financial_anomaly \
  --batch-size 32 --max-wait-ms 250 --max-retries 3
```

Raw sink lưu bốn trường balance nhưng không có PaySim labels. Prediction và
alert sink validate contract trước khi upsert. MongoDB upsert là idempotent:
raw theo `transactionId`, prediction/alert theo
`(transactionId, modelVersion)`. Event lỗi schema được ghi vào
`transactions.dlq`; lỗi MongoDB hoặc lỗi publish DLQ làm batch retry và không
commit offset cho tới khi lưu thành công.

Các collection/index được tạo bởi `MongoStore`:

```text
transactions
transaction_predictions
transaction_alerts
```

## Phase 6 — API và dashboard

Go API đọc MongoDB bằng các biến môi trường:

```bash
MONGO_URI=mongodb://localhost:27017 \
MONGO_DATABASE=financial_anomaly \
API_ADDR=:8080 \
make api
```

Endpoints chính:

```text
GET /healthz
GET /readyz
GET /api/v1/summary
GET /api/v1/metrics
GET /api/v1/transactions?limit=25&anomaliesOnly=false
GET /api/v1/alerts?limit=25
```

API chỉ expose transaction/prediction/alert data đã persist; labels PaySim
không được đọc hoặc trả về. Dashboard React tự refresh mỗi 10 giây và có thể
đổi API host bằng `VITE_API_BASE_URL`.

Transaction có `status` là `pending`, `normal` hoặc `anomaly`; chưa có
prediction không đồng nghĩa giao dịch bình thường. Nhãn `API connected` chỉ
xác nhận API đọc được dữ liệu, không xác nhận Kafka/consumer đang khỏe.
Danh sách thường sort/limit trước khi join predictions; nhánh `anomaliesOnly`
lọc prediction mới nhất trước limit. Các index hỗ trợ query được tạo khi sink
khởi động (khởi động lại sink sau khi cập nhật).

DLQ lưu JSON đã decode nếu có; với JSON/UTF-8 lỗi, `originalPayload` chứa
`rawBase64` và `encoding: "base64"` để giữ nguyên bytes cho điều tra/replay.

Chạy MongoDB integration regression bằng database test riêng, tự dọn:

```bash
cd server
MONGO_TEST_URI=mongodb://localhost:27017 go test ./internal/mongo -v
```

```bash
make api
make web
```

Health check backend:

```bash
curl http://localhost:8080/healthz
```

## Contract tests

Install Python dependencies and run the validation suite:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
make contracts-test
```
