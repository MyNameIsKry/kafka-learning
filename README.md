# Financial Anomaly Detection

Hệ thống phát hiện giao dịch tài chính bất thường trên PaySim sử dụng Kafka,
MongoDB, GATv2 và React Dashboard.

## Demo đơn giản: một server + một client + Locust

Go server tại `server/cmd/app` chạy API và ba worker nền trong **một process**:

```text
Locust → POST /api/v1/transactions → KurrentDB (save trước)
                                      ├─ projection worker → MongoDB
                                      └─ relay worker → Kafka transactions.raw
                                                          ↓
                                                      ONNX mock model worker
                                                          ↓
                                                      KurrentDB
                                                          ↓
                                      projection worker → MongoDB predictions/alerts
React dashboard ← GET API ← MongoDB
```

Relay cũng chuyển scored/anomaly/DLQ sang Kafka để giữ các topic kết quả.
Projection đọc **trực tiếp KurrentDB**, không phụ thuộc Kafka/model. Hai worker
có persistent subscription/checkpoint riêng. Khi Kafka lỗi, transaction vẫn có
thể hiện trên UI với trạng thái `pending`; khi model hoàn tất, thành `normal`
hoặc `anomaly`. API trả **202 chỉ sau KurrentDB xác nhận**, không đợi scoring.

Tại thư mục gốc project:

```bash
make demo-infra
# Terminal 1: API + tất cả worker, không cần các terminal relay/sink/consumer
make server ANOMALY_THRESHOLD=0.3 MODEL_VERSION=mock-onnx-demo
# Terminal 2 (npm ci trong web khi chạy lần đầu):
make web
# Terminal 3 (cài một lần):
python3 -m venv .venv  # chỉ khi chưa có .venv
.venv/bin/pip install -r requirements-locust.txt
make locust
```

- Dashboard: http://localhost:5173 (tự refresh).
- Locust: http://localhost:8089 — thử 5 users, spawn rate 1 trước khi tăng tải.
- API: http://localhost:8080.
- Server dùng **ONNX mock model** với input tensor `[batch, 7]`, không phải độ
  chính xác GATv2; Locust tạo dữ liệu synthetic để đo HTTP/delivery, không phục
  vụ đánh giá ML.
- Dừng các process runtime cũ trước khi chạy server mới, nhất là inference và
  relay: chạy hai cấu hình model khác nhau cùng version có thể gây conflict.
- `make api` và các command riêng vẫn giữ cho debug/legacy; **demo dùng `make server`**.
- Không có auth/TLS/rate limit cho ingestion MVP; chỉ chạy local/trusted network.

Gửi một transaction bằng curl (không gửi labels):

```bash
ID="http-demo-$(date +%s)"
curl -i http://localhost:8080/api/v1/transactions \
  -H 'Content-Type: application/json' \
  -d "{\"schemaVersion\":2,\"transactionId\":\"$ID\",\"sourceRowIndex\":1,\"step\":1,\"type\":\"TRANSFER\",\"amount\":20,\"nameOrig\":\"C1\",\"nameDest\":\"C2\",\"oldbalanceOrg\":200,\"newbalanceOrig\":180,\"oldbalanceDest\":0,\"newbalanceDest\":20}"
curl http://localhost:8080/api/v1/summary
```

`transactionId` là idempotency key: retry cùng ID và cùng body → 202, không thêm
event; cùng ID nhưng khác nội dung → 409; invalid/labels → 400; event store lỗi
→ 503. Nếu timeout không biết append đã thành công chưa, retry giữ nguyên ID và
body. Giới hạn body 64 KiB. HTTP nhận giao dịch online theo thứ tự append, không
sort toàn bộ request theo `step`; preprocessing/graph ordering của model thật
vẫn cần chốt. `GET /readyz` hiện kiểm tra MongoDB, không báo worker lag/parked.

Dashboard dùng phân trang thật từ API, không chỉ cắt danh sách ở frontend:

```text
GET /api/v1/transactions?limit=12&offset=12&status=anomaly
GET /api/v1/alerts?limit=8&offset=8
```

Response có `offset` và `hasMore`; UI hiển thị nút Previous/Next. Tổng anomaly
trên summary vẫn là tổng toàn bộ (ví dụ 39), còn danh sách alert hiển thị theo
từng trang.

Kiểm thử server một process, độc lập Kafka và recovery:

```bash
make demo-infra
python3 scripts/app-smoke.py
```

Test dùng port 18080 và namespace/topics/database riêng, giữ dữ liệu để kiểm tra.
Delivery at-least-once; upsert MongoDB và ID ổn định chống trùng khi retry.
Scored/anomaly hiện append riêng: nếu crash giữa hai append, retry hoàn tất
output còn thiếu trước commit Kafka offset; không hứa atomic multi-stream.
Subscription có thể park sau nhiều timeout; kiểm tra/replay parked trong
KurrentDB UI. Không xóa event source-of-truth khi rebuild projection.

Các mục command riêng bên dưới dành cho replay/debug, không cần để chạy demo này.

## Trạng thái triển khai

- Phase 9: Offline scored-prediction evaluation
- KurrentDB, Kafka KRaft, Kafka UI và MongoDB chạy bằng Docker Compose.
- Các topic PaySim được tạo với một partition cho MVP.
- MongoDB sẵn sàng; collections được tạo tự động khi sink khởi động.
- Go runtime contracts, PaySim replay producer, ONNX inference consumer, raw
  sink, prediction sink và alert consumer đã được triển khai.
- KurrentDB là event store/source of truth. Go relay chuyển event sang Kafka;
  MongoDB chỉ là read-model projection.
- Metrics vận hành và benchmark runner đã có; GATv2 thật và graph-state recovery
  chưa tích hợp. ONNX mock đã được dùng trong unified server.

## Khởi động infrastructure

```bash
make kafka-up
```

Các service:

- KurrentDB/UI: `http://localhost:2113` (insecure chỉ cho local demo)
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

Cài dependencies cho Go runtime:

```bash
cd server
go mod download
cd ..
```

Python environment dùng cho offline pipeline/evaluation và Locust, không cho Go server.

Go runtime yêu cầu Go theo `server/go.mod` (hiện 1.26); Go có thể tự tải toolchain.

Validate CSV và tạo ground truth offline mà không kết nối Kafka:

```bash
ROOT="$PWD"
(cd server && go run ./cmd/producer \
  --csv "$ROOT/data/PS_20174392719_1491204439457_log.csv" \
  --ground-truth-path "$ROOT/data/ground_truth.csv" \
  --dry-run)
```

Chạy relay trong terminal riêng, rồi ghi CSV vào KurrentDB:

```bash
make event-relay
# terminal khác:
make producer \
  CSV=data/PS_20174392719_1491204439457_log.csv \
  GROUND_TRUTH=data/ground_truth.csv
```

Hoặc dùng Makefile:

```bash
make producer CSV=data/PS_20174392719_1491204439457_log.csv
```

Producer publish raw event có đủ bốn balance nhưng không publish
`isFraud`/`isFlaggedFraud`. Labels được ghi riêng vào ground-truth CSV.

Producer xác thực toàn bộ CSV, external-sort theo `(step, sourceRowIndex)` với
chunk giới hạn bộ nhớ và publish theo thứ tự đó. Khi delivery
failure, producer dừng mặc định để không bỏ qua event giữa chuỗi.
Producer không gửi trực tiếp vào Kafka: append KurrentDB thành công trước.

## Inference consumer với ONNX mock predictor

Chạy consumer sau khi infrastructure đã khởi động:

```bash
(cd server && go run ./cmd/inference \
  --model-path "$(pwd)/gatv2-pipeline/artifacts/mock/anomaly_score.onnx" \
  --threshold 0.8 \
  --model-version mock-onnx-v1)
```

Để kiểm tra nhánh anomaly trong wiring test:

```bash
(cd server && go run ./cmd/inference \
  --model-path "$(pwd)/gatv2-pipeline/artifacts/mock/anomaly_score.onnx" \
  --threshold 0.3 \
  --model-version mock-onnx-v1)
```

Consumer chỉ commit input offset sau khi validation/DLQ, inference và các
output append cần thiết được KurrentDB acknowledge. Schema lỗi được ghi vào KurrentDB trước khi relay sang
`transactions.dlq`; lỗi inference/output sẽ retry với backoff và dừng mà
không commit sau khi hết retry. Consumer có bounded dedupe cache trong RAM
(`--dedupe-cache-size`); graph-state/dedupe durable sau restart sẽ được xử lý
khi tích hợp GATv2 thật.

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
(cd server && go run ./cmd/sink --kind prediction \
  --mongo-uri mongodb://localhost:27017 \
  --mongo-db financial_anomaly \
  --max-retries 3)
```

Raw sink lưu bốn trường balance nhưng không có PaySim labels. Prediction và
alert sink validate contract trước khi upsert. MongoDB upsert là idempotent:
raw theo `transactionId`, prediction/alert theo
`(transactionId, modelVersion)`. Event lỗi schema được ghi vào
`transactions.dlq`; lỗi MongoDB hoặc lỗi publish DLQ làm batch retry và không
commit offset cho tới khi lưu thành công.

KurrentDB là source of truth. Mỗi event có stream `financial-anomaly-event-<UUID>` và
envelope gồm `schemaVersion`, `eventId`, `eventType`, `occurredAt`, `topic`, `key`,
`payload`. Event types: `RawTransactionRecorded`, `TransactionScored`,
`TransactionAnomalyDetected`, `InvalidEventRecorded`. ID ổn định theo topic và
transaction/model version (DLQ theo source topic/partition/offset). Append dùng
`NoStream`; retry kiểm tra event cũ, từ chối payload xung đột, không ghi đè.
Timestamp retry không thay đổi event đã lưu. Payload Kafka vẫn giữ DTO cũ để
API/dashboard/offline tooling tương thích; `eventId` nằm trong Kafka header.

Relay dùng persistent subscription `$all` lọc prefix, một subscriber và buffer
1; ACK chỉ sau Kafka acknowledge. Delivery là **at-least-once**, không phải
exactly-once. Mất ACK có thể tạo duplicate; sinks upsert idempotent.

Rebuild Kafka: dừng relay cũ, chạy `make event-relay RELAY_GROUP=rebuild-<id>`
để replay từ đầu KurrentDB. Rebuild MongoDB: chạy sinks với consumer group mới
và database mới qua `--mongo-db`; không cần xóa database đang dùng. Nếu Kafka
đã mất history, replay từ KurrentDB trước. Không chạy hai relay cùng lúc khi
cần giữ thứ tự. KurrentDB single node cần backup; volume không thay backup.

Luồng demo:
`CSV → Go producer → KurrentDB → Go relay → Kafka raw → Go inference →
KurrentDB scored/anomaly → Go relay → Kafka → Go sinks → MongoDB → Go API → React`.
DLQ từ inference/sinks cũng được append vào KurrentDB trước khi relay.

Các runtime Python cũ và Python benchmark direct-to-Kafka là tooling legacy,
không đại diện cho luồng KurrentDB authoritative và không dùng để nhập event
production-like. Labels/ground truth vẫn offline, không nằm trong event store.

Kiểm thử KurrentDB và end-to-end (không xóa dữ liệu hiện có):

```bash
make kafka-up
(cd server && KURRENTDB_TEST_CONNECTION='kurrentdb://localhost:2113?tls=false' go test ./internal/eventstore -v)
bash scripts/kurrent-smoke.sh
```

Smoke test dùng namespace/topics/databases riêng, giữ lại resources để kiểm tra.
Persistent subscription có thể park event sau nhiều lần timeout: kiểm tra parked
messages trong KurrentDB UI và replay parked; không coi consumer lag thấp là
bằng chứng tất cả event đã được chuyển. Kafka/MongoDB lỗi không được xóa event
KurrentDB. Single-node/insecure chỉ dành cho demo, không phải HA production.

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

## Phase 7 — Metrics và benchmark

Metrics HTTP là opt-in, mặc định tắt (`--metrics-port 0`). Mỗi process dùng
port riêng, bind loopback mặc định; không export account IDs, balance hoặc
PaySim labels:

```bash
(cd server && go run ./cmd/inference --metrics-port 9101)
(cd server && go run ./cmd/sink --kind raw --metrics-port 9102)
(cd server && go run ./cmd/sink --kind prediction --metrics-port 9103)
(cd server && go run ./cmd/sink --kind alert --metrics-port 9104)
# Thêm --metrics-port 9100 vào cmd/producer nếu cần scrape khi replay.
curl http://127.0.0.1:9101/metrics
```

Metrics chính:

- `pipeline_published_records_total`: Kafka publish đã acknowledge; gồm publish lặp do retry.
- `pipeline_eventstore_appended_records_total`: producer/inference append đã được KurrentDB xác nhận, gồm idempotent retries; không phải Kafka publish.
- `pipeline_stored_records_total`: DB upsert đã acknowledge; không phải unique documents.
- `pipeline_committed_records_total`: input records trong batches commit thành công.
- `pipeline_failures_total`, `pipeline_failed_record_attempts_total`, `pipeline_retries_total`:
  lỗi theo attempt, không phải số giao dịch lỗi duy nhất.
- `pipeline_delivery_seconds`: thời gian producer send → ack, không gồm rate limiting.
- `pipeline_inference_seconds`: thời gian gọi engine mỗi attempt, không gồm output publishing.
- `pipeline_batch_seconds`: xử lý batch và commit, không gồm retry backoff/poll wait.
- `pipeline_input_to_completion_seconds`: Kafka input timestamp → batch commit thành công.
  Với prediction/alert sink, điểm bắt đầu là publication của scored/anomaly event,
  **không** phải raw ingress. Không dùng `step` để tính wall-clock latency.
- `pipeline_consumer_lag`: log end offset trừ committed offset; sample mỗi 10 giây,
  dùng earliest retained offset nếu group chưa commit. `pipeline_lag_sample_timestamp_seconds`
  cho biết sample còn mới hay không; sampling lỗi không commit hoặc retry input batch.
- `pipeline_last_commit_timestamp_seconds`: thời điểm commit thành công gần nhất.

Ví dụ PromQL khi đã cấu hình Prometheus scrape các ports trên:

```promql
rate(pipeline_published_records_total[1m])
rate(pipeline_committed_records_total[1m])
histogram_quantile(0.95, sum by (le, component) (rate(pipeline_inference_seconds_bucket[5m])))
pipeline_consumer_lag
```

API `/api/v1/metrics` hiện vẫn là summary MongoDB, không phải Prometheus endpoint.
Phase này chưa thêm Prometheus/Grafana service hoặc biểu đồ lag vào dashboard.

### Benchmark runner

Runner không tạo/xóa topics, không khởi động model và không sửa dữ liệu production.
Tạo 4 topics **riêng** (1 partition, replication 1) rồi chạy consumer trên topics đó:

```bash
for topic in bench.raw bench.scored bench.anomalies bench.dlq; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done

(cd server && go run ./cmd/inference --topic bench.raw --group-id bench-inference \
  --scored-topic bench.scored --anomalies-topic bench.anomalies \
  --dlq-topic bench.dlq --metrics-port 9101)
```

Ở terminal khác:

```bash
python3 -m benchmark.main --raw-topic bench.raw --scored-topic bench.scored \
  --rates 100 500 1000 --max-events 1000
# Hoặc: make benchmark RAW_TOPIC=bench.raw SCORED_TOPIC=bench.scored
```

Không truyền `--csv` thì dùng synthetic wiring data; truyền `--csv data/PS_*.csv`
để chạy dữ liệu PaySim thật. Mỗi scenario có dataset version riêng để không bị
dedupe giữa các lần chạy. Kết quả và ground truth offline nằm trong
`benchmark-results/<run-id>/` (gitignored).

JSON báo cáo target/achieved producer rate, observed scored rate, số scored
nhận được/bị thiếu, p50/p95/p99 delivery và raw-send → scored publication.
Latencies benchmark dùng Kafka timestamp và local send wall clock: cần đồng bộ
clock, không gồm MongoDB persistence, không phải ML metrics. Runner trả exit code
khác 0 nếu thiếu scored sau `--drain-timeout` (mặc định 60 giây).

Để đo sinks, chạy chúng trên topics benchmark với `--topic`, `--group-id`,
`--mongo-db` riêng; scrape metrics hoặc xác nhận document count. Giữ broker,
batch size, model version, hardware và CSV giống nhau giữa các kịch bản.
Không coi target 100/500/1.000 events/s là throughput đã đạt — producer MVP
vẫn send/ack tuần tự.

## Phase 8 — Offline/online feature parity

Pipeline offline và Kafka adapter dùng cùng whitelist:

```text
transactionId, sourceRowIndex, step, type, amount, nameOrig, nameDest
```

Test parity fail-closed nếu thứ tự hoặc tên cột lệch:

```bash
# sau khi pip install -e gatv2-pipeline
make pipeline-test
```

Pipeline cũng kiểm thử quarantine hàng lỗi, giữ nguyên balance consistency
không hợp lệ theo giả định nghiệp vụ, xuất Parquet không có balance/labels và
ghi manifest gồm `rowsInput`, `rowsAccepted`, `rowsRejected` cùng
`rejectionReasons`. Chạy pipeline offline:

```bash
make paysim-pipeline
```

Phase này chưa tự chốt graph builder, edge/node target, graph window,
encoder/scaler, threshold hoặc graph-state recovery.

## Phase 9 — Offline evaluation

Evaluation đọc ground truth offline và scored predictions từ hai file riêng;
labels không được đưa vào Kafka/inference. Prediction file là JSONL với mỗi
dòng là `TransactionPredictionEvent`:

```bash
python3 -m evaluation.main \
  --ground-truth data/ground_truth.csv \
  --predictions artifacts/scored.jsonl \
  --threshold 0.8 \
  --output artifacts/evaluation.json
```

Hoặc:

```bash
make evaluate \
  GROUND_TRUTH=data/ground_truth.csv \
  PREDICTIONS=artifacts/scored.jsonl \
  THRESHOLD=0.8
```

Ground truth CSV phải có `transactionId,step,isFraud` (có thể thêm
`isFlaggedFraud`). Tool yêu cầu tập ID và `step` của ground truth/prediction
khớp hoàn toàn, từ chối ID trùng, step không hợp lệ, label ngoài `{0,1}`,
score không hữu hạn và file rỗng. Report gồm
confusion matrix, Precision, Recall, F1, PR-AUC dạng average precision và số
prediction có `isAnomaly` khác với threshold explicit. Threshold phải được
chọn bên ngoài test report; tool không tuning threshold hoặc dùng labels cho
inference.

Đây là ML evaluation offline, khác với Prometheus metrics throughput/latency
của Phase 7 và chưa phải kết quả GATv2 thật.

## Contract tests

Go runtime tests:

```bash
cd server
go test ./...
go vet ./...
cd ..
```

Python dependencies are only needed for offline pipeline/evaluation and the
legacy regression suite:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
make python-legacy-test
```
