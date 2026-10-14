# TECHNICAL DESIGN DOCUMENT
## Hệ thống phát hiện giao dịch tài chính bất thường sử dụng Kafka và GATv2

**Phiên bản:** 1.0 — Đề xuất triển khai  
**Kiến trúc:** Event-Driven Architecture  
**Phạm vi:** Đồ án môn Big Data

---

## 1. Tổng quan hệ thống

### 1.1. Mục tiêu

Xây dựng hệ thống phát hiện giao dịch tài chính bất thường dựa trên luồng dữ liệu giao dịch, kết hợp:

- **Apache Kafka:** Tiếp nhận, lưu trữ và phân phối sự kiện giao dịch.
- **Python + Pandas:** Tiền xử lý dữ liệu đầu vào.
- **GATv2:** Phân tích mối quan hệ giữa các tài khoản và dự đoán mức độ bất thường.
- **MongoDB:** Lưu trữ giao dịch và kết quả phân tích.
- **Backend API + Dashboard:** Cung cấp dữ liệu, hiển thị cảnh báo và thống kê.

Hệ thống sử dụng dữ liệu lịch sử để giả lập luồng giao dịch theo thời gian thực.

Đây là hệ thống phân tích và cảnh báo, không thực hiện chuyển tiền thực tế và không tự động khóa tài khoản.

### 1.2. Vai trò của Kafka

Kafka là trung tâm phân phối sự kiện giữa các thành phần của hệ thống.

Các chức năng chính:

1. Tiếp nhận sự kiện từ Transaction Producer.
2. Lưu trữ sự kiện trong các Kafka Topic.
3. Phân phối giao dịch tới GATv2 Inference Consumer.
4. Tiếp nhận kết quả phân tích từ Inference Consumer.
5. Phân phối kết quả tới MongoDB Sink và Alert Consumer.
6. Cho phép consumer đọc lại sự kiện khi cần thiết.

Kafka không thực hiện tiền xử lý bằng Pandas, xây dựng đồ thị hay chạy GATv2.

---

## 2. System Architecture

### 2.1. Tổng quan kiến trúc

```text
                OFFLINE TRAINING
               ┌──────────────────┐
               │ Historical       │
               │ Dataset          │
               └────────┬─────────┘
                        │
                        ▼
               ┌──────────────────┐
               │ Preprocessing    │
               │ Graph Builder    │
               └────────┬─────────┘
                        │
                        ▼
               ┌──────────────────┐
               │ GATv2 Training   │
               └────────┬─────────┘
                        │
                        ▼
               ┌──────────────────┐
               │ Trained Model    │
               │ + Artifacts      │
               └────────┬─────────┘
                        │
                        │ Load model
                        ▼

                STREAMING PIPELINE

 ┌──────────────────┐
 │ Transaction      │
 │ Simulator        │
 └────────┬─────────┘
          │ Publish event
          ▼
 ┌────────────────────────────┐
 │         KAFKA              │
 │ Topic: transactions.raw    │
 └─────────────┬──────────────┘
               │ Consume
               ▼
 ┌────────────────────────────┐
 │ GATv2 Inference Consumer   │
 │                            │
 │ - Pandas preprocessing     │
 │ - Graph state update       │
 │ - Subgraph construction    │
 │ - GATv2 inference          │
 │ - Anomaly scoring          │
 └─────────────┬──────────────┘
               │ Publish results
               ▼
 ┌────────────────────────────┐
 │          KAFKA             │
 │                            │
 │ transactions.scored        │
 │ transactions.anomalies     │
 └─────────────┬──────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
 ┌────────────┐   ┌────────────┐
 │ MongoDB    │   │ Alert      │
 │ Sink       │   │ Consumer   │
 └──────┬─────┘   └────────────┘
        │
        ▼
 ┌────────────┐
 │ MongoDB    │
 └──────┬─────┘
        │
        ▼
 ┌────────────┐
 │ Backend API│
 └──────┬─────┘
        │
        ▼
 ┌────────────┐
 │ Dashboard  │
 └────────────┘
```

### 2.2. Phân chia trách nhiệm

| Thành phần | Người phụ trách | Chức năng |
|---|---|---|
| Kafka Infrastructure | Thành viên Kafka | Cấu hình và vận hành Kafka |
| Transaction Producer | Thành viên Kafka | Phát giao dịch giả lập |
| Kafka Topics | Thành viên Kafka | Thiết kế và quản lý topics |
| Kafka Consumer Integration | Phối hợp | Kết nối Kafka với inference pipeline |
| Dataset Preprocessing | Thành viên AI | Làm sạch và chuẩn hóa dữ liệu |
| Graph Construction | Thành viên AI | Xây dựng đồ thị giao dịch |
| GATv2 Training | Thành viên AI | Huấn luyện và đánh giá mô hình |
| GATv2 Inference | Thành viên AI | Dự đoán bất thường |
| Result Publisher | Phối hợp | Publish kết quả phân tích |
| MongoDB Sink | Thành viên Kafka / Backend | Lưu kết quả |
| Dashboard | Thành viên Frontend | Trực quan hóa dữ liệu |

**Nguyên tắc:** Thành viên Kafka không cần triển khai thuật toán GATv2. Thành viên AI không cần quản lý hạ tầng Kafka. Hai bên thống nhất event schema và interface để tích hợp.

---

## 3. Kafka Topic Design

Hệ thống dự kiến sử dụng bốn topics.

| Topic | Producer | Consumer | Nội dung |
|---|---|---|---|
| `transactions.raw` | Transaction Simulator | GATv2 Consumer | Giao dịch đầu vào |
| `transactions.scored` | GATv2 Consumer | MongoDB Sink | Kết quả phân tích |
| `transactions.anomalies` | GATv2 Consumer | Alert Consumer | Giao dịch bất thường |
| `transactions.dlq` | Các service xử lý lỗi | Monitoring / Debug | Sự kiện không xử lý được |

### 3.1. Topic: transactions.raw

Đây là topic đầu vào chính.

Mỗi message đại diện cho một giao dịch.

**Message key:** `senderAccountId`

Ví dụ:

```json
{
  "schemaVersion": 1,
  "transactionId": "TXN0001",
  "senderAccountId": "ACC001",
  "receiverAccountId": "ACC002",
  "amount": 50000000,
  "currency": "VND",
  "transactionType": "TRANSFER",
  "timestamp": "2026-10-13T10:00:00Z"
}
```

Các trường bắt buộc:

- `transactionId`: Mã định danh giao dịch duy nhất.
- `senderAccountId`: Tài khoản gửi.
- `receiverAccountId`: Tài khoản nhận.
- `amount`: Số tiền giao dịch.
- `timestamp`: Thời điểm giao dịch theo UTC.
- `schemaVersion`: Phiên bản định dạng sự kiện.

Các trường khác được bổ sung tùy theo dataset thực tế và schema đã thống nhất với nhóm AI.

Không tự tạo các thuộc tính không có trong dataset rồi xem chúng như dữ liệu thực.

### 3.2. Topic: transactions.scored

Chứa kết quả phân tích cho tất cả giao dịch được đánh giá thành công.

```json
{
  "schemaVersion": 1,
  "transactionId": "TXN0001",
  "anomalyScore": 0.95,
  "isAnomaly": true,
  "modelVersion": "gatv2-v1",
  "processedAt": "2026-10-13T10:00:01Z",
  "reasonCodes": []
}
```

Lưu ý:

- `anomalyScore` là điểm do pipeline AI trả về.
- Quy ước thang điểm và ngưỡng bất thường do nhóm AI xác định.
- `reasonCodes` chỉ được bổ sung nội dung khi pipeline có khả năng giải thích tương ứng.
- Không tự suy diễn nguyên nhân bất thường từ attention weights.

### 3.3. Topic: transactions.anomalies

Chỉ chứa các giao dịch có `isAnomaly = true`.

Sử dụng cấu trúc tương tự `transactions.scored`, có thể bổ sung các thông tin phục vụ cảnh báo sau khi thống nhất schema.

Topic này phục vụ:

- Hiển thị cảnh báo.
- Thống kê giao dịch nghi vấn.
- Phân tích lịch sử các trường hợp bất thường.

### 3.4. Topic: transactions.dlq

Dead Letter Queue dùng để lưu sự kiện không thể xử lý bình thường theo chính sách xử lý lỗi.

Ví dụ:

```json
{
  "sourceTopic": "transactions.raw",
  "sourcePartition": 0,
  "sourceOffset": 125,
  "errorCode": "INVALID_SCHEMA",
  "errorMessage": "Missing transactionId",
  "failedAt": "2026-10-13T10:00:02Z",
  "originalPayload": {}
}
```

Không nên đưa ngay mọi lỗi tạm thời vào DLQ. Những lỗi như broker tạm mất kết nối cần có retry policy riêng.

---

## 4. Quy trình hoạt động

### 4.1. Transaction Producer

Producer đọc dữ liệu từ file CSV hoặc dữ liệu đã được nhóm AI chuẩn hóa.

Quy trình:

1. Đọc dataset.
2. Chuyển từng dòng thành transaction event.
3. Kiểm tra schema cơ bản.
4. Serialize event thành JSON.
5. Gửi tới Kafka topic `transactions.raw`.
6. Xác nhận publish thành công hoặc ghi nhận lỗi.

Producer hỗ trợ các chế độ:

- Replay dữ liệu theo thứ tự thời gian.
- Phát giao dịch với tốc độ cố định.
- Tùy chỉnh số events/giây.
- Tạm dừng và tiếp tục phát dữ liệu.

Các tốc độ thử nghiệm đề xuất:

- 100 events/second.
- 500 events/second.
- 1.000 events/second.

Đây là các mức cấu hình để benchmark, không phải hiệu năng cam kết.

### 4.2. Kafka Broker

Kafka nhận các sự kiện từ Producer.

Quy trình:

1. Nhận message.
2. Xác định partition từ message key.
3. Append message vào partition log.
4. Gán offset.
5. Cung cấp message cho consumer.

Kafka lưu dữ liệu theo cấu hình retention, không xóa ngay sau khi một consumer đã đọc.

### 4.3. GATv2 Inference Consumer

Consumer được viết bằng Python.

Quy trình:

1. Subscribe topic `transactions.raw`.
2. Poll các transaction events.
3. Gom message thành micro-batch.
4. Chuyển dữ liệu cho inference pipeline.
5. Nhận kết quả anomaly detection.
6. Publish kết quả vào Kafka.
7. Commit offset khi việc xử lý và publish đã thành công.

**Inference pipeline dự kiến:**

```text
Input: List[Transaction]
          │
          ▼
Pandas Preprocessing
          │
          ▼
Graph State Update
          │
          ▼
Subgraph Construction
          │
          ▼
GATv2 Inference
          │
          ▼
Output: List[Prediction]
```

Phần graph state, subgraph construction và model inference do nhóm AI xây dựng.

**Lưu ý về consistency:** Commit offset sau khi publish chỉ là nền tảng của cơ chế at-least-once. Nếu tiến trình gặp sự cố, giao dịch vẫn có thể được xử lý lại. Trạng thái đồ thị phải hỗ trợ replay an toàn, và kết quả đầu ra phải được xử lý idempotent.

### 4.4. Result Publisher

Sau khi có kết quả inference:

- Publish kết quả của mọi giao dịch hợp lệ vào `transactions.scored`.
- Nếu giao dịch được phân loại bất thường, publish thêm event vào `transactions.anomalies`.
- Chỉ xác nhận hoàn tất khi các bản ghi đầu ra cần thiết đã được Kafka ghi nhận.

Ở bản triển khai đơn giản, có thể sử dụng producer callback và flush/acknowledgment.

Nếu cần xử lý nguyên tử giữa input offset và các output topics, có thể nghiên cứu Kafka transactions. Cơ chế này không tự bảo đảm tính nguyên tử với trạng thái nằm trong MongoDB hoặc RAM.

### 4.5. MongoDB Sink

MongoDB Sink subscribe `transactions.scored`.

Quy trình:

1. Consume result event.
2. Validate schema.
3. Upsert kết quả vào MongoDB.
4. Commit offset sau khi MongoDB xác nhận ghi thành công.

Collection dự kiến:

`transaction_predictions`

Các trường chính:

- `transactionId`
- `anomalyScore`
- `isAnomaly`
- `modelVersion`
- `processedAt`
- `reasonCodes`

Tạo unique compound index cho `(transactionId, modelVersion)` nếu hệ thống cần giữ kết quả của nhiều phiên bản mô hình.

Nếu chỉ lưu phiên bản đánh giá mới nhất, phải thống nhất quy tắc cập nhật để tránh replay của mô hình cũ ghi đè kết quả mới.

### 4.6. Alert Consumer

Alert Consumer subscribe `transactions.anomalies`.

Nhiệm vụ:

- Tiếp nhận các bất thường được phát hiện.
- Tạo cảnh báo phục vụ dashboard.
- Tránh sinh cảnh báo trùng khi sự kiện được đọc lại.

Bản MVP có thể ghi cảnh báo vào MongoDB. Không bắt buộc phải tích hợp email hoặc notification service.

---

## 5. Kafka Configuration

### 5.1. Cấu hình đề xuất cho môi trường phát triển

| Thuộc tính | Giá trị ban đầu |
|---|---|
| Kafka Brokers | 1 |
| Partitions/topic | 1 |
| Replication Factor | 1 |
| Message Format | JSON |
| Consumer Group | `gatv2-inference-group` |
| Auto Commit | Disabled |
| Retention | 24 giờ |
| Processing Mode | At-least-once |
| Consumer Language | Python |

Cấu hình trên dành cho MVP chạy local.

Một partition giúp đơn giản hóa thứ tự đọc sự kiện và tránh phải đồng bộ graph state giữa nhiều consumer. Sau khi hệ thống ổn định, nhóm có thể tăng partition để thực nghiệm khả năng mở rộng.

Không nên xem một broker và replication factor bằng 1 là cấu hình production có tính sẵn sàng cao.

### 5.2. Consumer Group

Các consumer phục vụ những mục đích khác nhau sử dụng consumer group riêng.

Ví dụ:

```text
transactions.raw
       │
       ├── gatv2-inference-group
       │
       └── debug-monitor-group
```

Điều này cho phép inference consumer và debug consumer đọc cùng luồng sự kiện độc lập.

Nhiều consumer trong cùng group sẽ phân chia các partition được gán; chúng không mặc định cùng nhận mọi event.

### 5.3. Offset Management

Tắt automatic commit.

Chỉ commit khi:

- Event hợp lệ đã xử lý thành công.
- Output events đã được publish và xác nhận.
- Graph state đã đạt trạng thái có thể khôi phục an toàn.

Đối với event không hợp lệ, cần có chính sách riêng: phát DLQ thành công trước khi bỏ qua offset tương ứng.

Không commit offset chỉ vì consumer đã đọc message.

---

## 6. Giao diện tích hợp với nhóm AI

Để hai thành viên làm việc độc lập, cần định nghĩa rõ đầu vào và đầu ra.

### 6.1. Đầu vào của inference pipeline

Đề xuất Python interface:

```python
def predict_transactions(
    transactions: list[dict]
) -> list[dict]:
    ...
```

Input:

```python
[
    {
        "transactionId": "TXN0001",
        "senderAccountId": "ACC001",
        "receiverAccountId": "ACC002",
        "amount": 50000000,
        "timestamp": "2026-10-13T10:00:00Z"
    }
]
```

Output:

```python
[
    {
        "transactionId": "TXN0001",
        "anomalyScore": 0.95,
        "isAnomaly": True,
        "modelVersion": "gatv2-v1",
        "reasonCodes": []
    }
]
```

Interface này là **hợp đồng đề xuất**, chưa phải API đã được triển khai.

Nếu pipeline cần graph state và timestamp của batch, nhóm AI có thể sử dụng một class `InferenceEngine` thay vì function stateless.

### 6.2. Trách nhiệm nhóm Kafka

Nhóm Kafka chịu trách nhiệm:

- Chuyển Kafka messages thành cấu trúc đầu vào thống nhất.
- Gọi inference interface.
- Ghép kết quả với `transactionId`.
- Publish prediction events.
- Quản lý offset, retry và logging.

Không triển khai lại preprocessing hoặc graph construction mà nhóm AI đã cung cấp.

### 6.3. Trách nhiệm nhóm AI

Nhóm AI chịu trách nhiệm:

- Cung cấp model checkpoint và preprocessing artifacts.
- Cung cấp interface inference.
- Xác định input schema cần thiết.
- Xác định cách quản lý graph state.
- Quy định cách tính anomaly score.
- Xác định ngưỡng `isAnomaly`.
- Đảm bảo mapping giữa input transactions và predictions.
- Cung cấp cách khôi phục hoặc tái tạo graph state khi replay.

---

## 7. Xử lý lỗi và khôi phục hệ thống

| Trường hợp | Hướng xử lý |
|---|---|
| Event JSON không hợp lệ | Chuyển DLQ sau khi xác nhận ghi thành công |
| Thiếu thuộc tính bắt buộc | Validate và ghi nhận lỗi schema |
| Kafka tạm mất kết nối | Retry với backoff |
| GATv2 inference thất bại | Không commit offset; retry có giới hạn hoặc tạm dừng để xử lý |
| MongoDB mất kết nối | Retry; không commit message chưa lưu |
| Consumer restart | Resume từ committed offset |
| Event bị xử lý trùng | Idempotent upsert và chống trùng graph state |
| Model chưa được load | Consumer không bắt đầu xử lý inference |
| Event đến trễ hoặc sai thứ tự | Áp dụng chính sách event-time do nhóm AI thống nhất |

**Lưu ý:** Nếu graph state chỉ được lưu trong RAM, việc consumer restart có thể làm mất lịch sử đang được dùng để suy luận. Khi đó, replay giao dịch hoặc khôi phục checkpoint phải tái tạo được graph state trước khi tiếp tục inference.

Đây là yêu cầu đặc biệt quan trọng khi sử dụng GATv2.

---

## 8. Cấu trúc project đề xuất

```text
financial-anomaly-detection/
│
├── infrastructure/
│   └── docker-compose.yml
│
├── producer/
│   ├── main.py
│   └── config.py
│
├── consumer/
│   ├── main.py
│   ├── config.py
│   ├── event_handler.py
│   └── result_publisher.py
│
├── inference/
│   ├── predictor.py
│   ├── preprocessing.py
│   ├── graph_builder.py
│   └── artifacts/
│
├── sink/
│   └── mongodb_consumer.py
│
├── contracts/
│   └── event_schema.md
│
└── README.md
```

Đây là cấu trúc tham khảo, không bắt buộc phải tạo đủ mọi file nếu logic còn đơn giản.

`consumer/event_handler.py` chỉ điều phối và gọi `inference/predictor.py`; không viết lại logic của mô hình.

---

## 9. Các bước triển khai

### Phase 1 — Kafka Infrastructure

- [ ] Tạo Docker Compose.
- [ ] Khởi động Kafka.
- [ ] Tạo các topics cần thiết.
- [ ] Kiểm tra kết nối producer và consumer.
- [ ] Kiểm tra publish/consume JSON message.

**Kết quả:** Kafka hoạt động độc lập, gửi và nhận sự kiện thành công.

### Phase 2 — Transaction Producer

- [ ] Đọc dữ liệu CSV.
- [ ] Mapping dữ liệu theo event schema.
- [ ] Publish vào `transactions.raw`.
- [ ] Cấu hình tốc độ phát giao dịch.
- [ ] Theo dõi số message gửi thành công/thất bại.

**Kết quả:** Giả lập được luồng giao dịch liên tục.

### Phase 3 — Inference Consumer Integration

- [ ] Tạo Python Kafka Consumer.
- [ ] Thực hiện micro-batching.
- [ ] Gọi inference interface với mock prediction.
- [ ] Publish kết quả sang `transactions.scored`.
- [ ] Publish bất thường sang `transactions.anomalies`.
- [ ] Triển khai manual offset commit.

**Kết quả:** Hoàn thành luồng Kafka mà chưa cần mô hình GATv2 thực tế.

### Phase 4 — GATv2 Integration

- [ ] Tích hợp pipeline inference do nhóm AI cung cấp.
- [ ] Load trained model.
- [ ] Kiểm tra đầu vào và đầu ra.
- [ ] Xử lý graph state.
- [ ] Kiểm tra khả năng replay.
- [ ] Đo inference latency.

**Kết quả:** Consumer phát hiện được bất thường bằng mô hình thực.

### Phase 5 — MongoDB Integration

- [ ] Tạo MongoDB Sink.
- [ ] Lưu các prediction events.
- [ ] Tạo unique indexes phù hợp.
- [ ] Xử lý upsert.
- [ ] Kiểm tra duplicate events.

**Kết quả:** Kết quả inference được lưu trữ để truy vấn.

### Phase 6 — System Testing

- [ ] Kiểm tra luồng từ Producer đến MongoDB.
- [ ] Kiểm tra consumer restart.
- [ ] Kiểm tra sự kiện không hợp lệ.
- [ ] Kiểm tra producer gửi dữ liệu nhanh hơn consumer.
- [ ] Đo throughput và latency.
- [ ] Đo consumer lag.
- [ ] Thu thập log và kết quả benchmark.

---

## 10. Integration Test Cases

| ID | Test case | Expected result |
|---|---|---|
| INT-01 | Producer gửi 100 giao dịch hợp lệ | Consumer xử lý đủ 100 giao dịch |
| INT-02 | Gửi một event sai schema | Event được xử lý theo chính sách lỗi, không làm consumer crash liên tục |
| INT-03 | Tắt consumer trong lúc producer chạy | Kafka tiếp tục lưu các sự kiện trong giới hạn cấu hình |
| INT-04 | Khởi động lại consumer | Tiếp tục xử lý từ committed offset và khôi phục graph state |
| INT-05 | Gửi một transactionId trùng | Không tạo thêm bản ghi dự đoán trùng trong MongoDB |
| INT-06 | Inference trả về isAnomaly = true | Event được publish vào topic bất thường |
| INT-07 | MongoDB tạm thời không hoạt động | Sink không đánh dấu hoàn tất dữ liệu chưa lưu |
| INT-08 | Tăng tốc producer | Thu thập được throughput, latency và consumer lag |
| INT-09 | Publish result thất bại | Offset đầu vào không được commit như thể đã hoàn tất |
| INT-10 | Replay cùng dữ liệu lịch sử | Không phát sinh dữ liệu trùng ngoài quy tắc đã thiết kế |

---

## 11. Đánh giá hiệu năng

Các chỉ số cần thu thập:

**Throughput**

Số lượng giao dịch được xử lý thành công mỗi giây.

**End-to-end latency**

Thời gian từ khi Producer publish giao dịch đến khi prediction được lưu thành công hoặc sẵn sàng cho downstream consumer, tùy điểm kết thúc đo đã chọn.

**Inference latency**

Thời gian mô hình GATv2 cần để phân tích một micro-batch.

**Consumer lag**

Số lượng records chưa được consumer group xử lý đến vị trí committed offset.

**Error rate**

Tỷ lệ events không được xử lý thành công trong điều kiện thử nghiệm.

Các kịch bản:

| Kịch bản | Input rate | Điều cần quan sát |
|---|---|---|
| TEST-01 | 100 events/s | Throughput, latency |
| TEST-02 | 500 events/s | Throughput, consumer lag |
| TEST-03 | 1.000 events/s | Throughput, backlog, latency |
| TEST-04 | Burst traffic | Khả năng đệm tải |
| TEST-05 | Consumer restart | Khôi phục xử lý |
| TEST-06 | Inference chậm | Ảnh hưởng của GATv2 đến Kafka lag |

Các thông số phải đo thực tế trên môi trường triển khai. Không giả định hệ thống đáp ứng được các tải trên trước khi chạy benchmark.

---

## 12. Tiêu chí nghiệm thu

Phần Kafka được xem là hoàn thành MVP khi:

- Producer có thể phát giao dịch từ dataset vào Kafka.
- Topic đầu vào và đầu ra hoạt động đúng schema.
- Inference Consumer đọc được giao dịch và gọi được mock hoặc real inference.
- Mỗi kết quả được publish đến topic thích hợp.
- MongoDB Sink lưu dữ liệu thành công.
- Có khả năng xử lý lại giao dịch sau khi consumer restart.
- Không tạo bản ghi dự đoán trùng khi cùng transaction được xử lý lại.
- Có logging và xử lý các sự kiện không hợp lệ.
- Có báo cáo throughput, latency và consumer lag.
- Nhóm có thể chạy được hệ thống bằng hướng dẫn README.

**Kết quả bàn giao tối thiểu:**

1. Docker Compose cho Kafka và MongoDB.
2. Source code Producer.
3. Source code Kafka Consumer và Result Publisher.
4. Source code MongoDB Sink.
5. Event schema thống nhất.
6. File cấu hình mẫu.
7. Hướng dẫn chạy và tích hợp.
8. Kết quả integration test và benchmark.

---

## 13. Các quyết định cần thống nhất trước khi coding

1. Dataset giao dịch có các trường gì? Có đầy đủ sender/receiver account không?
2. Mô hình GATv2 dự đoán theo transaction, node hay edge?
3. Inference sử dụng toàn bộ graph hay subgraph theo time window?
4. Graph state được lưu và khôi phục như thế nào?
5. Kích thước micro-batch và độ trễ tối đa chấp nhận được là bao nhiêu?
6. Có cần lưu cả giao dịch gốc trong MongoDB không?
7. Hệ thống có cần replay để so sánh nhiều model version không?
8. Khi inference gặp lỗi, nên retry, pause consumer hay chuyển sang DLQ?
9. Dashboard nhận cảnh báo bằng polling API hay WebSocket/SSE?
10. Các chỉ số thực nghiệm sẽ được đo trên cùng cấu hình phần cứng nào?

Những quyết định này ảnh hưởng trực tiếp đến event schema, consumer flow và cách khôi phục trạng thái.

---

## 14. Kết luận

Trong hệ thống này, Kafka đóng vai trò là nền tảng kết nối các thành phần theo kiến trúc Event-Driven, hỗ trợ truyền tải sự kiện giao dịch, xử lý bất đồng bộ và replay dữ liệu.

GATv2 Inference Consumer chịu trách nhiệm phân tích đồ thị giao dịch và xác định mức độ bất thường, trong khi MongoDB đảm nhiệm việc lưu trữ phục vụ truy vấn và trực quan hóa.

Thiết kế tách biệt Kafka Infrastructure và AI Inference Pipeline cho phép các thành viên phát triển độc lập, tích hợp thông qua event contracts và mở rộng hệ thống trong các giai đoạn tiếp theo.
