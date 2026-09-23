# SPEC — Demo Kafka vs Không-Kafka

> Tài liệu đặc tả cho bài tìm hiểu trên lớp: dựng một hệ thống nhỏ (React TS + Go + Kafka)
> để **so sánh trực quan** kiến trúc gọi HTTP đồng bộ và kiến trúc event-driven qua Kafka.
> Mọi phần code theo tài liệu này. Chữ trên giao diện và tài liệu: tiếng Việt.
> Tên biến/hàm và comment trong code: tiếng Anh.

---

## 1. Mục tiêu

| # | Mục tiêu | Thể hiện trên demo |
|---|---|---|
| M1 | Cho thấy **độ trễ người dùng phải chờ** khác nhau thế nào | Cùng một đơn hàng: luồng đồng bộ ~1.3s, luồng Kafka ~20ms |
| M2 | Cho thấy **tính chịu lỗi** | Tắt service Email: luồng đồng bộ hỏng cả đơn, luồng Kafka vẫn nhận đơn và xử lý bù sau |
| M3 | Cho thấy **khả năng hấp thụ tải** | Bắn 100 đơn: luồng đồng bộ timeout/lỗi, luồng Kafka nhận hết rồi xử lý dần |
| M4 | Cho thấy **scale ngang bằng consumer group** | Bật thêm instance service Kho, partition được chia lại, throughput tăng |
| M5 | Cho thấy **giảm coupling (fan-out)** | Gateway đồng bộ phải biết tên 3 service; bên Kafka chỉ publish 1 lần, thêm service mới không sửa gateway |

Nghiệp vụ mô phỏng: **user đặt đơn → trừ tồn kho → gửi mail xác nhận → tạo vận đơn**.

### Nguyên tắc thiết kế quan trọng

Ba service dùng **chung một hàm nghiệp vụ**, chỉ khác đường vào:

- **HTTP handler** `POST /process` → dùng cho luồng đồng bộ.
- **Kafka consumer** đọc topic `orders` → dùng cho luồng event-driven.

Nhờ vậy khi thuyết trình có thể chỉ thẳng vào code: *"nghiệp vụ y hệt nhau, chỉ đổi cách giao tiếp,
và đây là toàn bộ khác biệt về kết quả"*.

---

## 2. Kiến trúc

```
                 ┌── POST /api/orders/sync ──► inventory:8081 ──► email:8082 ──► shipping:8083
                 │        (HTTP tuần tự — user chờ tới khi cả 3 service xong)
React (5173) ──► gateway:8080
   ▲             └── POST /api/orders/async ─► topic "orders" (3 partitions, key = productId)
   │  WebSocket                                        │
   │  /ws                          ┌──────────────────┼────────────────────┐
   │                       inventory-group      email-group          shipping-group
   └── log realtime ◄──────────────┴──────────────────┴────────────────────┘
       (mỗi service POST /internal/log về gateway sau mỗi bước)
```

**Vì sao log đi bằng HTTP chứ không qua Kafka:** để luồng đồng bộ vẫn hiện log được ngay cả khi
broker chết — nếu log cũng qua Kafka thì demo "không có Kafka" sẽ không có gì để xem.

### Tiến trình cần chạy

| Tiến trình | Cổng | Vai trò |
|---|---|---|
| Kafka broker (Docker, KRaft 1 node) | 9092 | Message broker |
| Kafka UI (kafbat/kafka-ui) | 8090 | Xem topic, message, consumer lag khi thuyết trình |
| `gateway` | 8080 | REST API + WebSocket cho UI; producer Kafka |
| `inventory` | 8081 | Trừ tồn kho — HTTP handler + consumer `inventory-group` |
| `email` | 8082 | "Gửi mail" — HTTP handler + consumer `email-group` |
| `shipping` | 8083 | Tạo vận đơn — HTTP handler + consumer `shipping-group` |
| `inventory` instance #2 | 8084 | Chỉ bật khi demo M4 (consumer group scale) |
| Vite dev server | 5173 | Giao diện React |

---

## 3. Cấu trúc thư mục

```
kafka-learning/
├─ SPEC.md                    tài liệu này
├─ README.md                  hướng dẫn chạy + kịch bản thuyết trình
├─ docker-compose.yml         kafka (KRaft) + kafka-ui
├─ Makefile                   kafka-up, gateway, inventory, email, shipping, inventory-2, web
├─ server/
│  ├─ go.mod                  module kafka-demo/server (Go 1.25)
│  ├─ cmd/
│  │  ├─ gateway/main.go      REST + WebSocket + producer
│  │  ├─ inventory/main.go    ~40 dòng: khai báo nghiệp vụ rồi gọi service.Run
│  │  ├─ email/main.go
│  │  └─ shipping/main.go
│  └─ internal/
│     ├─ events/events.go     Order, StepLog, tên topic & consumer group
│     ├─ broker/broker.go     tạo topic, Producer (kafka.Writer), Reader (consumer group)
│     ├─ hub/hub.go           WebSocket hub + ring buffer 300 log gần nhất
│     ├─ service/service.go   khung dùng chung: HTTP + consumer + toggle up/down + báo log
│     └─ store/store.go       tồn kho in-memory (mutex)
└─ web/
   ├─ package.json            Vite + React 19 + TypeScript
   └─ src/
      ├─ App.tsx
      ├─ api.ts  useSocket.ts  types.ts  styles.css
      └─ components/
         ├─ ControlBar.tsx    chọn sản phẩm/số lượng, các nút demo
         ├─ FlowColumn.tsx    một cột (đồng bộ hoặc Kafka)
         ├─ FlowDiagram.tsx   sơ đồ SVG, chấm message chạy theo cạnh
         ├─ LatencyBar.tsx    số ms cỡ lớn + thanh so sánh
         ├─ LogList.tsx       log realtime
         ├─ StatePanel.tsx    tồn kho + số đơn mỗi service đã xử lý
         └─ BurstResult.tsx   bảng p50/p95/max của hai chế độ
```

---

## 4. Mô hình dữ liệu

```go
// events.Order — vừa là body HTTP của luồng đồng bộ, vừa là value của message Kafka
type Order struct {
    ID        string    `json:"id"`        // "ORD-7F3A"
    ProductID string    `json:"productId"` // "SP-01"
    Qty       int       `json:"qty"`
    Mode      string    `json:"mode"`      // "sync" | "kafka"
    Burst     bool      `json:"burst"`     // true nếu sinh ra từ nút "bắn N đơn"
    CreatedAt time.Time `json:"createdAt"`
}

// events.StepLog — một dòng log realtime đẩy về UI qua WebSocket
type StepLog struct {
    ID         string    `json:"id"`
    Mode       string    `json:"mode"`       // "sync" | "kafka" → UI biết đẩy vào cột nào
    Service    string    `json:"service"`    // "gateway" | "inventory" | "email" | "shipping"
    Instance   string    `json:"instance"`   // "inventory-1", "inventory-2"
    OrderID    string    `json:"orderId"`
    ProductID  string    `json:"productId"`
    Qty        int       `json:"qty"`
    Status     string    `json:"status"`     // "start" | "done" | "error"
    Message    string    `json:"message"`    // câu tiếng Việt hiện thẳng lên UI
    Partition  int       `json:"partition"`  // -1 nếu bước này không đến từ Kafka
    Offset     int64     `json:"offset"`
    DurationMs int64     `json:"durationMs"`
    Burst      bool      `json:"burst"`
    At         time.Time `json:"at"`
}
```

**Kafka:** topic `orders`, **3 partitions**, replication factor 1, **key = `productId`**.
Chọn key là `productId` để cùng một sản phẩm luôn vào cùng một partition → các đơn của sản phẩm đó
được xử lý **đúng thứ tự**, không tranh chấp khi trừ kho. Đây là một điểm giảng quan trọng.

Tồn kho khởi tạo: `SP-01: 100`, `SP-02: 100`, `SP-03: 100`.

---

## 5. API

### 5.1 Gateway (`:8080`)

| Method | Path | Body / Query | Trả về |
|---|---|---|---|
| POST | `/api/orders/sync` | `{productId, qty}` | `{orderId, totalMs, steps[], ok}` — chỉ trả về **sau khi cả 3 service xong**; một service lỗi ⇒ `502` kèm `steps` đã chạy |
| POST | `/api/orders/async` | `{productId, qty}` | `202 {orderId, totalMs}` — publish xong là trả ngay (~20ms) |
| POST | `/api/burst` | `{mode, count, productId}` | `{mode, count, ok, failed, p50, p95, max, elapsedMs}` |
| GET | `/api/state` | — | `{stock:{...}, services:[{name, instance, down, processed}]}` |
| GET | `/api/logs` | — | 300 log gần nhất (để UI mới mở vẫn có lịch sử) |
| POST | `/api/reset` | — | reset tồn kho, đếm lại từ 0, xoá log |
| POST | `/api/services/{name}/down` | — | gọi `/admin/down` của service đó |
| POST | `/api/services/{name}/up` | — | gọi `/admin/up` |
| GET | `/ws` | — | WebSocket: server đẩy `StepLog` mỗi khi có bước mới |
| POST | `/internal/log` | `StepLog` | service gọi vào; gateway broadcast cho UI |

`steps[]` trong response sync: `[{service, ok, durationMs, message}]`.

### 5.2 Mỗi service nghiệp vụ (`:8081` / `:8082` / `:8083`)

| Method | Path | Ý nghĩa |
|---|---|---|
| POST | `/process` | Xử lý một đơn theo kiểu đồng bộ. `503` nếu đang "down" hoặc quá tải |
| GET | `/state` | `{name, instance, down, processed, stock?}` |
| POST | `/admin/down` | Bật chế độ hỏng: HTTP trả `503` **và** đóng Kafka reader (message dồn trong topic) |
| POST | `/admin/up` | Mở lại: tạo reader mới, đọc tiếp **từ offset đã commit** → xử lý bù |

---

## 6. Tham số mô phỏng

Đặt qua biến môi trường, mặc định:

| Tham số | Giá trị | Ghi chú |
|---|---|---|
| `WORK_MS` inventory | 200 | thời gian "xử lý" giả lập |
| `WORK_MS` email | 700 | mail luôn là khâu chậm nhất |
| `WORK_MS` shipping | 400 | |
| ⇒ tổng luồng đồng bộ | ~1300 ms | user phải chờ đúng bằng tổng |
| ⇒ luồng Kafka | ~20 ms | chỉ tốn thời gian ghi vào topic |
| `CAPACITY` | 5 | số request HTTP xử lý song song tối đa mỗi service |
| `QUEUE_TIMEOUT_MS` | 2000 | chờ quá lâu để tới lượt ⇒ `503` (mô phỏng quá tải) |
| `SYNC_CALL_TIMEOUT_MS` | 3000 | timeout gateway gọi từng service |

Consumer **không** dùng `CAPACITY`: nó xử lý tuần tự theo nhịp của mình, không bao giờ timeout —
message chỉ nằm chờ trong topic và làm **lag** tăng. Đây chính là điểm khác biệt cần giảng ở M3.

---

## 7. Giao diện

Một trang duy nhất, nền tối, chữ lớn để chiếu máy chiếu. Ba tầng:

**Tầng 1 — Thanh điều khiển**
- Chọn sản phẩm + số lượng.
- `Đặt hàng (Không Kafka)` · `Đặt hàng (Có Kafka)`
- `Bắn N đơn (Không Kafka)` · `Bắn N đơn (Có Kafka)` — N chỉnh được, mặc định 50.
- `Tắt service Email` / `Bật lại` · `Reset`

**Tầng 2 — Hai cột so sánh** (trái đỏ "Không Kafka (đồng bộ)", phải xanh "Có Kafka (event-driven)")

Mỗi cột gồm:
1. `FlowDiagram` — sơ đồ SVG. Cột trái: `Gateway → Kho → Mail → Ship` nối tiếp. Cột phải:
   `Gateway → [Topic orders] → 3 service song song`. Message là một chấm sáng chạy theo cạnh;
   node đang xử lý thì sáng, node bị tắt thì xám và gạch chéo.
2. `LatencyBar` — số **ms người dùng phải chờ** cỡ rất lớn + thanh tỉ lệ so với cột kia.
3. Timeline các bước: `✓` xong / `✗` lỗi / `⏳` đang chạy, kèm ms từng bước.
4. `LogList` — log realtime. Dòng bên Kafka hiện thêm `instance · partition · offset`.

**Tầng 3 —** `StatePanel` (tồn kho từng sản phẩm, số đơn mỗi service đã xử lý, service nào đang tắt)
và `BurstResult` (bảng p50/p95/max/số lỗi của hai chế độ cạnh nhau).

Kỹ thuật: Vite + React + TS, **CSS thuần** (không Tailwind, không thư viện chart — thanh bar bằng
`div`). WebSocket tự kết nối lại khi rớt. Log của burst (`burst: true`) không đổ vào LogList mà chỉ
cập nhật bộ đếm, tránh ngập màn hình.

---

## 8. Kịch bản thuyết trình

| # | Thao tác | Kết quả mong đợi | Câu chốt |
|---|---|---|---|
| 1 | Bấm hai nút đặt hàng | Trái ~1300ms, phải ~20ms | "Cùng một nghiệp vụ, người dùng chờ gấp 60 lần" |
| 2 | `Tắt service Email` → đặt hai đơn | Trái: lỗi 502, đơn hỏng. Phải: vẫn `202`, mở Kafka UI thấy `email-group` lag tăng | "Kafka giữ hộ message" |
| 3 | `Bật lại Email` | Log mail chạy bù đủ số đơn còn nợ | "Đọc tiếp từ offset đã commit — không mất đơn nào" |
| 4 | Bắn 50 đơn hai bên | Trái: nhiều lỗi 503/timeout, p95 rất cao. Phải: nhận đủ 50, consumer xử lý dần | "Kafka làm tấm đệm hấp thụ tải" |
| 5 | Mở terminal 2: `make inventory-2` | Log hiện hai instance xử lý các partition khác nhau; Kafka UI thấy 2 member trong group | "Thêm instance = thêm sức xử lý, không đổi code" |
| 6 | Chỉ vào code `cmd/gateway` | Luồng đồng bộ gọi tên 3 service; luồng Kafka chỉ `Publish` một lần | "Thêm service mới chỉ cần thêm consumer group" |

Thuật ngữ cần giải thích trong README: producer, consumer, topic, partition, offset, consumer group,
lag, at-least-once, fan-out.

---

## 9. Ngoài phạm vi

Không làm: gửi mail thật, xác thực, Docker hoá phần Go/React (chạy bằng `go run` và
`npm run dev` cho dễ demo và dễ sửa tại chỗ; chỉ Kafka, Kafka UI và MongoDB chạy Docker),
Schema Registry, Avro, exactly-once/transaction,
dead-letter queue. Nếu còn thời gian có thể bàn thêm ở phần Q&A.

---

## 10. Thứ tự thực hiện

1. `docker-compose.yml` + `Makefile` + `go.mod`; `make kafka-up`, kiểm tra Kafka UI ở `:8090`.
2. `internal/events`, `internal/store`, `internal/broker`.
3. `internal/service` (khung dùng chung) → ba binary `inventory` / `email` / `shipping`.
4. `cmd/gateway`: luồng sync, luồng async, `/internal/log`, WebSocket hub.
5. `/api/burst`, `/api/state`, `/api/reset`, proxy down/up.
6. `web`: scaffold Vite, `useSocket`, ControlBar, FlowColumn → chạy được kịch bản 1 và 2.
7. `FlowDiagram` (SVG animation), `StatePanel`, `BurstResult`.
8. `README.md`: cách chạy, 6 kịch bản kèm lời thoại, giải thích thuật ngữ, bảng ưu/nhược để đưa vào slide.

## 11. Tiêu chí hoàn thành

- `make kafka-up` + 4 tiến trình Go + `npm run dev` là chạy được toàn bộ demo.
- Cả 6 kịch bản ở mục 8 tái hiện được, không cần gõ lệnh phụ (trừ kịch bản 5 cần terminal thứ hai).
- Sau `Reset`, tồn kho và bộ đếm về đúng giá trị ban đầu.
- `go vet ./...` và `npm run build` sạch lỗi.

---

## 12. MongoDB (ngoài SPEC gốc — thêm để trực quan)

MongoDB là dependency bắt buộc (`make kafka-up` kéo theo). Xem bằng Compass:
`mongodb://localhost:27017/kafka-demo`. Biến môi trường: `MONGO_URI`, `MONGO_DB`.

| Collection | Nội dung | Ai ghi |
|---|---|---|
| `stock` | `{_id: productId, stock}` — tồn kho **dùng chung** mọi instance inventory (trừ atomic `$inc` có guard `$gte`) | inventory (seed + trừ + reset) |
| `orders` | `{_id, productId, qty, mode, burst, status, steps[], totalMs, createdAt}` — lịch sử + timeline từng đơn | gateway (tạo/xóa) + gateway gom từ `StepLog` ở `/internal/log` |

`status`: `received → processing → done | error`. Đơn kafka `done` khi đủ 3 bước
flow; `totalMs` của đơn kafka là end-to-end (tạo → xong), của đơn sync là thời gian
user phải chờ. API thêm: `GET /api/orders?limit=50` (mới nhất trước) phục vụ bảng
lịch sử ở FE. `POST /api/reset` xoá cả `orders`.
