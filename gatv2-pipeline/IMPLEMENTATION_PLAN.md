# Pipeline plan and coverage review

Phạm vi: pipeline dữ liệu train/evaluate; chưa triển khai GATv2 hay suy diễn các quyết định graph còn mở.

## Pass 1 — Hợp đồng và nguồn dữ liệu

- [x] Đọc đúng 11 cột PaySim; fail sớm khi thiếu/thừa cột.
- [x] Tạo `sourceRowIndex` từ thứ tự dòng file ban đầu và `transactionId` ổn định `paysim-{sourceRowIndex}`.
- [x] Giữ `step` là giờ mô phỏng; tie-break theo source row.
- [x] Tách `isFraud`/`isFlaggedFraud` thành ground truth.
- [x] Cấm balance và labels trong model input bằng whitelist.

## Pass 2 — Làm sạch có thể kiểm toán

- [x] Validate kiểu, giá trị hữu hạn, miền `step`, `amount`, `type`, nhãn nhị phân và node IDs.
- [x] Đưa hàng sai vào quarantine kèm `rejectReason`, không âm thầm drop.
- [x] Không loại giao dịch trùng, không clip/outlier removal, không tự sửa balance.
- [ ] Bổ sung thống kê chất lượng dữ liệu sâu hơn sau khi chốt quy tắc domain (balance consistency không được tự xem là lỗi).

## Pass 3 — Split và chống leakage

- [x] Chia train/validation/test theo thứ tự thời gian trên distinct `step`.
- [x] Không chia cùng một step sang nhiều tập.
- [x] Giữ validation/test ở tương lai so với train.
- [x] Ghi ranh giới và số lượng từng split vào manifest.
- [ ] Sau khi có graph/window, kiểm tra context warm-up/edge boundary không lấy nhãn hoặc node/edge tương lai.

## Pass 4 — Artifact và điểm nối model

- [x] Xuất Parquet riêng cho features và labels.
- [x] Xuất versioned feature schema, dataset version và manifest.
- [x] Giữ account identifiers cho graph builder nhưng khai báo chúng không phải vector số.
- [ ] Bổ sung graph builder theo quyết định edge-level/node-level, window và thứ tự update-vs-predict.
- [ ] Fit encoder/scaler chỉ từ train; lưu preprocessing artifact và tái sử dụng y hệt khi inference.
- [ ] Thêm train/evaluate/checkpoint/threshold selection sau khi model được bổ sung.

## Pass 5 — Vận hành và xác nhận

- [x] TOML config, CLI và README.
- [x] Ghi rõ giới hạn RAM của pandas eager trên PaySim đầy đủ.
- [x] Thêm unit/integration tests trên dataset nhỏ có quarantine, Parquet export và manifest.
- [x] Đối chiếu feature schema offline với `ModelInputDTO` của Kafka adapter.

## Coverage review

Đã rà ngược các điểm trong design v2: schema PaySim, giữ balances ở dữ liệu nguồn nhưng loại khỏi input, nhãn offline-only, thứ tự `step`, ID bền vững, chronological split, artifact metadata và online/offline feature parity. Chưa triển khai graph/model vì design còn để mở edge/node target, window, prediction timing, score/threshold và state recovery.
