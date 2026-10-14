# PaySim training data pipeline

Pipeline chuẩn bị dữ liệu offline theo `financial_anomaly_kafka_paysim_design_v2.md`. Phạm vi hiện tại dừng trước graph builder và model; các artifact có schema rõ ràng để tích hợp model sau.

## Data flow

`CSV → schema/load → pre-clean EDA report → validate + quarantine → chronological split → safe feature/label export`

- Giữ nguyên bản ghi hợp lệ; không deduplicate giao dịch, không clip/outlier removal hay sửa balance theo suy đoán.
- Hàng thiếu/sai schema hoặc sai miền cơ bản được đưa vào `rejected.parquet` kèm lý do.
- `eda/raw_dataset_report.json` được tạo trước cleaning, gồm missing/blank, numeric summaries, type/label counts, exact duplicates, thứ tự `step` và balance consistency. EDA không mutate/drop dữ liệu; nhãn trên toàn tập chỉ là mô tả khám phá, không dùng để fit/tune/chọn threshold.
- Chia theo các giá trị `step` nguyên vẹn; cùng một step không bị xé qua hai tập. Mặc định 70/15/15 theo thứ tự thời gian.
- `features/*.parquet` chỉ theo whitelist `transactionId, sourceRowIndex, step, type, amount, nameOrig, nameDest`; IDs và step là metadata/graph structure, chưa biến thành vector số.
- `ground_truth/*.parquet` tách nhãn `isFraud` và `isFlaggedFraud`; balance không xuất hiện trong features. Chỉ dùng nhãn train/validation cho fit/tuning, test chỉ đánh giá cuối.
- `feature_schema.json` và `manifest.json` mô tả contract, ranh giới step, số hàng và dataset version.
- Test `gatv2-pipeline/tests/test_pipeline_parity.py` đối chiếu thứ tự cột với `contracts.events.ModelInputDTO`; pipeline fail closed nếu whitelist offline drift.
- Manifest ghi `rowsInput`, `rowsAccepted`, `rowsRejected` và phân bố `rejectionReasons`; balance consistency không bị dùng làm reject rule tự động.

## Run

Python 3.11+ (Nix dev shell dùng Python 3.12).

```sh
nix develop .#paysim
# Từ thư mục gốc repo; Makefile tự cấu hình đường dẫn dataset/output.
PYTHONPATH=gatv2-pipeline/src python -m paysim_pipeline.cli --config gatv2-pipeline/configs/pipeline.toml
```

Từ thư mục gốc repo, Makefile hỗ trợ các bước thường dùng. Nix flake cung cấp Python, pandas và pyarrow; không cần pip hay Kaggle CLI:

```sh
make dataset-download       # tạo data/ và fetch public dataset từ Kaggle
make dataset-check
make eda                    # chỉ chạy EDA, chưa cleaning
# Xem gatv2-pipeline/artifacts/paysim/eda/raw_dataset_report.json
make prepare                # chạy pipeline trong Nix dev shell
make train                  # hiện báo thiếu module train; model chưa được triển khai
```

Có thể đổi đường dẫn mà không sửa TOML:

```sh
make dataset-download PAYSIM_DATA_DIR=/data/paysim
make prepare PAYSIM_CSV=/data/paysim/PS_20174392719_1491204439457_log.csv
```

`make train` mặc định tìm `paysim_pipeline.train` với config `gatv2-pipeline/configs/train.toml`; đổi bằng `TRAIN_MODULE` và `TRAIN_CONFIG` khi bổ sung trainer. Target chạy `prepare` trước để train luôn nhận artifact mới nhất. Dataset công khai được tải trực tiếp từ Kaggle; không cần token/API key.

## Kafka ONNX inference mock

Repo chưa có checkpoint GATv2 đã train. Để Kafka member tích hợp trước, mock ONNX graph được xuất trực tiếp (không giả lập quá trình train) và Go inference consumer gọi ONNX Runtime:

```sh
make mock-onnx-export
make onnx-test
make consumer
```

Artifact mặc định là `gatv2-pipeline/artifacts/mock/anomaly_score.onnx`. Go chuyển mỗi DTO thành tensor float32 `[log1p(amount), step/744, one-hot(type)]`, gọi ONNX input `features` và đọc score từ output `scores`; score được ngưỡng hóa thành prediction event JSON trên Kafka. `MODEL_PATH`, `ANOMALY_THRESHOLD` và `MODEL_VERSION` có thể override.

Hướng dẫn Kafka member về cách chạy producer/consumer/sinks, input/output event contract và thay mock bằng model thật: [`KAFKA_ONNX_INTEGRATION.md`](../KAFKA_ONNX_INTEGRATION.md).

Đây chỉ là model feed-forward giả lập để kiểm thử gọi model và mapping event. Feature layout/graph inputs của GATv2 thật chưa được chốt; khi có model thật, nhóm AI cần export đúng input/output contract và Go adapter cần map cùng feature schema đó. Không dùng mock để báo cáo metric ML.

Có thể dùng config TOML khác. Đường dẫn tương đối được tính từ thư mục `gatv2-pipeline`; output mặc định ở `artifacts/paysim/`.

`make eda` dừng sau báo cáo khám phá. Hãy xem report trước khi quyết định thêm xử lý outlier, missing values, imbalance hoặc augmentation. `make prepare` hiện chỉ áp dụng validation/quarantine cơ bản rồi split/export; nó chưa áp dụng các chiến lược này.

## Explicitly deferred

Graph construction/window semantics, category encoding/scaling fit chỉ trên train, imbalance strategy, model training, threshold selection, metrics/checkpointing, online graph-state recovery. Các quyết định này phụ thuộc GATv2 input/edge-level contract chưa được chốt trong design. Không ghi model giả hoặc metric giả.

## Resource note

Đang dùng pandas eager DataFrame, dễ đọc/đổi stage nhưng cần RAM đủ cho toàn bộ PaySim (khoảng 6.3M dòng). Khi môi trường bị giới hạn RAM, thay stage load/export bằng xử lý chunk/Parquet mà vẫn giữ nguyên split theo `step` và schema contract.
