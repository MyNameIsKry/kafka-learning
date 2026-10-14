# PaySim training data pipeline

Pipeline chuẩn bị dữ liệu offline theo `financial_anomaly_kafka_paysim_design_v2.md`. Phạm vi hiện tại dừng trước graph builder và model; các artifact có schema rõ ràng để tích hợp model sau.

## Data flow

`CSV → schema/load → validate + quarantine → chronological split → safe feature/label export`

- Giữ nguyên bản ghi hợp lệ; không deduplicate giao dịch, không clip/outlier removal hay sửa balance theo suy đoán.
- Hàng thiếu/sai schema hoặc sai miền cơ bản được đưa vào `rejected.parquet` kèm lý do.
- Chia theo các giá trị `step` nguyên vẹn; cùng một step không bị xé qua hai tập. Mặc định 70/15/15 theo thứ tự thời gian.
- `features/*.parquet` chỉ theo whitelist `transactionId, sourceRowIndex, step, type, amount, nameOrig, nameDest`; IDs và step là metadata/graph structure, chưa biến thành vector số.
- `ground_truth/*.parquet` tách nhãn `isFraud` và `isFlaggedFraud`; balance không xuất hiện trong features. Chỉ dùng nhãn train/validation cho fit/tuning, test chỉ đánh giá cuối.
- `feature_schema.json` và `manifest.json` mô tả contract, ranh giới step, số hàng và dataset version.
- Test `gatv2-pipeline/tests/test_pipeline_parity.py` đối chiếu thứ tự cột với `contracts.events.ModelInputDTO`; pipeline fail closed nếu whitelist offline drift.
- Manifest ghi `rowsInput`, `rowsAccepted`, `rowsRejected` và phân bố `rejectionReasons`; balance consistency không bị dùng làm reject rule tự động.

## Run

Python 3.11+.

```sh
cd gatv2-pipeline
python -m venv .venv
. .venv/bin/activate
pip install -e .
# Sửa input_csv trong configs/pipeline.toml trỏ tới PaySim CSV.
paysim-pipeline --config configs/pipeline.toml
```

Có thể dùng config TOML khác. Đường dẫn tương đối được tính từ thư mục `gatv2-pipeline`; output mặc định ở `artifacts/paysim/`.

## Explicitly deferred

Graph construction/window semantics, category encoding/scaling fit chỉ trên train, imbalance strategy, model training, threshold selection, metrics/checkpointing, online graph-state recovery. Các quyết định này phụ thuộc GATv2 input/edge-level contract chưa được chốt trong design. Không ghi model giả hoặc metric giả.

## Resource note

Đang dùng pandas eager DataFrame, dễ đọc/đổi stage nhưng cần RAM đủ cho toàn bộ PaySim (khoảng 6.3M dòng). Khi môi trường bị giới hạn RAM, thay stage load/export bằng xử lý chunk/Parquet mà vẫn giữ nguyên split theo `step` và schema contract.
