NIX ?= nix
PAYSIM_DATA_DIR ?= gatv2-pipeline/data
PAYSIM_CSV ?= $(PAYSIM_DATA_DIR)/PS_20174392719_1491204439457_log.csv
PIPELINE_CONFIG ?= gatv2-pipeline/configs/pipeline.toml
PIPELINE_OUTPUT_DIR ?= gatv2-pipeline/artifacts/paysim
TRAIN_MODULE ?= paysim_pipeline.train
TRAIN_CONFIG ?= gatv2-pipeline/configs/train.toml
GROUND_TRUTH ?= data/ground_truth.csv
BOOTSTRAP_SERVERS ?= localhost:9092
GROUP_ID ?=
MODEL_VERSION ?= mock-v1
MOCK_SCORE ?= 0.1
MOCK_THRESHOLD ?= 0.8
MAX_RETRIES ?= 3
DATASET_VERSION ?=
MAX_EVENTS ?= 0
METRICS_PORT ?= 0

.PHONY: help kafka-up kafka-down topics api producer consumer raw-sink prediction-sink alert-consumer web vet build contracts-test go-test python-legacy-test paysim-pipeline pipeline-test benchmark evaluate shell dataset-download dataset-check prepare test train

help:
	@printf '%s\n' \
	  'Kafka/Python pipeline:' \
	  '  make kafka-up             Start Kafka, Kafka UI and MongoDB' \
	  '  make api|web              Run API or dashboard' \
	  '  make producer CSV=...     Replay PaySim into Kafka (Go)' \
	  '  make consumer             Run mock inference consumer (Go)' \
	  '  make raw-sink|prediction-sink|alert-consumer' \
	  '  make contracts-test       Run Go runtime and Python regression tests' \
	  '  make benchmark RAW_TOPIC=... SCORED_TOPIC=...' \
	  '  make evaluate GROUND_TRUTH=... PREDICTIONS=... THRESHOLD=...' \
	  '' \
	  'Offline PaySim pipeline:' \
	  '  make dataset-download     Fetch PaySim with the Nix shell' \
	  '  make prepare              Validate, split and export artifacts' \
	  '  make pipeline-test        Run parity tests' \
	  '  make train                Prepare data, then run configured trainer'

kafka-up:
	docker compose run --rm --no-deps kafka-volume-init
	docker compose up -d kafka kafka-ui mongo
	$(MAKE) topics

kafka-down:
	docker compose down

topics:
	docker compose run --rm --no-deps kafka-init

api:
	cd server && go run ./cmd/api

producer:
	test -n "$(CSV)" || (echo "Usage: make producer CSV=path/to/PS_*.csv" && exit 1)
	cd server && go run ./cmd/producer --bootstrap-servers "$(BOOTSTRAP_SERVERS)" --csv "$(abspath $(CSV))" --ground-truth-path "$(abspath $(GROUND_TRUTH))" --dataset-version "$(DATASET_VERSION)" --max-events "$(MAX_EVENTS)" --metrics-port "$(METRICS_PORT)"

consumer:
	cd server && go run ./cmd/inference --bootstrap-servers "$(BOOTSTRAP_SERVERS)" --group-id "$(if $(GROUP_ID),$(GROUP_ID),gatv2-inference-group)" --mock-score "$(MOCK_SCORE)" --mock-threshold "$(MOCK_THRESHOLD)" --model-version "$(MODEL_VERSION)" --max-retries "$(MAX_RETRIES)" --metrics-port "$(METRICS_PORT)"

raw-sink:
	cd server && go run ./cmd/sink --kind raw --bootstrap-servers "$(BOOTSTRAP_SERVERS)" --group-id "$(if $(GROUP_ID),$(GROUP_ID),mongodb-raw-sink-group)" --max-retries "$(MAX_RETRIES)" --metrics-port "$(METRICS_PORT)"

prediction-sink:
	cd server && go run ./cmd/sink --kind prediction --bootstrap-servers "$(BOOTSTRAP_SERVERS)" --group-id "$(if $(GROUP_ID),$(GROUP_ID),mongodb-sink-group)" --max-retries "$(MAX_RETRIES)" --metrics-port "$(METRICS_PORT)"

alert-consumer:
	cd server && go run ./cmd/sink --kind alert --bootstrap-servers "$(BOOTSTRAP_SERVERS)" --group-id "$(if $(GROUP_ID),$(GROUP_ID),alert-group)" --max-retries "$(MAX_RETRIES)" --metrics-port "$(METRICS_PORT)"

web:
	cd web && npm run dev

vet:
	cd server && go vet ./...

build:
	cd server && go build ./...

contracts-test:
	$(MAKE) go-test
	$(MAKE) python-legacy-test

go-test:
	cd server && go test ./...

python-legacy-test:
	python3 -m unittest discover -s tests -p 'test_*.py'

pipeline-test:
	PYTHONPATH=gatv2-pipeline/src:$$PYTHONPATH python3 -m unittest discover -s gatv2-pipeline/tests -p 'test_*.py'

benchmark:
	test -n "$(RAW_TOPIC)" -a -n "$(SCORED_TOPIC)" || (echo "Usage: make benchmark RAW_TOPIC=bench.raw SCORED_TOPIC=bench.scored" && exit 1)
	python3 -m benchmark.main --raw-topic "$(RAW_TOPIC)" --scored-topic "$(SCORED_TOPIC)"

evaluate:
	test -n "$(GROUND_TRUTH)" -a -n "$(PREDICTIONS)" -a -n "$(THRESHOLD)" || (echo "Usage: make evaluate GROUND_TRUTH=... PREDICTIONS=... THRESHOLD=..." && exit 1)
	python3 -m evaluation.main --ground-truth "$(GROUND_TRUTH)" --predictions "$(PREDICTIONS)" --threshold "$(THRESHOLD)" $(if $(OUTPUT),--output "$(OUTPUT)",)

shell:
	$(NIX) develop .#paysim

dataset-download:
	$(NIX) develop .#paysim --command python gatv2-pipeline/scripts/download_dataset.py --output-dir $(abspath $(PAYSIM_DATA_DIR))

dataset-check:
	test -f $(PAYSIM_CSV) || (echo "Không tìm thấy $(PAYSIM_CSV); chạy 'make dataset-download' hoặc đặt PAYSIM_CSV đúng đường dẫn." >&2; exit 1)

prepare: dataset-check
	$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src python -m paysim_pipeline.cli --config $(PIPELINE_CONFIG) --input-csv $(abspath $(PAYSIM_CSV)) --output-dir $(abspath $(PIPELINE_OUTPUT_DIR))

paysim-pipeline: prepare

test:
	$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src:gatv2-pipeline/scripts python -m unittest discover -s gatv2-pipeline/tests -v

train: prepare
	@$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src python -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$(TRAIN_MODULE)') else 1)" || (echo "Chưa có module train '$(TRAIN_MODULE)'. Bổ sung model và $(TRAIN_CONFIG), rồi chạy lại make train." >&2; exit 2)
	$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src python -m $(TRAIN_MODULE) --config $(TRAIN_CONFIG)
