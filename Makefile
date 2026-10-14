NIX ?= nix
PAYSIM_DATA_DIR ?= gatv2-pipeline/data
PAYSIM_CSV ?= $(PAYSIM_DATA_DIR)/PS_20174392719_1491204439457_log.csv
PIPELINE_CONFIG ?= gatv2-pipeline/configs/pipeline.toml
PIPELINE_OUTPUT_DIR ?= gatv2-pipeline/artifacts/paysim
TRAIN_MODULE ?= paysim_pipeline.train
TRAIN_CONFIG ?= gatv2-pipeline/configs/train.toml

.PHONY: help kafka-up kafka-down gateway inventory email shipping inventory-2 web vet build \
	shell dataset-download dataset-check prepare paysim-pipeline train test

help:
	@printf '%s\n' \
	  'Kafka demo:' \
	  '  make kafka-up             Start Kafka and MongoDB containers' \
	  '  make kafka-down           Stop containers' \
	  '  make gateway              Run the API gateway' \
	  '  make inventory|email|shipping  Run a demo service' \
	  '  make inventory-2          Run a second inventory instance' \
	  '  make web                  Run the frontend dev server' \
	  '  make vet|build            Check or build Go services' \
	  '' \
	  'PaySim pipeline:' \
	  '  make shell               Enter the Nix PaySim development shell' \
	  '  make dataset-download    Fetch and extract PaySim from Kaggle' \
	  '  make dataset-check       Check that the CSV exists' \
	  '  make prepare             Clean, split, and export train artifacts' \
	  '  make test                Run PaySim pipeline unit tests' \
	  '  make train               Prepare data, then run the configured trainer' \
	  '' \
	  'Overrides: PAYSIM_DATA_DIR, PAYSIM_CSV, PIPELINE_CONFIG,' \
	  '           PIPELINE_OUTPUT_DIR, TRAIN_MODULE, TRAIN_CONFIG'

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down

gateway:
	cd server && go run ./cmd/gateway

inventory:
	cd server && go run ./cmd/inventory

email:
	cd server && WORK_MS=700 go run ./cmd/email

shipping:
	cd server && WORK_MS=400 go run ./cmd/shipping

inventory-2:
	cd server && ADDR=:8084 INSTANCE=inventory-2 go run ./cmd/inventory

web:
	cd web && npm run dev

vet:
	cd server && go vet ./...

build:
	cd server && go build ./...

shell:
	$(NIX) develop .#paysim

dataset-download:
	$(NIX) develop .#paysim --command python gatv2-pipeline/scripts/download_dataset.py \
		--output-dir $(abspath $(PAYSIM_DATA_DIR))

dataset-check:
	test -f $(PAYSIM_CSV) || (echo "Không tìm thấy $(PAYSIM_CSV); chạy 'make dataset-download' hoặc đặt PAYSIM_CSV đúng đường dẫn." >&2; exit 1)

prepare: dataset-check
	$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src python -m paysim_pipeline.cli \
		--config $(PIPELINE_CONFIG) \
		--input-csv $(abspath $(PAYSIM_CSV)) \
		--output-dir $(abspath $(PIPELINE_OUTPUT_DIR))

paysim-pipeline: prepare

test:
	$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src:gatv2-pipeline/scripts python -m unittest discover -s gatv2-pipeline/tests -v

train: prepare
	@$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src python -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$(TRAIN_MODULE)') else 1)" || (echo "Chưa có module train '$(TRAIN_MODULE)'. Bổ sung model và $(TRAIN_CONFIG), rồi chạy lại make train." >&2; exit 2)
	$(NIX) develop .#paysim --command env PYTHONPATH=gatv2-pipeline/src python -m $(TRAIN_MODULE) --config $(TRAIN_CONFIG)
