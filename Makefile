.PHONY: kafka-up kafka-down gateway inventory email shipping inventory-2 web vet build paysim-pipeline

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

paysim-pipeline:
	cd gatv2-pipeline && python -m paysim_pipeline.cli --config configs/pipeline.toml
