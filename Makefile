.PHONY: kafka-up kafka-down topics api producer consumer raw-sink prediction-sink alert-consumer web vet build contracts-test paysim-pipeline pipeline-test benchmark evaluate

GROUND_TRUTH ?= data/ground_truth.csv

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
	python3 -m producer.main --csv "$(CSV)" --ground-truth-path "$(GROUND_TRUTH)"

consumer:
	python3 -m consumer.main

raw-sink:
	python3 -m sink.main --kind raw

prediction-sink:
	python3 -m sink.main --kind prediction

alert-consumer:
	python3 -m sink.main --kind alert

web:
	cd web && npm run dev

vet:
	cd server && go vet ./...

build:
	cd server && go build ./...

contracts-test:
	python3 -m unittest discover -s tests -p 'test_*.py'

paysim-pipeline:
	cd gatv2-pipeline && python -m paysim_pipeline.cli --config configs/pipeline.toml

pipeline-test:
	PYTHONPATH=gatv2-pipeline/src:$$PYTHONPATH python3 -m unittest discover -s gatv2-pipeline/tests -p 'test_*.py'

evaluate:
	test -n "$(GROUND_TRUTH)" -a -n "$(PREDICTIONS)" -a -n "$(THRESHOLD)" || (echo "Usage: make evaluate GROUND_TRUTH=... PREDICTIONS=... THRESHOLD=..." && exit 1)
	python3 -m evaluation.main --ground-truth "$(GROUND_TRUTH)" --predictions "$(PREDICTIONS)" --threshold "$(THRESHOLD)" $(if $(OUTPUT),--output "$(OUTPUT)",)

benchmark:
	test -n "$(RAW_TOPIC)" -a -n "$(SCORED_TOPIC)" || (echo "Usage: make benchmark RAW_TOPIC=bench.raw SCORED_TOPIC=bench.scored" && exit 1)
	python3 -m benchmark.main --raw-topic "$(RAW_TOPIC)" --scored-topic "$(SCORED_TOPIC)"
