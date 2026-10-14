#!/usr/bin/env bash
# Non-destructive integration smoke: unique streams/topics/databases per run.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN="kurrent-smoke-$(date +%s)-$$"
DIR="${TMPDIR:-/tmp/opencode}/$RUN"
mkdir -p "$DIR"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
cd "$ROOT/server"
for command in producer event-relay inference sink; do
  go build -o "$DIR/$command" "./cmd/$command"
done
cd "$ROOT"
for kind in raw scored anomalies dlq; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "$RUN.$kind" --partitions 1 --replication-factor 1
done
cat > "$DIR/input.csv" <<'CSV'
step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
2,TRANSFER,20,C2,200,180,C3,0,20,1,0
1,PAYMENT,10,C1,100,90,M1,0,10,0,0
CSV
PREFIX="$RUN-event-"
DB="$RUN"
start() {
  "$@" >"$DIR/process-${#PIDS[@]}.log" 2>&1 &
  PIDS+=("$!")
}
count() {
  docker compose exec -T mongo mongosh --quiet --eval \
    "db.getSiblingDB('$1').$2.countDocuments()" | tail -n 1
}
wait_count() {
  for ((attempt=0;attempt<60;attempt++)); do
    if [[ $(count "$1" "$2") == "$3" ]]; then return; fi
    sleep 1
  done
  cat "$DIR"/*.log
  echo "Timeout waiting for $1.$2=$3" >&2; exit 1
}
# Append before relay. Kafka/Mongo must not receive producer events yet.
"$DIR/producer" --stream-prefix "$PREFIX" --topic "$RUN.raw" --csv "$DIR/input.csv" \
  --dataset-version "$RUN" --sort-chunk-size 1 --ground-truth-path "$DIR/labels.csv"
[[ $(count "$DB" transactions) == 0 ]]
END=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic "$RUN.raw" --time -1)
[[ "$END" == "$RUN.raw:0:0" ]]
# Simulate an unreachable Kafka broker without stopping the user's real broker.
# The same subscription must still deliver these unacknowledged events later.
start "$DIR/event-relay" --stream-prefix "$PREFIX" --group "$RUN-relay" --bootstrap-servers localhost:1
FAILED_RELAY_PID=${PIDS[-1]}
sleep 3
kill "$FAILED_RELAY_PID"; wait "$FAILED_RELAY_PID" || true
start "$DIR/inference" --stream-prefix "$PREFIX" --topic "$RUN.raw" --group-id "$RUN-inference" \
  --scored-topic "$RUN.scored" --anomalies-topic "$RUN.anomalies" --dlq-topic "$RUN.dlq" \
  --model-version "$RUN-model" --mock-score 0.95
for kind in raw prediction alert; do
  case "$kind" in raw) topic=raw;; prediction) topic=scored;; alert) topic=anomalies;; esac
  start "$DIR/sink" --stream-prefix "$PREFIX" --kind "$kind" --topic "$RUN.$topic" \
    --group-id "$RUN-$kind" --mongo-db "$DB" --dlq-topic "$RUN.dlq"
done
start "$DIR/event-relay" --stream-prefix "$PREFIX" --group "$RUN-relay"
RELAY_PID=${PIDS[-1]}
wait_count "$DB" transactions 2
wait_count "$DB" transaction_predictions 2
wait_count "$DB" transaction_alerts 2
# Restart relay with the same durable checkpoint, and repeat producer append.
kill "$RELAY_PID"; wait "$RELAY_PID" || true
"$DIR/producer" --stream-prefix "$PREFIX" --topic "$RUN.raw" --csv "$DIR/input.csv" \
  --dataset-version "$RUN" --ground-truth-path "$DIR/labels.csv"
start "$DIR/event-relay" --stream-prefix "$PREFIX" --group "$RUN-relay"
RELAY_PID=${PIDS[-1]}
wait_count "$DB" transaction_predictions 2
# Rebuild a new projection, replaying history with a new relay subscription.
kill "$RELAY_PID"; wait "$RELAY_PID" || true
start "$DIR/event-relay" --stream-prefix "$PREFIX" --group "$RUN-replay"
for kind in raw prediction alert; do
  case "$kind" in raw) topic=raw;; prediction) topic=scored;; alert) topic=anomalies;; esac
  start "$DIR/sink" --stream-prefix "$PREFIX" --kind "$kind" --topic "$RUN.$topic" \
    --group-id "$RUN-rebuild-$kind" --mongo-db "$DB-rebuilt" --dlq-topic "$RUN.dlq"
done
wait_count "$DB-rebuilt" transactions 2
wait_count "$DB-rebuilt" transaction_predictions 2
wait_count "$DB-rebuilt" transaction_alerts 2
echo "PASS: KurrentDB → Kafka → inference → KurrentDB → Kafka → projections"
echo "PASS: Kafka unavailable recovery, append retry, relay restart, historical replay and projection rebuild"
echo "Isolated test resources retained: $RUN; logs/labels: $DIR"
