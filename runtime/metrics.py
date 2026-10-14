"""Process-local Prometheus metrics; no business fields or labels are exported."""

import logging
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

LOGGER = logging.getLogger(__name__)
BUCKETS = (.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60)


class PipelineMetrics:
    def __init__(self, component: str, port: int = 0, host: str = "127.0.0.1") -> None:
        if not 0 <= port <= 65535:
            raise ValueError("metrics port must be between 0 and 65535")
        self.registry = CollectorRegistry()
        self.component = component
        labels = ["component"]
        options = {"registry": self.registry}
        self.published = Counter("pipeline_published_records", "Acknowledged Kafka publishes, including retries", labels + ["topic"], **options)
        self.stored = Counter("pipeline_stored_records", "Acknowledged DB upserts, including retries", labels, **options).labels(component)
        self.committed = Counter("pipeline_committed_records", "Input records in successfully committed batches", labels, **options).labels(component)
        self.failures = Counter("pipeline_failures", "Failed attempts (not unique failed records)", labels + ["stage"], **options)
        self.failed_records = Counter("pipeline_failed_record_attempts", "Records in failed attempts; retries can count the same record again", labels, **options).labels(component)
        self.retries = Counter("pipeline_retries", "Scheduled retries", labels, **options).labels(component)
        self.delivery = Histogram("pipeline_delivery_seconds", "Producer send-to-ack duration", labels, buckets=BUCKETS, **options).labels(component)
        self.inference = Histogram("pipeline_inference_seconds", "Engine predict duration per attempt", labels, buckets=BUCKETS, **options).labels(component)
        self.batch = Histogram("pipeline_batch_seconds", "Processing and commit duration per attempt", labels, buckets=BUCKETS, **options).labels(component)
        self.record_age = Histogram("pipeline_input_to_completion_seconds", "Input Kafka timestamp to successful batch commit; scored sinks start at scored publication, not raw ingress", labels, buckets=BUCKETS, **options).labels(component)
        self.lag = Gauge("pipeline_consumer_lag", "Log end minus committed offset, or earliest retained offset if never committed", labels + ["topic", "partition"], **options)
        self.last_commit = Gauge("pipeline_last_commit_timestamp_seconds", "Last successful batch commit wall clock", labels, **options).labels(component)
        self.lag_sample = Gauge("pipeline_lag_sample_timestamp_seconds", "Last successful lag sample", labels, **options).labels(component)
        self._lag_children = set()
        self._next_lag_sample = 0.0
        self.enabled = port != 0
        self._server = self._thread = None
        if self.enabled:
            self._server, self._thread = start_http_server(port, addr=host, registry=self.registry)

    def failure(self, stage: str) -> None:
        self.failures.labels(self.component, stage).inc()

    def published_record(self, topic: str) -> None:
        self.published.labels(self.component, topic).inc()

    def completed(self, records) -> None:
        now = time.time()
        self.committed.inc(len(records))
        self.last_commit.set(now)
        for record in records:
            timestamp = record.timestamp_ms
            if timestamp is not None and timestamp >= 0 and timestamp <= now * 1000:
                self.record_age.observe(now - timestamp / 1000)

    def sample_lag(self, consumer) -> None:
        if not self.enabled or time.monotonic() < self._next_lag_sample:
            return
        self._next_lag_sample = time.monotonic() + 10
        try:
            partitions = consumer.assignment()
            children = {(self.component, tp.topic, str(tp.partition)) for tp in partitions}
            for child in self._lag_children - children:
                self.lag.remove(*child)
            self._lag_children = children
            if not partitions:
                return
            ends = consumer.end_offsets(partitions)
            starts = consumer.beginning_offsets(partitions)
            for tp in partitions:
                committed = consumer.committed(tp)
                baseline = starts[tp] if committed is None else committed
                self.lag.labels(self.component, tp.topic, str(tp.partition)).set(max(0, ends[tp] - baseline))
            self.lag_sample.set(time.time())
        except Exception:
            # Observability failure must never enter the processing/commit retry path.
            self.failure("lag_sample")
            LOGGER.warning("cannot sample consumer lag", exc_info=True)

    def close(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=2)


def add_metrics_arguments(parser) -> None:
    parser.add_argument("--metrics-port", type=int, default=0, help="Prometheus HTTP port; 0 disables serving")
    parser.add_argument("--metrics-host", default="127.0.0.1")
