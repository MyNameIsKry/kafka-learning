import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, create_autospec, patch

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from benchmark.main import latency_summary, percentile
from consumer.adapter import ConsumerRecord
from consumer.publisher import KafkaEventPublisher
from runtime.kafka import collect_batch
from runtime.metrics import PipelineMetrics


class MetricsTests(unittest.TestCase):
    def test_publish_counter_only_advances_after_ack(self):
        metrics = PipelineMetrics("inference")
        with patch("consumer.publisher.KafkaProducer") as factory:
            publisher = KafkaEventPublisher(["unused"], metrics=metrics)
            factory.return_value.send.return_value.get.side_effect = RuntimeError("no ack")
            with self.assertRaises(RuntimeError):
                publisher._publish("scored", "x", {})
            labels = {"component": "inference", "topic": "scored"}
            self.assertIsNone(metrics.registry.get_sample_value("pipeline_published_records_total", labels))
            factory.return_value.send.return_value.get.side_effect = None
            publisher._publish("scored", "x", {})
            self.assertEqual(metrics.registry.get_sample_value("pipeline_published_records_total", labels), 1)

    def test_counters_are_acknowledgement_based_and_registries_isolated(self):
        metrics = PipelineMetrics("producer")
        other = PipelineMetrics("producer")
        metrics.published_record("transactions.raw")
        labels = {"component": "producer", "topic": "transactions.raw"}
        self.assertEqual(metrics.registry.get_sample_value("pipeline_published_records_total", labels), 1)
        self.assertIsNone(other.registry.get_sample_value("pipeline_published_records_total", labels))

    def test_completion_uses_kafka_time_not_paysim_step(self):
        metrics = PipelineMetrics("inference")
        records = [ConsumerRecord("raw", 0, 0, b"{}", 100_000),
                   ConsumerRecord("raw", 0, 1, b"{}"),
                   ConsumerRecord("raw", 0, 2, b"{}", 300_000)]
        with patch("runtime.metrics.time.time", return_value=101):
            metrics.completed(records)
        labels = {"component": "inference"}
        self.assertEqual(metrics.registry.get_sample_value("pipeline_committed_records_total", labels), 3)
        self.assertEqual(metrics.registry.get_sample_value("pipeline_input_to_completion_seconds_count", labels), 1)
        self.assertEqual(metrics.registry.get_sample_value("pipeline_input_to_completion_seconds_sum", labels), 1)

    def test_lag_uses_committed_offset_not_read_position(self):
        metrics = PipelineMetrics("inference")
        metrics.enabled = True
        tp = TopicPartition("raw", 0)
        consumer = create_autospec(KafkaConsumer, instance=True)
        consumer.assignment.return_value = {tp}
        consumer.end_offsets.return_value = {tp: 100}
        consumer.beginning_offsets.return_value = {tp: 10}
        consumer.committed.return_value = 20
        metrics.sample_lag(consumer)
        labels = {"component": "inference", "topic": "raw", "partition": "0"}
        self.assertEqual(metrics.registry.get_sample_value("pipeline_consumer_lag", labels), 80)
        consumer.position.assert_not_called()
        metrics._next_lag_sample = 0
        consumer.committed.return_value = None
        metrics.sample_lag(consumer)
        self.assertEqual(metrics.registry.get_sample_value("pipeline_consumer_lag", labels), 90)

    def test_lag_sampling_failure_does_not_propagate_or_commit(self):
        metrics = PipelineMetrics("inference")
        metrics.enabled = True
        consumer = MagicMock()
        consumer.assignment.side_effect = RuntimeError("Kafka unavailable")
        with patch("runtime.metrics.LOGGER"):
            metrics.sample_lag(consumer)
        consumer.commit.assert_not_called()
        self.assertEqual(metrics.registry.get_sample_value("pipeline_failures_total", {
            "component": "inference", "stage": "lag_sample"}), 1)

    def test_poll_retains_kafka_timestamp_without_changing_payload(self):
        consumer = MagicMock()
        consumer.poll.return_value = {TopicPartition("raw", 0): [SimpleNamespace(offset=3, value=b"{}", timestamp=123)]}
        batch = collect_batch(consumer, 1, 0)
        self.assertEqual(batch[0].timestamp_ms, 123)
        self.assertEqual(batch[0].value, b"{}")

    def test_percentiles_and_missing_samples(self):
        self.assertIsNone(percentile([], .95))
        self.assertEqual(percentile([3, 1, 2], .5), 2)
        self.assertEqual(latency_summary([1, 2, 3])["p99"], 3)
        self.assertEqual(latency_summary([]), {"samples": 0, "p50": None, "p95": None, "p99": None})


if __name__ == "__main__":
    unittest.main()
