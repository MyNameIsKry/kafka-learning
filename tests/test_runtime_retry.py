"""Regression coverage for in-memory retry and manual offset commits."""

import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import consumer.main as inference_runtime
import sink.main as sink_runtime
from consumer.adapter import ConsumerRecord


class RuntimeRetryTests(unittest.TestCase):
    def run_runtime(self, module, processing_results, commit_results=None):
        records = [ConsumerRecord("transactions.raw", 0, 4, b"{}")]
        consumer = MagicMock()
        if commit_results is not None:
            consumer.commit.side_effect = commit_results
        argv = ["--max-retries", "1"]
        if module is sink_runtime:
            argv += ["--kind", "raw"]
        args = module.build_parser().parse_args(argv)
        with ExitStack() as stack:
            stack.enter_context(patch.object(module, "KafkaConsumer", return_value=consumer))
            stack.enter_context(patch.object(module, "KafkaEventPublisher"))
            if module is sink_runtime:
                stack.enter_context(patch.object(module, "MongoStore"))
            poll = stack.enter_context(patch.object(
                module, "collect_batch", side_effect=[records, KeyboardInterrupt()]
            ))
            process = stack.enter_context(patch.object(
                module, "process_batch", side_effect=processing_results
            ))
            stack.enter_context(patch.object(module.time, "sleep"))
            stack.enter_context(patch.object(module, "LOGGER"))
            result = module.run(args)
        return result, consumer, poll, process

    def test_processing_retry_does_not_rewind_consumer(self):
        for module in (inference_runtime, sink_runtime):
            with self.subTest(runtime=module.__name__):
                result, consumer, poll, process = self.run_runtime(
                    module, [ValueError("temporary storage failure"), (1, 0)]
                )
                self.assertEqual(result, 0)
                consumer.seek.assert_not_called()
                consumer.commit.assert_called_once()
                offsets = consumer.commit.call_args.kwargs["offsets"]
                self.assertEqual(next(iter(offsets.values())).offset, 5)
                self.assertEqual(process.call_count, 2)
                self.assertEqual(poll.call_count, 2)
                self.assertEqual(process.call_args_list[0].args[0], process.call_args_list[1].args[0])

    def test_commit_retry_does_not_rewind_consumer(self):
        for module in (inference_runtime, sink_runtime):
            with self.subTest(runtime=module.__name__):
                result, consumer, _, _ = self.run_runtime(
                    module, [(1, 0), (1, 0)], [RuntimeError("commit failed"), None]
                )
                self.assertEqual(result, 0)
                consumer.seek.assert_not_called()
                self.assertEqual(consumer.commit.call_count, 2)

    def test_exhausted_processing_retries_leave_offsets_uncommitted(self):
        for module in (inference_runtime, sink_runtime):
            with self.subTest(runtime=module.__name__):
                result, consumer, poll, process = self.run_runtime(
                    module, [TypeError("storage failure"), TypeError("storage failure")]
                )
                self.assertEqual(result, 1)
                consumer.commit.assert_not_called()
                consumer.seek.assert_not_called()
                self.assertEqual(process.call_count, 2)
                self.assertEqual(poll.call_count, 1)
                consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
