"""Consumer regression checks; no Spark cluster or RabbitMQ broker required."""
import importlib.util
import json
from pathlib import Path
from queue import Queue
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch


def load_job():
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = MagicMock()
    spec = importlib.util.spec_from_file_location(
        "consumer_test_job", Path(__file__).resolve().parents[1] / "spark/spark_job.py"
    )
    job = importlib.util.module_from_spec(spec)
    with patch.dict("sys.modules", {"pyspark": ModuleType("pyspark"), "pyspark.sql": sql}):
        spec.loader.exec_module(job)
    return job


job = load_job()


def valid_event():
    return {
        "event_id": "event-1", "image_id": "image-1", "bucket": "raw-images",
        "s3_key": "image.jpg", "uploaded_at": "2026-11-09T12:00:00",
        "filename": "image.jpg", "s3_uri": "s3://raw-images/image.jpg",
        "file_size_bytes": 123, "mime_type": "image/jpeg",
    }


class ConsumerTests(unittest.TestCase):
    def run_consumer(self, bodies, publish_error=None, failures=None, headers=None):
        channel = MagicMock()
        properties = SimpleNamespace(
            headers={"source": "test", **(headers or {})},
            content_type="application/json", message_id="original-id"
        )
        channel.basic_get.side_effect = [
            (SimpleNamespace(delivery_tag=i), properties, body)
            for i, body in enumerate(bodies, 1)
        ] + [(None, None, None)]
        channel.basic_publish.side_effect = publish_error
        connection = MagicMock()
        connection.channel.return_value = channel
        pika = ModuleType("pika")
        pika.BlockingConnection = MagicMock(return_value=connection)
        pika.ConnectionParameters = MagicMock()
        pika.PlainCredentials = MagicMock()
        pika.BasicProperties = lambda **kw: SimpleNamespace(**kw)
        inbox, commands, replies = Queue(), Queue(), Queue()
        commands.put(("COMPLETE", failures or {}))
        with patch.dict("sys.modules", {"pika": pika}), patch.object(job, "logger"):
            job.rabbitmq_consumer(inbox, commands, replies)
        return channel, connection, inbox.get_nowait(), replies

    def test_bad_json_does_not_block_good_events(self):
        good = json.dumps(valid_event()).encode()
        channel, connection, (events, error), replies = self.run_consumer([good, b"{bad", good])
        self.assertIsNone(error)
        self.assertEqual(len(events), 2)
        self.assertIsNone(replies.get_nowait())
        publication = channel.basic_publish.call_args.kwargs
        self.assertEqual(publication["routing_key"], job.DLQ_NAME)
        self.assertEqual(publication["body"], b"{bad")
        self.assertTrue(publication["mandatory"])
        self.assertEqual(publication["properties"].message_id, "original-id")
        self.assertEqual(publication["properties"].headers["source"], "test")
        self.assertEqual(publication["properties"].headers["x-error-code"], "INVALID_EVENT")
        actions = [call[0] for call in channel.method_calls]
        self.assertLess(actions.index("basic_publish"), actions.index("basic_ack"))
        self.assertEqual([call.kwargs["delivery_tag"] for call in channel.basic_ack.call_args_list], [2, 1, 3])
        connection.close.assert_called_once()

    def test_invalid_utf8_and_schema_go_to_dlq(self):
        cases = [b"\xff", b"[]", b"{}"]
        for key, value in (
            ("event_id", []), ("file_size_bytes", "123"),
            ("file_size_bytes", True), ("file_size_bytes", -1),
            ("file_size_bytes", 2**64), ("uploaded_at", "bad-date"),
            ("capture_timestamp", None), ("capture_timestamp", "2200-01-01"),
            ("camera_id", []), ("s3_key", ""),
        ):
            event = valid_event()
            event[key] = value
            cases.append(json.dumps(event).encode())
        for body in cases:
            with self.subTest(body=body):
                channel, _, (events, error), _ = self.run_consumer([body])
                self.assertIsNone(error)
                self.assertEqual(events, [])
                channel.basic_publish.assert_called_once()
                channel.basic_ack.assert_called_once_with(delivery_tag=1, multiple=False)

    def test_publish_failure_leaves_originals_unacknowledged(self):
        good = json.dumps(valid_event()).encode()
        channel, connection, (events, error), _ = self.run_consumer(
            [good, b"broken"], RuntimeError("broker confirmation failed")
        )
        self.assertIsNone(events)
        self.assertIsInstance(error, RuntimeError)
        channel.basic_ack.assert_not_called()
        connection.close.assert_called_once()

    def test_batch_read_limit_includes_invalid_messages(self):
        channel, _, (events, error), _ = self.run_consumer([b"broken"] * 60)
        self.assertIsNone(error)
        self.assertEqual(events, [])
        self.assertEqual(channel.basic_get.call_count, 50)
        self.assertEqual(channel.basic_publish.call_count, 50)

    def test_valid_optional_metadata_is_accepted(self):
        event = valid_event()
        event.update(camera_id="CAM-HN-001", capture_timestamp="2026-11-09T12:00:00+07:00")
        channel, _, (events, error), _ = self.run_consumer([json.dumps(event).encode()])
        self.assertIsNone(error)
        self.assertEqual(events, [event])
        channel.basic_publish.assert_not_called()
        channel.basic_ack.assert_called_once_with(delivery_tag=1, multiple=False)

    def test_permanent_image_errors_go_directly_to_dlq(self):
        bad = valid_event()
        good = {**valid_event(), "event_id": "good-event"}
        bodies = [json.dumps(e).encode() for e in [bad, good]]
        for code in ("EMPTY_IMAGE", "INVALID_IMAGE", "IMAGE_NOT_FOUND"):
            with self.subTest(code=code):
                failure = {"event-1": {"code": code, "message": "image failed"}}
                channel, _, (events, error), replies = self.run_consumer(
                    bodies, failures=failure,
                )
                self.assertIsNone(error)
                self.assertEqual(events, [bad, good])
                self.assertIsNone(replies.get_nowait())
                channel.confirm_delivery.assert_called_once()
                publication = channel.basic_publish.call_args.kwargs
                self.assertEqual(publication["routing_key"], job.DLQ_NAME)
                self.assertEqual(publication["body"], bodies[0])
                self.assertTrue(publication["mandatory"])
                props = publication["properties"]
                self.assertEqual(props.delivery_mode, 2)
                self.assertEqual(props.message_id, "event-1")
                self.assertEqual(props.headers["x-error-code"], code)
                self.assertEqual(props.headers["x-last-error"], "image failed")
                self.assertEqual(props.headers["x-processing-attempt"], 1)
                self.assertEqual(props.headers["source"], "test")
                actions = [call[0] for call in channel.method_calls]
                self.assertLess(actions.index("basic_publish"), actions.index("basic_ack"))
                self.assertEqual(
                    [call.kwargs["delivery_tag"] for call in channel.basic_ack.call_args_list],
                    [1, 2],
                )

    def test_image_dlq_publish_failure_does_not_ack_original(self):
        body = json.dumps(valid_event()).encode()
        channel, connection, (_, error), replies = self.run_consumer(
            [body], publish_error=RuntimeError("broker confirmation failed"),
            failures={"event-1": {"code": "INVALID_IMAGE", "message": "bad image"}},
        )
        self.assertIsNone(error)  # Fetch succeeded; completion failed afterwards.
        self.assertIsInstance(replies.get_nowait(), RuntimeError)
        channel.basic_ack.assert_not_called()
        connection.close.assert_called_once()

    def test_non_permanent_failure_uses_code_not_message_to_retry(self):
        body = json.dumps(valid_event()).encode()
        for previous, target in [(0, job.QUEUE_NAME), (2, job.DLQ_NAME)]:
            with self.subTest(previous=previous):
                channel, _, _, replies = self.run_consumer(
                    [body], headers={"x-processing-attempt": previous},
                    failures={"event-1": {
                        "code": "TEMPORARY_ERROR", "message": "text contains NoSuchKey",
                    }},
                )
                self.assertIsNone(replies.get_nowait())
                self.assertEqual(channel.basic_publish.call_args.kwargs["routing_key"], target)
                channel.basic_ack.assert_called_once()


if __name__ == "__main__":
    unittest.main()
