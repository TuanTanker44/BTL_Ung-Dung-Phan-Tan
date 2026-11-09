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
    def run_consumer(self, bodies, publish_error=None):
        channel = MagicMock()
        properties = SimpleNamespace(
            headers={"source": "test"}, content_type="application/json", message_id="original-id"
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
        commands.put(("COMPLETE", {}))
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


if __name__ == "__main__":
    unittest.main()
