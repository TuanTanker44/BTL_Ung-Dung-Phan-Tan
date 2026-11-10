"""Image isolation checks using real Pillow decoding and fake external services."""
from contextlib import contextmanager
import io
from queue import Queue
from types import ModuleType
import unittest
from unittest.mock import MagicMock, patch

from PIL import Image

from test_rabbitmq_consumer import job, valid_event


class FakeS3Error(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def image_bytes(format="PNG", mode="RGB"):
    output = io.BytesIO()
    with Image.new(mode, (32, 32), color=0) as image:
        image.save(output, format=format)
    return output.getvalue()


def event_row(event_id):
    return (event_id, f"image-{event_id}", "raw-images", f"{event_id}.jpg",
            "2026-11-10T12:00:00", f"{event_id}.jpg", "CAM-HN-001",
            "2026-11-10T12:00:00")


@contextmanager
def fake_services(objects):
    client = MagicMock()
    responses = {}

    def get_object(bucket, key):
        data = objects[key]
        if isinstance(data, Exception):
            raise data
        response = MagicMock()
        if callable(data):
            response.read.side_effect = data
        else:
            response.read.return_value = data
        responses[key] = response
        return response

    client.get_object.side_effect = get_object
    minio = ModuleType("minio")
    minio.Minio = MagicMock(return_value=client)
    errors = ModuleType("minio.error")
    errors.S3Error = FakeS3Error
    ultralytics = ModuleType("ultralytics")
    model = MagicMock(return_value=[])
    ultralytics.YOLO = MagicMock(return_value=model)
    with patch.dict("sys.modules", {
        "minio": minio, "minio.error": errors, "ultralytics": ultralytics,
    }), patch.object(job.Path, "is_file", return_value=True), patch.object(job, "logger"):
        yield client, responses, model


class ImageProcessingTests(unittest.TestCase):
    def test_bad_images_do_not_block_good_images_in_either_mode(self):
        good = image_bytes()
        # Keep JPEG headers intact: open() succeeds but full decoding fails.
        truncated = image_bytes("JPEG")[:-10]
        with Image.open(io.BytesIO(truncated)):
            pass
        objects = {
            "good-before.jpg": good, "empty.jpg": b"", "invalid.jpg": b"not an image",
            "truncated.jpg": truncated, "missing.jpg": FakeS3Error("NoSuchKey"),
            "good-after.jpg": good,
        }
        for mode in ("mock", "yolo"):
            with self.subTest(mode=mode), fake_services(objects) as (_, responses, model):
                outcomes = list(job.process_partition(
                    iter(event_row(key[:-4]) for key in objects), mode,
                ))
                self.assertEqual(len(outcomes), 6)
                for i in (0, 5):
                    event_id, rows, reason = outcomes[i]
                    self.assertIsNone(reason)
                    self.assertEqual(len(rows), 5)
                    self.assertEqual({row[0] for row in rows}, {event_id})
                    self.assertEqual({row[3] for row in rows}, {1, 2, 3, 4, 5})
                for i, code in enumerate(
                    ["EMPTY_IMAGE", "INVALID_IMAGE", "INVALID_IMAGE", "IMAGE_NOT_FOUND"], 1,
                ):
                    self.assertEqual(outcomes[i][1], [])
                    self.assertEqual(outcomes[i][2]["code"], code)
                for response in responses.values():
                    response.close.assert_called_once()
                    response.release_conn.assert_called_once()
                self.assertEqual(model.call_count, 2 if mode == "yolo" else 0)

    def test_valid_image_is_converted_to_rgb_before_yolo(self):
        with fake_services({"good.jpg": image_bytes(mode="L")}) as (_, _, model):
            def infer(image):
                self.assertEqual(image.mode, "RGB")
                self.assertEqual(image.size, (32, 32))
                return []
            model.side_effect = infer
            outcomes = list(job.process_partition(iter([event_row("good")]), "yolo"))
        self.assertIsNone(outcomes[0][2])

    def test_storage_errors_other_than_missing_object_propagate(self):
        for error in [FakeS3Error("AccessDenied"), FakeS3Error("InternalError"),
                      OSError("MinIO connection interrupted")]:
            with self.subTest(error=error), fake_services({"bad.jpg": error}):
                with self.assertRaises(type(error)):
                    list(job.process_partition(iter([event_row("bad")]), "mock"))

    def test_read_error_propagates_and_releases_connection(self):
        def fail_read():
            raise OSError("network read failed")
        with fake_services({"bad.jpg": fail_read}) as (_, responses, _):
            with self.assertRaisesRegex(OSError, "network read failed"):
                list(job.process_partition(iter([event_row("bad")]), "mock"))
            responses["bad.jpg"].close.assert_called_once()
            responses["bad.jpg"].release_conn.assert_called_once()

    def test_yolo_errors_are_not_classified_as_bad_images(self):
        for error in [ValueError("model configuration error"), OSError("model runtime error")]:
            with self.subTest(error=error), fake_services({"good.jpg": image_bytes()}) as (_, _, model):
                model.side_effect = error
                with self.assertRaises(type(error)):
                    list(job.process_partition(iter([event_row("good")]), "yolo"))
                image = model.call_args.args[0]
                with self.assertRaises(ValueError):
                    image.getpixel((0, 0))  # The decoded image was closed on failure.

    def test_missing_model_remains_a_task_error(self):
        with fake_services({}), patch.object(job.Path, "is_file", return_value=False):
            with self.assertRaises(FileNotFoundError):
                list(job.process_partition(iter([event_row("good")]), "yolo"))

    def test_missing_pillow_remains_a_dependency_error(self):
        with fake_services({"good.jpg": image_bytes()}), patch.dict("sys.modules", {"PIL": None}):
            with self.assertRaises(ImportError):
                list(job.process_partition(iter([event_row("good")]), "mock"))

    def test_driver_saves_only_good_events_and_reports_failures(self):
        events = [{**valid_event(), "event_id": name} for name in ("good", "bad")]
        rows = [("good", "2026-11-10T12:00:00", "CAM-HN-001", kind, 0,
                 "Low", "image-1", 1) for kind in range(1, 6)]
        failure = {"code": "INVALID_IMAGE", "message": "cannot decode"}
        inbox, commands, replies = Queue(), Queue(), Queue()
        inbox.put((events, None))
        replies.put(None)
        spark = MagicMock()
        spark.sparkContext.parallelize.return_value.mapPartitions.return_value.collect.return_value = [
            ("good", rows, None), ("bad", [], failure),
        ]
        with patch.dict("os.environ", {"DETECTION_MODE": "mock"}), \
             patch.object(job, "Queue", side_effect=[inbox, commands, replies]), \
             patch.object(job, "Thread"), patch.object(job, "logger"), \
             patch.object(job.SparkSession.builder.appName.return_value.master.return_value,
                          "getOrCreate", return_value=spark), \
             patch.object(job, "get_completed_event_ids", side_effect=[set(), {"good"}]) as verify, \
             patch.object(job, "save_to_clickhouse") as save:
            job.main()
        save.assert_called_once_with([events[0]], rows, "mock")
        self.assertEqual(verify.call_args.args[0], {"good"})
        self.assertEqual(commands.get_nowait(), ("COMPLETE", {"bad": failure}))
        spark.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
