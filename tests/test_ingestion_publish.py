import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


class _Response:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def load_server():
    grpc = types.ModuleType("grpc")
    grpc.StatusCode = types.SimpleNamespace(INTERNAL="INTERNAL", UNAVAILABLE="UNAVAILABLE")
    grpc.server = lambda *args, **kwargs: None

    grpc_tools = types.ModuleType("grpc_tools")
    grpc_tools.protoc = types.SimpleNamespace(main=lambda *args: 0)

    pb2 = types.ModuleType("image_ingestion_pb2")
    pb2.ImageUploadResponse = _Response
    pb2_grpc = types.ModuleType("image_ingestion_pb2_grpc")
    pb2_grpc.ImageIngestionServiceServicer = object
    pb2_grpc.add_ImageIngestionServiceServicer_to_server = lambda *args: None

    minio = types.ModuleType("minio")
    minio.Minio = object
    minio_error = types.ModuleType("minio.error")
    minio_error.S3Error = type("S3Error", (Exception,), {})

    pika = types.ModuleType("pika")
    pika.BasicProperties = lambda **kwargs: types.SimpleNamespace(**kwargs)
    pika.ConnectionParameters = lambda **kwargs: kwargs
    pika.PlainCredentials = lambda *args: args
    pika.BlockingConnection = None

    stubs = {
        "grpc": grpc,
        "grpc_tools": grpc_tools,
        "image_ingestion_pb2": pb2,
        "image_ingestion_pb2_grpc": pb2_grpc,
        "minio": minio,
        "minio.error": minio_error,
        "pika": pika,
    }
    with mock.patch.dict(sys.modules, stubs):
        path = Path(__file__).parents[1] / "ingestion" / "server.py"
        spec = importlib.util.spec_from_file_location("ingestion_server_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class _Context:
    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


class IngestionPublishTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = load_server()

    def test_unknown_publish_outcome_retains_uploaded_object(self):
        service = self.server.ImageIngestionService.__new__(
            self.server.ImageIngestionService
        )
        service.minio_client = mock.Mock()
        service._publish_event = mock.Mock(side_effect=ConnectionError("confirm lost"))
        request = types.SimpleNamespace(
            filename="frame.jpg",
            mime_type="image/jpeg",
            image_data=b"image-bytes",
            camera_id="CAM-1",
            capture_timestamp="2026-11-10T12:00:00",
            source="test",
        )
        context = _Context()

        response = service.UploadImage(request, context)

        service.minio_client.put_object.assert_called_once()
        service.minio_client.remove_object.assert_not_called()
        self.assertEqual(context.code, self.server.grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(response.status, "FAILED")
        self.assertTrue(response.image_id)
        self.assertTrue(response.s3_uri.startswith("s3://"))
        self.assertIn("retained", response.message)

    def test_explicit_negative_confirm_is_a_publish_failure(self):
        channel = mock.Mock()
        channel.basic_publish.return_value = False
        connection = mock.MagicMock()
        connection.__enter__.return_value.channel.return_value = channel
        service = self.server.ImageIngestionService.__new__(
            self.server.ImageIngestionService
        )
        service.rabbitmq_params = object()
        payload = {"event_id": "event-1"}

        with mock.patch.object(
            self.server.pika, "BlockingConnection", return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, "rejected"):
                service._publish_event(payload, retries=1)

        channel.confirm_delivery.assert_called_once_with()
        self.assertTrue(channel.basic_publish.call_args.kwargs["mandatory"])
        self.assertEqual(
            channel.basic_publish.call_args.kwargs["properties"].message_id,
            "event-1",
        )


if __name__ == "__main__":
    unittest.main()
