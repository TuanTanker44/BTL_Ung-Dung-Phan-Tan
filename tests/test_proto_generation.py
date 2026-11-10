import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


class ProtobufGenerationTests(unittest.TestCase):
    def test_client_prepares_output_and_refreshes_import_cache(self):
        source = (ROOT / "ingestion" / "client.py").read_text(encoding="utf-8")
        self.assertIn("os.makedirs(GEN_DIR, exist_ok=True)", source)
        self.assertIn("if result != 0:", source)
        self.assertIn("importlib.invalidate_caches()", source)

    def test_server_checks_protoc_result(self):
        source = (ROOT / "ingestion" / "server.py").read_text(encoding="utf-8")
        self.assertIn("result = protoc.main", source)
        self.assertIn("Protobuf compilation failed with exit code", source)


if __name__ == "__main__":
    unittest.main()
