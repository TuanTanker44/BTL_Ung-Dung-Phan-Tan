import importlib.util
import json
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location(
    "verify_services", ROOT / "scripts" / "verify_services.py"
)
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)


class ServiceVerificationTests(unittest.TestCase):
    def test_infrastructure_requires_an_alive_spark_worker(self):
        responses = {
            "http://localhost:8123/ping": "Ok.",
            "http://localhost:9000/minio/health/ready": "",
            "http://localhost:15672/api/health/checks/ready-to-serve-clients": json.dumps(
                {"status": "ok"}
            ),
            "http://localhost:8080/json/": json.dumps({"workers": []}),
        }
        with mock.patch.object(VERIFY, "http_get", side_effect=lambda url, *args: responses[url]):
            with self.assertRaisesRegex(RuntimeError, "no ALIVE worker"):
                VERIFY.check_infrastructure()

    def test_infrastructure_checks_airflow_scheduler_and_database(self):
        def response(url, *args):
            if url.endswith("/ping"):
                return "Ok."
            if "ready-to-serve" in url:
                return json.dumps({"status": "ok"})
            if url.endswith("8080/json/"):
                return json.dumps({"workers": [{"state": "ALIVE"}]})
            if url.endswith("8085/health"):
                return json.dumps(
                    {
                        "metadatabase": {"status": "healthy"},
                        "scheduler": {"status": "healthy"},
                    }
                )
            return ""

        with mock.patch.object(VERIFY, "http_get", side_effect=response):
            VERIFY.check_infrastructure()


if __name__ == "__main__":
    unittest.main()
