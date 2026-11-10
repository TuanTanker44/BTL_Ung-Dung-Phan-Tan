import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


class DependencyContractTests(unittest.TestCase):
    def test_driver_and_worker_pin_the_same_spark_and_library_ranges(self):
        worker = (ROOT / "spark" / "requirements.txt").read_text(encoding="utf-8")
        airflow = (ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")
        spark_dockerfile = (ROOT / "spark" / "Dockerfile").read_text(
            encoding="utf-8"
        )
        for requirement in (
            "minio>=7.2,<8",
            "pika>=1.3,<2",
            "clickhouse-connect>=0.7,<1",
            "Pillow>=10,<12",
        ):
            self.assertIn(requirement, worker)
            self.assertIn(f'"{requirement}"', airflow)
        self.assertIn('"pyspark==3.4.1"', airflow)
        self.assertIn("FROM bitnamilegacy/spark:3.4.1", spark_dockerfile)

    def test_images_fail_the_build_when_required_imports_are_missing(self):
        for dockerfile in (ROOT / "spark" / "Dockerfile", ROOT / "airflow" / "Dockerfile"):
            source = dockerfile.read_text(encoding="utf-8")
            for module in ("clickhouse_connect", "minio", "pika", "PIL", "pyspark"):
                self.assertIn(module, source, f"{module} missing from {dockerfile}")

    def test_runtime_verifier_checks_driver_worker_parity(self):
        verifier = (ROOT / "scripts" / "verify_dependencies.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("driver/worker Python mismatch", verifier)
        self.assertIn("expected PySpark 3.4.1", verifier)
        self.assertIn("image_pipeline_spark_worker", verifier)


if __name__ == "__main__":
    unittest.main()
