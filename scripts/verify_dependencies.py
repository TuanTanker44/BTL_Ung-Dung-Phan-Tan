"""Verify host imports and Python/package parity across Spark driver/workers.

Run from the repository root after Docker Compose is healthy:
    python scripts/verify_dependencies.py
The command exits non-zero on every missing import or driver/worker mismatch.
"""

import importlib
import importlib.metadata
import json
import shutil
import subprocess
import sys


HOST_MODULES = {
    "grpc": "grpcio",
    "minio": "minio",
    "pika": "pika",
    "clickhouse_connect": "clickhouse-connect",
    "streamlit": "streamlit",
    "plotly": "plotly",
    "pandas": "pandas",
    "PIL": "Pillow",
}
CONTAINER_MODULES = {
    "pyspark": "pyspark",
    "minio": "minio",
    "pika": "pika",
    "clickhouse_connect": "clickhouse-connect",
    "PIL": "Pillow",
}


def inspect_current_python(modules):
    versions = {"python": ".".join(map(str, sys.version_info[:3]))}
    for module_name, distribution in modules.items():
        importlib.import_module(module_name)
        versions[distribution] = importlib.metadata.version(distribution)
    return versions


def inspect_container(container, python_path, modules):
    probe = (
        "import importlib,importlib.metadata,json,sys;"
        f"mods={modules!r};"
        "[importlib.import_module(m) for m in mods];"
        "print(json.dumps(dict([('python','.'.join(map(str,sys.version_info[:3])))]"
        "+[(d,importlib.metadata.version(d)) for m,d in mods.items()])))"
    )
    command = ["docker", "exec", container, python_path, "-c", probe]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(result.stdout.strip().splitlines()[-1])


def minor(version):
    return ".".join(version.split(".")[:2])


def main():
    failures = []
    report = {}
    try:
        report["host"] = inspect_current_python(HOST_MODULES)
    except Exception as exc:
        failures.append(f"host imports: {exc}")

    if not shutil.which("docker"):
        failures.append("Docker CLI is not installed or not on PATH")
    else:
        targets = {
            "spark-master": (
                "image_pipeline_spark_master",
                "/opt/bitnami/python/bin/python",
                CONTAINER_MODULES,
            ),
            "spark-worker": (
                "image_pipeline_spark_worker",
                "/opt/bitnami/python/bin/python",
                CONTAINER_MODULES,
            ),
            "airflow-driver": (
                "image_pipeline_airflow_scheduler",
                "/home/airflow/spark-venv/bin/python",
                CONTAINER_MODULES,
            ),
        }
        for label, (container, python_path, modules) in targets.items():
            try:
                report[label] = inspect_container(container, python_path, modules)
            except Exception as exc:
                failures.append(f"{label} imports: {exc}")

        runtimes = [report.get(name) for name in targets]
        if all(runtimes):
            python_minors = {minor(runtime["python"]) for runtime in runtimes}
            pyspark_versions = {runtime["pyspark"] for runtime in runtimes}
            if len(python_minors) != 1:
                failures.append(f"driver/worker Python mismatch: {python_minors}")
            if pyspark_versions != {"3.4.1"}:
                failures.append(f"expected PySpark 3.4.1: {pyspark_versions}")

    print(json.dumps({"runtimes": report, "failures": failures}, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
