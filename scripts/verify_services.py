"""Wait for real service readiness, including Spark worker and app health."""

import argparse
import base64
import json
import time
import urllib.request


def http_get(url, username=None, password=None):
    request = urllib.request.Request(url)
    if username is not None:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.read().decode("utf-8")


def check_infrastructure():
    if http_get("http://localhost:8123/ping").strip() != "Ok.":
        raise RuntimeError("ClickHouse /ping did not return Ok.")
    http_get("http://localhost:9000/minio/health/ready")

    rabbit = json.loads(
        http_get(
            "http://localhost:15672/api/health/checks/ready-to-serve-clients",
            "guest",
            "guest",
        )
    )
    if rabbit.get("status") != "ok":
        raise RuntimeError(f"RabbitMQ is not ready: {rabbit}")

    spark = json.loads(http_get("http://localhost:8080/json/"))
    alive_workers = [
        worker for worker in spark.get("workers", []) if worker.get("state") == "ALIVE"
    ]
    if not alive_workers:
        raise RuntimeError("Spark master has no ALIVE worker")

    airflow = json.loads(http_get("http://localhost:8085/health"))
    if airflow.get("metadatabase", {}).get("status") != "healthy":
        raise RuntimeError(f"Airflow metadata database is unhealthy: {airflow}")
    if airflow.get("scheduler", {}).get("status") != "healthy":
        raise RuntimeError(f"Airflow scheduler is unhealthy: {airflow}")


def check_applications():
    import grpc
    from grpc_health.v1 import health_pb2, health_pb2_grpc

    with grpc.insecure_channel("localhost:50051") as channel:
        reply = health_pb2_grpc.HealthStub(channel).Check(
            health_pb2.HealthCheckRequest(service=""), timeout=5
        )
        if reply.status != health_pb2.HealthCheckResponse.SERVING:
            raise RuntimeError(f"gRPC ingestion health status is {reply.status}")
    if http_get("http://localhost:8501/_stcore/health").strip().lower() != "ok":
        raise RuntimeError("Streamlit health endpoint did not return ok")


def wait_for(check, timeout, label):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            check()
            print(f"OK: {label}")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {label}: {last_error}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "scope", choices=("infrastructure", "applications", "all"), default="all", nargs="?"
    )
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if args.scope in ("infrastructure", "all"):
        wait_for(check_infrastructure, args.timeout, "infrastructure services")
    if args.scope in ("applications", "all"):
        wait_for(check_applications, args.timeout, "gRPC and Streamlit applications")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
