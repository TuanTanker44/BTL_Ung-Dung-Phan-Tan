"""Run one real ingestion -> RabbitMQ -> Spark worker -> ClickHouse smoke test."""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def get_spark_snapshot():
    from verify_services import http_get

    return json.loads(http_get("http://localhost:8080/json/"))


def active_workers(snapshot):
    active = []
    for worker in snapshot.get("workers", []):
        executors = worker.get("executors", [])
        if worker.get("state") == "ALIVE" and (
            int(worker.get("coresused", 0)) > 0 or executors
        ):
            active.append(
                {
                    "id": worker.get("id"),
                    "host": worker.get("host"),
                    "coresused": worker.get("coresused", 0),
                }
            )
    return active


def upload_probe():
    import grpc
    from ingestion import client

    image = client.create_dummy_traffic_image(
        "CAM-HN-001",
        {"Car": 2, "Motorcycle": 3, "Bus": 0, "Truck": 1, "Pedestrian": 1},
    )
    request = client.image_ingestion_pb2.ImageUploadRequest(
        filename=f"e2e_{int(time.time())}.jpg",
        image_data=image,
        source="e2e_smoke_test",
        mime_type="image/jpeg",
        camera_id="CAM-HN-001",
        capture_timestamp=datetime.now(timezone.utc).isoformat(),
    )
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = client.image_ingestion_pb2_grpc.ImageIngestionServiceStub(channel)
        response = stub.UploadImage(request, timeout=15)
    if response.status != "SUCCESS" or not response.image_id:
        raise RuntimeError(f"Ingestion failed: {response.status} {response.message}")
    return response.image_id


def run_spark_and_observe_worker(timeout):
    command = [
        "docker",
        "exec",
        "-e",
        "DETECTION_MODE=mock",
        "-e",
        "SPARK_MASTER_URL=spark://spark-master:7077",
        "-e",
        "MINIO_ENDPOINT=minio:9000",
        "-e",
        "RABBITMQ_HOST=rabbitmq",
        "-e",
        "CLICKHOUSE_HOST=clickhouse",
        "image_pipeline_spark_master",
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "/opt/spark-jobs/spark_job.py",
    ]
    process = subprocess.Popen(command)
    deadline = time.monotonic() + timeout
    observed = []
    while process.poll() is None and time.monotonic() < deadline:
        try:
            observed.extend(active_workers(get_spark_snapshot()))
        except Exception:
            pass
        time.sleep(0.2)
    if process.poll() is None:
        process.terminate()
        raise RuntimeError("spark-submit exceeded smoke-test timeout")
    if process.returncode:
        raise RuntimeError(f"spark-submit exited with code {process.returncode}")
    if not observed:
        raise RuntimeError("No Spark worker executor activity was observed")
    return list({item["id"]: item for item in observed}.values())


def verify_clickhouse(image_id, timeout):
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="traffic_password",
        database="traffic_dw",
    )
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = client.query(
                """
                SELECT count(), uniqExact(vehicle_type_id), any(detection_mode)
                FROM fact_traffic_flow FINAL
                WHERE image_id = {image_id:String}
                """,
                parameters={"image_id": image_id},
            ).result_rows[0]
            if int(result[0]) == 5 and int(result[1]) == 5:
                return {
                    "rows": int(result[0]),
                    "vehicle_categories": int(result[1]),
                    "detection_mode": result[2],
                }
            time.sleep(1)
    finally:
        client.close()
    raise RuntimeError(f"ClickHouse did not contain five categories for {image_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    from verify_services import check_applications, check_infrastructure

    check_infrastructure()
    check_applications()
    image_id = upload_probe()
    workers = run_spark_and_observe_worker(args.timeout)
    clickhouse = verify_clickhouse(image_id, args.timeout)
    evidence = {
        "status": "passed",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "image_id": image_id,
        "spark_workers_observed": workers,
        "clickhouse": clickhouse,
    }
    rendered = json.dumps(evidence, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
