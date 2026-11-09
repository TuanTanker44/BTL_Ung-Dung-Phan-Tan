import os
import sys
import json
import time
import random
import logging
from datetime import datetime
from queue import Queue, Empty
from threading import Thread
from functools import partial
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Load configurations
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "image_processing_queue")
DLQ_NAME = f"{QUEUE_NAME}.dlq"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "traffic_dw")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "traffic_password")

# Try to import PySpark
try:
    # pyrefly: ignore [missing-import]
    from pyspark.sql import SparkSession
except ImportError:
    logger.error("PySpark is not installed. Please install pyspark to run this script.")
    sys.exit(1)

def validate_event(payload):
    """Reject malformed metadata before it reaches Spark or ClickHouse."""
    if not isinstance(payload, dict):
        raise ValueError("Event must be a JSON object")
    required_strings = (
        "event_id", "image_id", "bucket", "s3_key", "uploaded_at",
        "filename", "s3_uri", "mime_type",
    )
    for key in required_strings:
        if not isinstance(payload.get(key), str) or not payload[key].strip():
            raise ValueError(f"{key} must be a non-empty string")
    size = payload.get("file_size_bytes")
    if type(size) is not int or not 0 <= size <= 2**64 - 1:
        raise ValueError("file_size_bytes must be a UInt64 integer")
    if not payload["s3_uri"].startswith("s3://"):
        raise ValueError("s3_uri must start with s3://")
    if "camera_id" in payload:
        if not isinstance(payload["camera_id"], str) or not payload["camera_id"].strip():
            raise ValueError("camera_id must be a non-empty string")
    for key in ("uploaded_at", "capture_timestamp"):
        if key not in payload:
            continue
        value = payload[key]
        if not isinstance(value, str):
            raise ValueError(f"{key} must be an ISO timestamp string")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{key} must be an ISO timestamp string") from exc
        # ClickHouse DateTime stores seconds in an unsigned 32-bit integer.
        # Naive timestamps are interpreted using the driver's local timezone.
        if not 0 <= parsed.timestamp() <= 2**32 - 1:
            raise ValueError(f"{key} is outside the ClickHouse DateTime range")


# The RabbitMQ connection/channel belong exclusively to this thread.
# Spark may block for a long time on the main thread without starving heartbeats.
def rabbitmq_consumer(inbox, commands, replies):
    import pika

    connection = None
    delivered = False
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=300,
            socket_timeout=15,
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_declare(queue=DLQ_NAME, durable=True)
        channel.confirm_delivery()

        events = []
        deliveries = []
        for _ in range(50):
            method, properties, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)
            if method is None:
                break
            try:
                payload = json.loads(body.decode("utf-8"))
                validate_event(payload)
            except (ValueError, UnicodeError, OverflowError, OSError) as exc:
                headers = dict(properties.headers or {})
                headers["x-error-code"] = "INVALID_EVENT"
                headers["x-last-error"] = str(exc)[:1000]
                # Keep the original bytes, even when no event_id can be decoded.
                # Publication errors must propagate: never ACK before confirmation.
                channel.basic_publish(
                    exchange="",
                    routing_key=DLQ_NAME,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type=properties.content_type,
                        delivery_mode=2,
                        message_id=properties.message_id,
                        headers=headers,
                    ),
                    mandatory=True,
                )
                channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
                logger.warning("Invalid delivery %s sent to %s: %s",
                               method.delivery_tag, DLQ_NAME, exc)
                continue
            events.append(payload)
            deliveries.append((method.delivery_tag, payload["event_id"], body, properties))

        inbox.put((events, None))
        delivered = True
        if not events:
            return

        # All Pika calls occur here, including heartbeat service and ACK.
        while True:
            connection.process_data_events(time_limit=1)
            try:
                command = commands.get_nowait()
            except Empty:
                continue
            if isinstance(command, tuple) and command[0] == "COMPLETE":
                _, failures = command

                for tag, event_id, body, properties in deliveries:
                    if event_id in failures:
                        headers = dict(properties.headers or {})
                        attempt = int(headers.get("x-processing-attempt", 0)) + 1

                        headers["x-processing-attempt"] = attempt
                        headers["x-last-error"] = failures[event_id]

                        # Anh khong ton tai: chuyen thang vao DLQ
                        is_missing_image = "NoSuchKey" in failures[event_id]

                        # Loi tam thoi: retry toi da 3 lan
                        target_queue = (
                        DLQ_NAME
                        if is_missing_image or attempt >= 3
                        else QUEUE_NAME
                    )
                        channel.basic_publish(
                            exchange="",
                            routing_key=target_queue,
                            body=body,
                            properties=pika.BasicProperties(
                                content_type=properties.content_type,
                                delivery_mode=2,
                                message_id=event_id,
                                headers=headers,
                            ),
                            mandatory=True,
                        )
                        logger.warning(
                            "Event %s failed on attempt %d; sent to %s",
                            event_id, attempt, target_queue,
                        )

                    channel.basic_ack(delivery_tag=tag, multiple=False)

                connection.process_data_events(time_limit=0)
                replies.put(None)
                return
            if command == "ABORT":
                # Connection close requeues every unacknowledged delivery.
                replies.put(None)
                return
            raise ValueError("Unknown consumer command")
    except BaseException as exc:
        logger.exception("RabbitMQ consumer failed")
        if not delivered:
            inbox.put((None, exc))
        else:
            replies.put(exc)
    finally:
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                logger.exception("Failed to close RabbitMQ connection")


def wait_for_consumer_reply(replies, consumer, timeout=30):
    try:
        error = replies.get(timeout=timeout)
    except Empty:
        raise RuntimeError("RabbitMQ consumer did not acknowledge completion")
    if error is not None:
        raise RuntimeError("RabbitMQ consumer failed") from error


def process_partition(iterator, detection_mode):
    """
    Processes a partition of events: downloads images from MinIO,
    runs YOLOv8 object detection (or mock detection), and yields metrics.
    """
    import io
    from minio.error import S3Error
    
    # Initialize MinIO client inside worker partition
    try:
        # pyrefly: ignore [missing-import]
        from minio import Minio
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
    except ImportError:
        logger.error("minio library missing on worker node.")
        raise

    if detection_mode == "yolo":
        model_path = Path("/opt/spark-jobs/weights/yolov8n.pt")
        if not model_path.is_file():
            raise FileNotFoundError(f"Thiếu model YOLOv8: {model_path}")

        from PIL import Image
        from ultralytics import YOLO

        model = YOLO(str(model_path))

    vehicle_type_mapping = {
        'Car': 1,
        'Motorcycle': 2,
        'Bus': 3,
        'Truck': 4,
        'Pedestrian': 5
    }

    for row in iterator:
        # row: (event_id, image_id, bucket, s3_key, uploaded_at, filename, camera_id, capture_timestamp)
        event_id, image_id, bucket, s3_key, uploaded_at, filename, camera_id, capture_timestamp = row
        start_time = time.time()
        
        vehicle_counts = {
            'Car': 0,
            'Motorcycle': 0,
            'Bus': 0,
            'Truck': 0,
            'Pedestrian': 0
        }

        try:
            # 1. Fetch image from MinIO
            response = None
            try:
                response = minio_client.get_object(bucket, s3_key)
                img_data = response.read()

                if not img_data:
                    raise ValueError(
                        f"Empty image data: bucket={bucket}, key={s3_key}"
                    )
            finally:
                if response is not None:
                    try:
                        response.close()
                    finally:
                        response.release_conn()

            if detection_mode == "yolo":
                # 2. Run real YOLOv8 object detection
                img_io = io.BytesIO(img_data)
                img = Image.open(img_io)
                results = model(img)
                
                # YOLO COCO class indices:
                # 0: person (Pedestrian)
                # 2: car (Car)
                # 3: motorcycle (Motorcycle)
                # 5: bus (Bus)
                # 7: truck (Truck)
                for r in results:
                    for box in r.boxes:
                        cls_id = int(box.cls[0].item())
                        if cls_id == 2:
                            vehicle_counts['Car'] += 1
                        elif cls_id == 3:
                            vehicle_counts['Motorcycle'] += 1
                        elif cls_id == 5:
                            vehicle_counts['Bus'] += 1
                        elif cls_id == 7:
                            vehicle_counts['Truck'] += 1
                        elif cls_id == 0:
                            vehicle_counts['Pedestrian'] += 1
            else:
                # 3. Fallback: High-fidelity Time-of-day Mock Detection
                try:
                    dt = datetime.fromisoformat(capture_timestamp)
                    hour = dt.hour
                except Exception:
                    hour = 12  # default to noon
                
                is_peak = (7 <= hour <= 9) or (17 <= hour <= 19)
                is_night = (hour >= 22) or (hour <= 5)

                if is_peak:
                    vehicle_counts = {
                        'Car': random.randint(15, 30),
                        'Motorcycle': random.randint(40, 80),
                        'Bus': random.randint(2, 6),
                        'Truck': random.randint(1, 3),
                        'Pedestrian': random.randint(5, 15)
                    }
                elif is_night:
                    vehicle_counts = {
                        'Car': random.randint(0, 3),
                        'Motorcycle': random.randint(1, 5),
                        'Bus': 0,
                        'Truck': random.randint(1, 4),
                        'Pedestrian': random.randint(0, 1)
                    }
                else:
                    vehicle_counts = {
                        'Car': random.randint(5, 15),
                        'Motorcycle': random.randint(15, 35),
                        'Bus': random.randint(0, 2),
                        'Truck': random.randint(0, 2),
                        'Pedestrian': random.randint(2, 6)
                    }

                # Camera-specific modifers
                if camera_id == 'CAM-HN-003':  # Nhat Tan Bridge
                    vehicle_counts['Pedestrian'] = 0
                    vehicle_counts['Car'] = int(vehicle_counts['Car'] * 1.5)
                    vehicle_counts['Truck'] = int(vehicle_counts['Truck'] * 1.5)
                elif camera_id == 'CAM-HN-001': # Kim Ma
                    vehicle_counts['Motorcycle'] = int(vehicle_counts['Motorcycle'] * 1.3)
                    vehicle_counts['Truck'] = max(0, vehicle_counts['Truck'] - 1)

        except S3Error as exc:
            if exc.code != "NoSuchKey":
                raise

            reason = f"MinIO {exc.code}: {bucket}/{s3_key}"
            logger.warning("Image missing for event %s: %s", event_id, reason)
            yield event_id, [], reason
            continue
        except Exception:
            logger.exception("Error processing image %s", image_id)
            raise

        inference_time_ms = int((time.time() - start_time) * 1000)
        
        # Determine congestion level
        total_vehicles = sum(vehicle_counts.values())
        if total_vehicles >= 45:
            congestion_level = "High"
        elif total_vehicles >= 15:
            congestion_level = "Medium"
        else:
            congestion_level = "Low"

        rows = []
        for v_type, count in vehicle_counts.items():
            v_type_id = vehicle_type_mapping.get(v_type, 0)
            rows.append((
                event_id,
                capture_timestamp,
                camera_id,
                v_type_id,
                count,
                congestion_level,
                image_id,
                inference_time_ms,
            ))

        yield event_id, rows, None



def save_to_clickhouse(events, traffic_results, detection_mode):
    """
    Save image metadata and traffic results to ClickHouse.

    Avoid inserting existing image IDs and existing
    (event_id, vehicle_type_id) pairs.

    Intended for a single active pipeline writer.
    """
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    try:
        if not events:
            logger.info("No events to save.")
            return

        # 1. Check existing images
        image_ids = list({
            event["image_id"] for event in events
        })

        existing_images_result = client.query(
            """
            SELECT DISTINCT image_id
            FROM dim_images
            WHERE image_id IN {ids:Array(String)}
            """,
            parameters={"ids": image_ids}
        )

        existing_images = {
            row[0]
            for row in existing_images_result.result_rows
        }

        # 2. Check existing event/vehicle keys
        event_ids = list({
            event["event_id"] for event in events
        })

        existing_facts_result = client.query(
            """
            SELECT DISTINCT event_id, vehicle_type_id, detection_mode
            FROM fact_traffic_flow
            WHERE event_id IN {ids:Array(String)}
            """,
            parameters={"ids": event_ids}
        )

        for event_id, _, stored_mode in existing_facts_result.result_rows:
            if stored_mode != detection_mode:
                raise RuntimeError(
                    f"Event {event_id} đã ghi một phần ở chế độ {stored_mode!r}"
                )

        existing_keys = {
            (row[0], int(row[1]))
            for row in existing_facts_result.result_rows
        }
        # 3. Prepare new image metadata
        dim_images_data = []
        seen_images = set()

        for event in events:
            image_id = event["image_id"]

            if image_id in existing_images:
                continue

            if image_id in seen_images:
                continue

            dim_images_data.append((
                image_id,
                event["filename"],
                event["s3_uri"],
                int(event["file_size_bytes"]),
                event["mime_type"],
                datetime.fromisoformat(
                    event["uploaded_at"]
                )
            ))

            seen_images.add(image_id)

        # 4. Prepare new traffic results
        fact_traffic_data = []
        seen_keys = set()

        for result in traffic_results:
            key = (
                result[0],
                int(result[3])
            )

            if key in existing_keys:
                continue

            if key in seen_keys:
                continue

            fact_traffic_data.append((
                result[0],
                datetime.fromisoformat(result[1]),
                result[2],
                int(result[3]),
                int(result[4]),
                result[5],
                result[6],
                int(result[7]),
                detection_mode
))

            seen_keys.add(key)

        # 5. Insert missing images
        if dim_images_data:
            client.insert(
                "dim_images",
                dim_images_data,
                column_names=[
                    "image_id",
                    "filename",
                    "s3_uri",
                    "file_size_bytes",
                    "mime_type",
                    "uploaded_at"
                ]
            )

        # 6. Insert missing traffic records
        if fact_traffic_data:
            client.insert(
                "fact_traffic_flow",
                fact_traffic_data,
                column_names=[
                    "event_id",
                    "timestamp",
                    "camera_id",
                    "vehicle_type_id",
                    "count",
                    "congestion_level",
                    "image_id",
                    "inference_time_ms",
                    "detection_mode"
                ]
            )

        # 7. Verify all event/vehicle keys exist
        expected_keys = {
            (event["event_id"], vehicle_type_id)
            for event in events
            for vehicle_type_id in range(1, 6)
        }

        verification_result = client.query(
            """
            SELECT event_id, vehicle_type_id
            FROM fact_traffic_flow
            WHERE event_id IN {ids:Array(String)}
            GROUP BY event_id, vehicle_type_id
            """,
            parameters={"ids": event_ids}
        )

        verified_keys = {
            (row[0], int(row[1]))
            for row in verification_result.result_rows
        }

        missing_keys = expected_keys - verified_keys

        if missing_keys:
            raise RuntimeError(
                f"Missing ClickHouse records: {missing_keys}"
            )

        logger.info(
            "ClickHouse verified. Inserted %d images "
            "and %d traffic rows.",
            len(dim_images_data),
            len(fact_traffic_data)
        )

    finally:
        client.close()

def get_completed_event_ids(event_ids):
    """
    Return event IDs that already have all five
    vehicle categories in ClickHouse.
    """
    if not event_ids:
        return set()

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    try:
        result = client.query(
            """
            SELECT event_id
            FROM fact_traffic_flow
            WHERE event_id IN {ids:Array(String)}
            GROUP BY event_id
            HAVING uniqExact(vehicle_type_id) = 5
            """,
            parameters={
                "ids": list(set(event_ids))
            }
        )

        return {
            row[0]
            for row in result.result_rows
        }

    finally:
        client.close()

def main():
    detection_mode = os.getenv("DETECTION_MODE", "").strip().lower()
    if detection_mode not in {"mock", "yolo"}:
        raise ValueError("Hãy đặt DETECTION_MODE=mock hoặc DETECTION_MODE=yolo")

    logger.info("Detection mode: %s", detection_mode)
    inbox = Queue(maxsize=1)
    commands = Queue(maxsize=1)
    replies = Queue(maxsize=1)
    consumer = Thread(
        target=rabbitmq_consumer,
        args=(inbox, commands, replies),
        name="rabbitmq-consumer",
        daemon=True,
    )
    consumer.start()
    spark = None
    command_sent = False
    try:
        try:
            events, error = inbox.get(timeout=45)
        except Empty:
            raise RuntimeError("Timed out fetching RabbitMQ events")
        if error is not None:
            raise RuntimeError("Unable to fetch RabbitMQ events") from error
        if not events:
            logger.info("RabbitMQ queue is empty")
            return

        unique_events = {}
        for event in events:
            eid = event["event_id"]
            if eid in unique_events and unique_events[eid] != event:
                raise ValueError("Conflicting messages for event_id: %s" % eid)
            unique_events[eid] = event
        event_ids = list(unique_events)
        completed = get_completed_event_ids(event_ids)
        pending = [e for eid, e in unique_events.items() if eid not in completed]
        logger.info("Messages=%d, completed=%d, pending=%d",
                    len(events), len(completed), len(pending))

        failures = {}
        if pending:
            spark = (
                SparkSession.builder
                .appName("Distributed-Traffic-YOLO-Detection")
                .master(
                    os.getenv(
                        "SPARK_MASTER_URL",
                        "spark://spark-master:7077"
                    )
                )
                .getOrCreate()
            )
            spark_input_data = [
                (e["event_id"], e["image_id"], e["bucket"], e["s3_key"],
                 e["uploaded_at"], e["filename"],
                 e.get("camera_id", "CAM-UNKNOWN"),
                 e.get("capture_timestamp", e["uploaded_at"]))
                for e in pending
            ]
            outcomes = (
                spark.sparkContext.parallelize(spark_input_data)
                .mapPartitions(
                    partial(process_partition, detection_mode=detection_mode)
                )
                .collect()
            )

            pending_ids = {event["event_id"] for event in pending}
            returned_ids = {event_id for event_id, _, _ in outcomes}
            if len(outcomes) != len(pending) or returned_ids != pending_ids:
                raise RuntimeError("Missing or duplicate Spark event results")

            traffic_results = []
            for event_id, rows, reason in outcomes:
                if reason is None:
                    traffic_results.extend(rows)
                else:
                    failures[event_id] = reason

            successful = [
                event for event in pending if event["event_id"] not in failures
            ]
            expected = {
                (event["event_id"], kind)
                for event in successful
                for kind in range(1, 6)
            }
            actual = [(row[0], int(row[3])) for row in traffic_results]
            if len(actual) != len(expected) or set(actual) != expected:
                raise RuntimeError("Incomplete or duplicate Spark output; refusing INSERT")

            if successful:
                save_to_clickhouse(successful, traffic_results, detection_mode)

        successful_ids = set(event_ids) - set(failures)
        verified = get_completed_event_ids(successful_ids)
        if successful_ids - verified:
            raise RuntimeError("ClickHouse verification failed; refusing ACK")

        commands.put(("COMPLETE", failures))
        command_sent = True
        wait_for_consumer_reply(replies, consumer)
        logger.info("Verified events; requested ACK on original RabbitMQ channel")
    except BaseException:
        logger.exception("Pipeline failed; unacknowledged RabbitMQ messages will be requeued")
        if not command_sent and consumer.is_alive():
            commands.put("ABORT")
            command_sent = True
            try:
                wait_for_consumer_reply(replies, consumer)
            except Exception:
                logger.exception("RabbitMQ cleanup failed")
        raise
    finally:
        if spark is not None:
            spark.stop()
        consumer.join(timeout=15)
        if consumer.is_alive():
            logger.error("RabbitMQ consumer thread still running after shutdown")


if __name__ == "__main__":
    main()
