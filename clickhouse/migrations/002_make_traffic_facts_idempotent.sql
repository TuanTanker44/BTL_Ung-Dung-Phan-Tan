-- Rebuild an existing fact table with the idempotent storage contract used by
-- schema.sql. This migration is safe to run again after it has completed.
DROP TABLE IF EXISTS traffic_dw.fact_traffic_flow_replacing;

CREATE TABLE traffic_dw.fact_traffic_flow_replacing (
    event_id String,
    timestamp DateTime,
    camera_id String,
    vehicle_type_id UInt8,
    count UInt16,
    congestion_level String,
    image_id String,
    inference_time_ms UInt32,
    detection_mode LowCardinality(String) DEFAULT 'unknown'
) ENGINE = ReplacingMergeTree()
ORDER BY (event_id, vehicle_type_id);

INSERT INTO traffic_dw.fact_traffic_flow_replacing
SELECT
    event_id,
    argMax(timestamp, tuple(timestamp, image_id)) AS timestamp,
    argMax(camera_id, tuple(timestamp, image_id)) AS camera_id,
    vehicle_type_id,
    argMax(count, tuple(timestamp, image_id)) AS count,
    argMax(congestion_level, tuple(timestamp, image_id)) AS congestion_level,
    argMax(image_id, tuple(timestamp, image_id)) AS image_id,
    argMax(inference_time_ms, tuple(timestamp, image_id)) AS inference_time_ms,
    argMax(detection_mode, tuple(timestamp, image_id)) AS detection_mode
FROM traffic_dw.fact_traffic_flow
GROUP BY event_id, vehicle_type_id;

EXCHANGE TABLES
    traffic_dw.fact_traffic_flow
    AND traffic_dw.fact_traffic_flow_replacing;

DROP TABLE traffic_dw.fact_traffic_flow_replacing;
