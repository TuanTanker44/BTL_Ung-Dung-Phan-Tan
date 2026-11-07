ALTER TABLE traffic_dw.fact_traffic_flow
ADD COLUMN IF NOT EXISTS detection_mode
LowCardinality(String) DEFAULT 'unknown';