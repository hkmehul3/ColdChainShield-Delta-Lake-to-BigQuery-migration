-- 1. Staging External Table over Delta Lake Parquet Export in GCS
CREATE OR REPLACE EXTERNAL TABLE `chronos_grid_staging.ext_coldchain_delta_stage`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://chronos-grid-bronze-chronos-grid-prod/coldchain_delta_export/*.parquet']
);

-- 2. Target Partitioned & Clustered Native BigQuery Table
CREATE TABLE IF NOT EXISTS `chronos_grid_prod.coldchain_thermal_excursions` (
  reading_id STRING NOT NULL,
  shipment_id STRING NOT NULL,
  batch_lot_id STRING NOT NULL,
  product_category STRING NOT NULL,
  carrier STRING NOT NULL,
  transit_lane STRING NOT NULL,
  destination_hub STRING NOT NULL,
  temp_celsius FLOAT64 NOT NULL,
  humidity_pct FLOAT64,
  recorded_at TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  min_safe_temp_c FLOAT64,
  max_safe_temp_c FLOAT64,
  temp_deviation_c FLOAT64,
  is_thermal_excursion BOOL NOT NULL,
  excursion_minutes INT64 NOT NULL,
  record_hash STRING,
  migration_row_hash STRING
)
PARTITION BY DATE(recorded_at)
CLUSTER BY product_category, batch_lot_id, destination_hub
OPTIONS (
  require_partition_filter = TRUE,
  description = "Migrated ColdChainShield Delta Lake thermal excursion table in BigQuery"
);

-- 3. Idempotent MERGE from Staged Delta Export into Native BigQuery Table
MERGE `chronos_grid_prod.coldchain_thermal_excursions` T
USING `chronos_grid_staging.ext_coldchain_delta_stage` S
ON T.reading_id = S.reading_id
   AND DATE(T.recorded_at) = DATE(S.recorded_at)
WHEN MATCHED AND T.migration_row_hash <> S.migration_row_hash THEN
  UPDATE SET
    shipment_id = S.shipment_id,
    batch_lot_id = S.batch_lot_id,
    product_category = S.product_category,
    carrier = S.carrier,
    transit_lane = S.transit_lane,
    destination_hub = S.destination_hub,
    temp_celsius = S.temp_celsius,
    humidity_pct = S.humidity_pct,
    recorded_at = S.recorded_at,
    ingested_at = S.ingested_at,
    min_safe_temp_c = S.min_safe_temp_c,
    max_safe_temp_c = S.max_safe_temp_c,
    temp_deviation_c = S.temp_deviation_c,
    is_thermal_excursion = S.is_thermal_excursion,
    excursion_minutes = S.excursion_minutes,
    record_hash = S.record_hash,
    migration_row_hash = S.migration_row_hash
WHEN NOT MATCHED THEN
  INSERT ROW;

-- 4. Live Gold Recall Watch View Cutover Target
CREATE OR REPLACE VIEW `chronos_grid_prod.vw_coldchain_recall_live` AS
SELECT
  batch_lot_id,
  product_category,
  shipment_id,
  carrier,
  transit_lane,
  destination_hub,
  COUNT(*) AS total_sensor_packets,
  SUM(excursion_minutes) AS cumulative_excursion_minutes,
  MAX(temp_celsius) AS peak_recorded_temp_c,
  MAX(temp_deviation_c) AS max_threshold_breach_c,
  CASE
    WHEN SUM(excursion_minutes) >= 180 OR MAX(temp_deviation_c) >= 5.0 THEN 'QUARANTINE_IMMEDIATE'
    WHEN SUM(excursion_minutes) >= 45 OR MAX(temp_deviation_c) >= 2.0 THEN 'QA_HOLD_INSPECT'
    ELSE 'RELEASE_APPROVED'
  END AS recall_disposition
FROM `chronos_grid_prod.coldchain_thermal_excursions`
WHERE recorded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1, 2, 3, 4, 5, 6;
