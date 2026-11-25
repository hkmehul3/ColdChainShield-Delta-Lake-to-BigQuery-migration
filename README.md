# ColdChainShield Delta Lake to ChronosGrid BigQuery Migration

Cross-lakehouse migration module connecting **ColdChainShield** (Databricks Delta Lake) to **ChronosGrid** (Google Cloud BigQuery) with deterministic row hashing, Google Data Validation Tool (DVT) parity assertions, Cloud Composer orchestration, and a phased cutover/rollback runbook.

## Repository Contents
1. **`databricks/delta_to_bigquery_export.py`:** Reads a pinned Delta table snapshot (`versionAsOf`) or Change Data Feed window from `main.coldchain_shield.silver_thermal_excursions`, computes a deterministic `migration_row_hash`, and writes Snappy-compressed Parquet staging files to Cloud Storage (`gs://...`).
2. **`bigquery/03_coldchain_delta_migration.sql`:** Provisions the partitioned (`DATE(recorded_at)`) and clustered (`product_category`, `batch_lot_id`, `destination_hub`) target table `chronos_grid_prod.coldchain_thermal_excursions`, a staging external table, an idempotent `MERGE` loader, and a cutover view (`vw_coldchain_recall_live`).
3. **`migration/dvt_delta_to_bq.yaml`:** Google Data Validation Tool (DVT) configuration verifying row count, `SUM(excursion_minutes)`, `MAX(temp_deviation_c)`, and SHA-256 hash parity between staged Delta exports and BigQuery.
4. **`migration/delta_to_bq_cutover_runbook.md`:** Step-by-step T-24h to T-0 shadow sync, write freeze, DVT gate, atomic view swap, and rollback procedure.
5. **`dags/delta_to_bq_migration_dag.py`:** Cloud Composer (Airflow) DAG automating GCS staging load, BigQuery `MERGE` promotion, Cloud Run DVT validation, and Dataplex asset discovery refresh.
