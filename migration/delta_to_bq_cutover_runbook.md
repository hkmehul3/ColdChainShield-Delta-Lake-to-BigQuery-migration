# ColdChainShield (Delta Lake) to ChronosGrid (BigQuery) Cutover & Rollback Runbook

## Pre-Cutover Readiness
- Delta Silver table (`main.coldchain_shield.silver_thermal_excursions`) compacted (`OPTIMIZE`) and snapshot version recorded via `DESCRIBE HISTORY`.
- Parquet export staged in `gs://chronos-grid-bronze-chronos-grid-prod/coldchain_delta_export/`.
- BigQuery target table `chronos_grid_prod.coldchain_thermal_excursions` created with `DATE(recorded_at)` partitioning and `require_partition_filter = TRUE`.

## Phased Cutover Steps
1. **T-24 Hours (Shadow Load & Recall Disposition Diff):**
   - Execute `bigquery/03_coldchain_delta_migration.sql` to merge Delta staging rows into BigQuery.
   - Compare `recall_disposition` outputs (`QUARANTINE_IMMEDIATE`, `QA_HOLD_INSPECT`, `RELEASE_APPROVED`) between Databricks Gold and BigQuery `vw_coldchain_recall_live`.
2. **T-15 Minutes (Source Write Freeze & Final Snapshot Export):**
   - Pause scheduled Databricks Silver `MERGE` jobs and capture final Delta version `V_final`.
   - Run `delta_to_bigquery_export.py --version-as-of <V_final>` to stage final snapshot to GCS.
3. **T-0 (DVT Parity Gate & Atomic View Switch):**
   - Run `data-validation validate column -c migration/dvt_delta_to_bq.yaml` and row-hash check on `migration_row_hash`.
   - Confirm `0.0` drift across row counts, `SUM(excursion_minutes)`, and SHA-256 hashes.
   - Promote `chronos_grid_prod.vw_coldchain_recall_live` as the active downstream BI/API view.

## Rollback Plan
- Repoint downstream consumers back to the Databricks Delta Gold view (`main.coldchain_shield.gold_batch_recall_watch`).
- Retain `V_final` in Delta Lake time travel (`VERSION AS OF`) to isolate and replay any failed batch window without data loss.
