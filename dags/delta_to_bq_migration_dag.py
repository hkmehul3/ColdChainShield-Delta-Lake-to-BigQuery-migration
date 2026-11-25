from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateOrUpdateAssetOperator

PROJECT_ID = "chronos-grid-prod"
REGION = "asia-south1"

default_args = {
    "owner": "harseet-tiwari",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="coldchain_delta_to_bigquery_migration",
    default_args=default_args,
    description="Merge staged ColdChainShield Delta exports into BigQuery with DVT parity & Dataplex sync",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 11, 25),
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "delta-lake", "bigquery", "migration", "dvt", "dataplex"],
) as dag:

    merge_delta_to_bq = BigQueryInsertJobOperator(
        task_id="merge_coldchain_delta_into_bigquery",
        configuration={
            "query": {
                "query": "{% include '03_coldchain_delta_migration.sql' %}",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    validate_delta_bq_parity = CloudRunExecuteJobOperator(
        task_id="dvt_delta_to_bigquery_parity_check",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="coldchain-dvt-reconciler",
    )

    refresh_dataplex_catalog = DataplexCreateOrUpdateAssetOperator(
        task_id="sync_dataplex_coldchain_asset",
        project_id=PROJECT_ID,
        region=REGION,
        lake_id="chronos-sovereign-lake",
        zone_id="curated-grid",
        asset_id="coldchain-excursions-bq",
        body={
            "resource_spec": {
                "name": f"projects/{PROJECT_ID}/datasets/chronos_grid_prod",
                "type": "BIGQUERY_DATASET",
            },
            "discovery_spec": {"enabled": True, "schedule": "0 */4 * * *"},
        },
    )

    merge_delta_to_bq >> validate_delta_bq_parity >> refresh_dataplex_catalog
