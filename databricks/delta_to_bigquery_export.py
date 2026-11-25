import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export ColdChainShield Delta table for BigQuery migration")
    parser.add_argument(
        "--delta-table",
        default="main.coldchain_shield.silver_thermal_excursions",
        help="Source Delta table name",
    )
    parser.add_argument(
        "--gcs-export-path",
        required=True,
        help="Destination GCS path for staged Parquet files",
    )
    parser.add_argument(
        "--version-as-of",
        type=int,
        required=False,
        help="Optional Delta table version for reproducible snapshot export",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("ColdChain-Delta-To-GCS-Export").getOrCreate()

    reader = spark.read.format("delta")
    if args.version_as_of is not None:
        reader = reader.option("versionAsOf", args.version_as_of)

    df = reader.table(args.delta_table)

    # Compute deterministic SHA-256 digest for post-load BigQuery DVT reconciliation
    export_df = df.withColumn(
        "migration_row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("reading_id"),
                F.col("batch_lot_id"),
                F.col("temp_celsius").cast("string"),
                F.col("excursion_minutes").cast("string"),
                F.col("recorded_at").cast("string"),
            ),
            256,
        ),
    )

    (
        export_df.write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.gcs_export_path)
    )

    print(f"Exported {export_df.count()} rows from {args.delta_table} to {args.gcs_export_path}")
    spark.stop()


if __name__ == "__main__":
    main()
