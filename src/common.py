import argparse

from pyspark.sql import SparkSession


def get_args(extra=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    for name in extra or []:
        parser.add_argument(f"--{name}", required=True)
    return parser.parse_args()


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def save(df, full_table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )
    print(f"Wrote {full_table_name}")
