from common import get_args, get_spark, save

FILES_TO_TABLES = {
    "customers.csv": "bronze_customers",
    "accounts.csv": "bronze_accounts",
    "transactions.csv": "bronze_transactions",
    "branches.csv": "bronze_branches",
}


def ingest_bronze(spark, catalog: str, schema: str, volume: str) -> None:
    base = f"/Volumes/{catalog}/{schema}/{volume}/raw"
    for file_name, table in FILES_TO_TABLES.items():
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{base}/{file_name}")
        )
        save(df, f"{catalog}.{schema}.{table}")


if __name__ == "__main__":
    args = get_args(["volume"])
    ingest_bronze(get_spark(), args.catalog, args.schema, args.volume)
