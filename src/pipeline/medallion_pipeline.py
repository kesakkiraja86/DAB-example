import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW = f"/Volumes/{spark.conf.get('catalog')}/{spark.conf.get('schema')}/{spark.conf.get('volume')}/raw"


def _raw(name):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{RAW}/{name}.csv")
    )


# Bronze: incremental raw ingestion with Auto Loader
@dlt.table(name="dlt_bronze_customers")
def bronze_customers():
    return _raw("customers")


@dlt.table(name="dlt_bronze_accounts")
def bronze_accounts():
    return _raw("accounts")


@dlt.table(name="dlt_bronze_transactions")
def bronze_transactions():
    return _raw("transactions")


@dlt.table(name="dlt_bronze_branches")
def bronze_branches():
    return _raw("branches")


# Silver: cleaned, typed, de-duplicated, with data quality expectations
dlt.create_streaming_table(
    "dlt_silver_customers",
    expect_all_or_drop={"valid_customer_id": "customer_id IS NOT NULL"},
)
dlt.apply_changes(
    target="dlt_silver_customers",
    source="dlt_bronze_customers",
    keys=["customer_id"],
    sequence_by=F.col("created_at"),
    stored_as_scd_type=1,
)

dlt.create_streaming_table(
    "dlt_silver_transactions",
    expect_all_or_drop={"valid_txn_id": "txn_id IS NOT NULL", "positive_amount": "amount > 0"},
)
dlt.apply_changes(
    target="dlt_silver_transactions",
    source="dlt_bronze_transactions",
    keys=["txn_id"],
    sequence_by=F.col("txn_timestamp"),
    stored_as_scd_type=1,
)


@dlt.table(name="dlt_silver_accounts")
@dlt.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
def silver_accounts():
    return (
        spark.read.table("dlt_bronze_accounts")
        .withColumn("balance", F.col("balance").cast("decimal(18,2)"))
        .withColumn("opened_at", F.to_date("opened_at"))
        .withColumn("status", F.upper("status"))
        .withColumn("account_type", F.upper("account_type"))
    )


# Gold: business aggregates and fraud signals
@dlt.table(name="dlt_gold_customer_monthly_spend")
def gold_customer_monthly_spend():
    txns = spark.read.table("dlt_silver_transactions")
    accounts = spark.read.table("dlt_silver_accounts").select("account_id", "customer_id")
    customers = spark.read.table("dlt_silver_customers").select("customer_id", "full_name", "city", "state")
    return (
        txns.groupBy(F.date_format("txn_timestamp", "yyyy-MM").alias("year_month"), "account_id")
        .agg(
            F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("total_debit"),
            F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("total_credit"),
            F.count(F.lit(1)).alias("txn_count"),
        )
        .join(accounts, "account_id", "left")
        .join(customers, "customer_id", "left")
    )


@dlt.table(name="dlt_gold_account_health")
def gold_account_health():
    return (
        spark.read.table("dlt_silver_accounts")
        .groupBy("account_type", "status")
        .agg(
            F.countDistinct("account_id").alias("accounts_count"),
            F.sum("balance").alias("total_balance"),
            F.avg("balance").alias("avg_balance"),
        )
    )


@dlt.table(name="dlt_gold_fraud_signals")
def gold_fraud_signals():
    w = Window.partitionBy("account_id").orderBy("txn_timestamp")
    return (
        spark.read.table("dlt_silver_transactions")
        .withColumn("minutes_since_prev", (F.col("txn_timestamp").cast("long") - F.lag("txn_timestamp").over(w).cast("long")) / 60.0)
        .withColumn("high_value_flag", (F.col("amount") >= 100000).cast("int"))
        .withColumn("rapid_repeat_flag", ((F.col("minutes_since_prev") >= 0) & (F.col("minutes_since_prev") <= 5)).cast("int"))
        .filter("high_value_flag = 1 OR rapid_repeat_flag = 1")
        .select("txn_id", "account_id", "txn_timestamp", "txn_type", "amount", "merchant", "high_value_flag", "rapid_repeat_flag")
    )
