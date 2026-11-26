from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_gold(spark: SparkSession, database: str = "banking_pipeline") -> None:
    customers = spark.table(f"{database}.silver_customers")
    accounts = spark.table(f"{database}.silver_accounts")
    transactions = spark.table(f"{database}.silver_transactions")

    customer_spend = (
        transactions.groupBy(
            F.date_format(F.col("txn_timestamp"), "yyyy-MM").alias("year_month"),
            F.col("account_id"),
        )
        .agg(
            F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(F.lit(0))).alias("total_debit"),
            F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(F.lit(0))).alias("total_credit"),
            F.count(F.lit(1)).alias("txn_count"),
        )
        .join(accounts.select("account_id", "customer_id"), on="account_id", how="left")
        .join(customers.select("customer_id", "full_name", "city", "state"), on="customer_id", how="left")
    )

    account_health = (
        accounts.groupBy("account_type", "status")
        .agg(
            F.countDistinct("account_id").alias("accounts_count"),
            F.sum("balance").alias("total_balance"),
            F.avg("balance").alias("avg_balance"),
        )
        .orderBy("account_type", "status")
    )

    txn_window = Window.partitionBy("account_id").orderBy(F.col("txn_timestamp"))
    fraud_signals = (
        transactions.withColumn("prev_txn_time", F.lag("txn_timestamp").over(txn_window))
        .withColumn("minutes_since_prev", (F.col("txn_timestamp").cast("long") - F.col("prev_txn_time").cast("long")) / 60.0)
        .withColumn("high_value_flag", F.when(F.col("amount") >= F.lit(100000), F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "rapid_repeat_flag",
            F.when((F.col("minutes_since_prev") >= 0) & (F.col("minutes_since_prev") <= 5), F.lit(1)).otherwise(F.lit(0)),
        )
        .filter((F.col("high_value_flag") == 1) | (F.col("rapid_repeat_flag") == 1))
        .select("txn_id", "account_id", "txn_timestamp", "txn_type", "amount", "merchant", "high_value_flag", "rapid_repeat_flag")
    )

    save(customer_spend, f"{database}.gold_customer_monthly_spend")
    save(account_health, f"{database}.gold_account_health")
    save(fraud_signals, f"{database}.gold_fraud_signals")


if __name__ == "__main__":
    args = get_args()
    build_gold(get_spark(), args.catalog, args.schema)
