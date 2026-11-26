from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_silver(spark: SparkSession, database: str = "banking_pipeline") -> None:
    customers = spark.table(f"{database}.bronze_customers")
    accounts = spark.table(f"{database}.bronze_accounts")
    transactions = spark.table(f"{database}.bronze_transactions")
    branches = spark.table(f"{database}.bronze_branches")

    customer_window = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc())
    silver_customers = (
        customers.withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("dob", F.to_date("dob"))
        .withColumn("full_name", F.trim("full_name"))
        .withColumn("rn", F.row_number().over(customer_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    silver_accounts = (
        accounts.withColumn("balance", F.col("balance").cast("decimal(18,2)"))
        .withColumn("opened_at", F.to_date("opened_at"))
        .withColumn("status", F.upper("status"))
        .withColumn("account_type", F.upper("account_type"))
    )

    txn_window = Window.partitionBy("txn_id").orderBy(F.col("txn_timestamp").desc())
    silver_transactions = (
        transactions.withColumn("txn_timestamp", F.to_timestamp("txn_timestamp"))
        .withColumn("txn_type", F.upper("txn_type"))
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("rn", F.row_number().over(txn_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn(
            "signed_amount",
            F.when(F.col("txn_type") == F.lit("DEBIT"), -F.col("amount")).otherwise(F.col("amount")),
        )
    )

    silver_branches = branches.select(
        F.col("branch_id"),
        F.trim(F.col("branch_name")).alias("branch_name"),
        F.upper(F.col("state")).alias("state"),
        F.trim(F.col("city")).alias("city"),
        F.col("ifsc_code"),
    )

    save(silver_customers, f"{database}.silver_customers")
    save(silver_accounts, f"{database}.silver_accounts")
    save(silver_transactions, f"{database}.silver_transactions")
    save(silver_branches, f"{database}.silver_branches")


if __name__ == "__main__":
    args = get_args()
    build_silver(get_spark(), args.catalog, args.schema)
