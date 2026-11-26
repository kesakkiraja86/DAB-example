from common import get_args, get_spark

VIEWS = {
    "vw_top_debit_customers": (
        "SELECT year_month, customer_id, full_name, city, state, total_debit, total_credit, txn_count "
        "FROM {db}.gold_customer_monthly_spend ORDER BY total_debit DESC"
    ),
    "vw_fraud_watchlist": (
        "SELECT txn_id, account_id, txn_timestamp, txn_type, amount, merchant, high_value_flag, rapid_repeat_flag "
        "FROM {db}.gold_fraud_signals ORDER BY txn_timestamp DESC"
    ),
    "vw_account_health": (
        "SELECT account_type, status, accounts_count, total_balance, avg_balance "
        "FROM {db}.gold_account_health ORDER BY account_type, status"
    ),
}

if __name__ == "__main__":
    args = get_args()
    spark = get_spark()
    db = f"{args.catalog}.{args.schema}"
    for name, query in VIEWS.items():
        spark.sql(f"CREATE OR REPLACE VIEW {db}.{name} AS {query.format(db=db)}")
        print(f"Created view {db}.{name}")
