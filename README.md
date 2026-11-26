# Databricks Banking Data Pipeline (Free Account Friendly)

This project helps you learn an end-to-end ETL data pipeline in Databricks using a banking domain sample.

## What You Get

- Raw sample data in CSV format (customers, accounts, transactions, branches)
- ETL code in PySpark
- Medallion architecture layers:
  - Bronze: raw ingestion to Delta tables
  - Silver: cleaned and standardized tables
  - Gold: business-ready aggregates and fraud signals
- SQL views for reporting

## Project Structure

- data/raw/customers.csv
- data/raw/accounts.csv
- data/raw/transactions.csv
- data/raw/branches.csv
- src/ingest_bronze.py
- src/transform_silver.py
- src/build_gold.py
- src/run_pipeline.py
- sql/gold_views.sql

## ETL Flow

1. Bronze
   - Read CSV files from DBFS path
   - Write to Delta tables in database banking_pipeline

2. Silver
   - Type casting and formatting
   - De-duplication logic
   - Signed transaction amount calculation

3. Gold
   - Monthly customer spend metrics
   - Account health summary
   - Fraud signal detection:
     - high-value transaction >= 100000
     - rapid repeats within 5 minutes

## Deploy with Databricks Asset Bundles

Layout: `databricks.yml` (targets dev/staging/prod), `resources/` (schema, volume, job), `src/` (ETL), `scripts/deploy.ps1`.

Objects created: catalog `prod_dab`, schema `schema_dab` (dev/staging use `schema_dab_dev` / `schema_dab_stg`),
volume `banking_volume` with CSVs in `/Volumes/<catalog>/<schema>/banking_volume/raw/`, and job `banking-medallion-etl-<target>`.

1. Install the Databricks CLI (>= 0.250) and run `databricks auth login --host <workspace-url>`.
2. Replace the `<...-workspace>` hosts in `databricks.yml`.
3. Run `./scripts/deploy.ps1 -Target dev` (or `staging` / `prod`). It creates the catalog, deploys the bundle,
   uploads `data/raw/*.csv` to the volume, and runs the job.

Job tasks: bronze_ingest -> silver_transform -> gold_build -> gold_views.
## Example Validation Queries

Run these in a SQL notebook cell:

SELECT * FROM prod_dab.schema_dab.gold_customer_monthly_spend ORDER BY total_debit DESC;

SELECT * FROM prod_dab.schema_dab.gold_fraud_signals ORDER BY txn_timestamp DESC;

SELECT * FROM prod_dab.schema_dab.gold_account_health;

## Learn Next

- Add incremental loading with Auto Loader
- Add data quality checks (expectations)
- Schedule pipeline as Databricks job
- Connect Power BI/Tableau to Gold layer

## Troubleshooting

- Error: path not found
  - Check DBFS upload location exactly matches dbfs:/FileStore/banking/raw/

- Error: table not found
  - Run Bronze first, then Silver, then Gold

- Import errors in run_pipeline.py
  - Ensure all src files are in same folder in Databricks Repo



## Lakeflow Declarative Pipeline

esources/pipeline.yml deploys a serverless pipeline (src/pipeline/medallion_pipeline.py): Auto Loader bronze, de-duplicated silver with expectations, gold aggregates and fraud signals. Tables are prefixed dlt_ in the same schema. Run with databricks bundle run banking_medallion_pipeline -t <target> or the job anking_dlt_job.

