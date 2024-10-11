from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
# from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyDatasetOperator
from airflow.utils.dates import days_ago


# Define your GCP project ID, GCS bucket, and BigQuery dataset
PROJECT_ID = "qwiklabs-gcp-02-d63aac9730df"
BUCKET_NAME = "qwiklabs-gcp-02-d63aac9730df"
REGION = "us-east4"

DATASET_NAME = "sales_data"
TABLE_NAME = "sales_raw"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='A DAG to process sales data',
    schedule_interval=None,
)

create_bucket = GCSCreateBucketOperator(
    task_id="create_bucket",
    bucket_name=BUCKET_NAME,
    project_id=PROJECT_ID,
    location="US",
    dag=dag,
)

# upload_to_gcs = LocalFilesystemToGCSOperator(
#     task_id="upload_to_gcs",
#     src="/path/to/your/local/sales_data.csv",
#     dst="raw/sales_data.csv",
#     bucket=BUCKET_NAME,
#     dag=dag,
# )

create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_bigquery_dataset",
    dataset_id=DATASET_NAME,
    project_id=PROJECT_ID,
    location=REGION,
    exists_ok=True,  # This will not raise an error if the dataset already exists
    dag=dag,
)

run_dataflow = DataflowCreatePythonJobOperator(
    task_id="run_dataflow",
    py_file=f"gs://{BUCKET_NAME}/dataflow_transform_udf.py",
    job_name="sales_data_to_bigquery",
    options={
        "input": f"gs://{BUCKET_NAME}/raw/sales_data.csv",
        "output": f"{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}",
        "temp_location": f"gs://{BUCKET_NAME}/tmp/",
        "region": REGION,
        # "setup_file": f"gs://{BUCKET_NAME}/dataflow_setup.py",
    },
    py_options=[],
    dag=dag,
)

create_materialized_view = BigQueryExecuteQueryOperator(
    task_id="create_materialized_view",
    sql=f"""
    CREATE MATERIALIZED VIEW `{PROJECT_ID}.{DATASET_NAME}.sales_by_category` AS
    SELECT
      category,
      DATE(date) as sale_date,
      SUM(total_sales) as daily_sales
    FROM
      `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    GROUP BY
      category, sale_date
    """,
    use_legacy_sql=False,
    dag=dag,
)

append_columns = BigQueryExecuteQueryOperator(
    task_id="append_columns",
    sql=f"""
    ALTER TABLE `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    ADD COLUMN IF NOT EXISTS profit FLOAT64,
    ADD COLUMN IF NOT EXISTS profit_margin FLOAT64;

    UPDATE `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    SET
      profit = total_sales * 0.2,
      profit_margin = (total_sales * 0.2) / total_sales
    WHERE
      profit IS NULL;
    """,
    use_legacy_sql=False,
    dag=dag,
)

create_summary = BigQueryExecuteQueryOperator(
    task_id="create_summary",
    sql=f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.sales_summary` AS
    SELECT
      category,
      SUM(total_sales) as total_sales,
      SUM(profit) as total_profit,
      AVG(profit_margin) as avg_profit_margin
    FROM
      `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    GROUP BY
      category
    """,
    use_legacy_sql=False,
    dag=dag,
)

data_quality_check = BigQueryExecuteQueryOperator(
    task_id="data_quality_check",
    sql=f"""
    SELECT
      CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
      END as quality_check_result,
      'No negative sales' as check_name
    FROM
      `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    WHERE
      total_sales < 0

    UNION ALL

    SELECT
      CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
      END as quality_check_result,
      'No future dates' as check_name
    FROM
      `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    WHERE
      DATE(date) > CURRENT_DATE()
    """,
    use_legacy_sql=False,
    dag=dag,
)

generate_sales_report = BigQueryExecuteQueryOperator(
    task_id="generate_sales_report",
    sql=f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.sales_report` AS
    SELECT
      DATE_TRUNC(date, MONTH) as month,
      category,
      SUM(total_sales) as monthly_sales,
      AVG(profit_margin) as avg_profit_margin
    FROM
      `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`
    GROUP BY
      month, category
    ORDER BY
      month, monthly_sales DESC
    """,
    use_legacy_sql=False,
    dag=dag,
)

# Define the task dependencies
create_bucket >> create_bigquery_dataset >> run_dataflow
run_dataflow >> create_materialized_view >> append_columns >> create_summary
run_dataflow >> data_quality_check
run_dataflow >> generate_sales_report