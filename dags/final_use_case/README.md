Needed:

 - Change project_id, bucket and region from `composer_dag.py`
 - Include `composer_dag.py` in dags folder from Airflow
 - Upload `sales_data.csv` to `gs://$BUCKET/raw/` folder
 - Upload `dataflow_transform_udf.py` to `gs://$BUCKET/`
 - Create BigQuery dataset called *sales_data*