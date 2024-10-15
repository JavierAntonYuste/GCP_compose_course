# Final Use Case

## Lab Overview:
In this lab, you will build a Google Cloud Composer pipeline that integrates with Dataflow and BigQuery to perform data processing and analysis. The pipeline will demonstrate the following workflow:

1. Extract a file from Google Cloud Storage (GCS)
2. Use Dataflow to transform and prepare the data
3. Load the processed data into BigQuery
4. Perform data analysis and aggregation in BigQuery

## Lab Objectives:
By completing this lab, you will learn how to:

* Design and implement a Composer pipeline to integrate with Dataflow and BigQuery
* Use Dataflow to transform and process data
* Load and analyze data in BigQuery
* Perform data aggregation and calculation in BigQuery

## Introduction:
This lab requires you to set up a Google Cloud Composer environment and configure the necessary files and datasets. Follow the steps below carefully to complete the setup.

## Step 1: Configure the DAG Script
Open the `composer_dag.py` file and update the following values:

* `PROJECT_ID`: Replace with your Google Cloud project ID
* `BUCKET_NAME`: Replace with the name of your Google Cloud Storage bucket
* `REGION`: Replace with the region where your Composer environment is located
* Save the changes to the `composer_dag.py` file

## Step 2: Deploy the DAG Script
* Copy the `composer_dag.py` file to the `dags` folder in your Airflow environment
* Ensure that the file is uploaded successfully and visible in the Airflow UI

## Step 3: Upload Required Files to Cloud Storage
Upload the following files:
* `sales_data.csv` to the `gs://$BUCKET_NAME/raw/` folder in your Cloud Storage bucket
* `dataflow_transform_udf.py` to the root of your Cloud Storage bucket (`gs://$BUCKET_NAME/`)

## Verification
Once you have completed the above steps, verify that:

* The `composer_dag.py` file is deployed successfully in Airflow
* The `sales_data.csv` and `dataflow_transform_udf.py` files are uploaded successfully to Cloud Storage
