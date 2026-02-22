# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# PROJECT_ID = "retail-batch-dev"
# REGION = "us-central1"
# CLUSTER_NAME = "retail-dev-cluster"

# default_args = {
#     "start_date": days_ago(1),
# }

# with DAG(
#     dag_id="retail_batch_pipeline",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=["retail", "batch", "bigquery"],
# ) as dag:

#     # 1️⃣ Spark Job (Bronze → Silver)
#     spark_task = DataprocSubmitJobOperator(
#         task_id="load_customers_to_staging",
#         job={
#             "placement": {"cluster_name": CLUSTER_NAME},
#             "pyspark_job": {
#                 "main_python_file_uri": "gs://retail-chetan-dev-temp/scripts/load_customers_to_staging.py",
#             },
#         },
#         region=REGION,
#         project_id=PROJECT_ID,
#     )

#     # 2️⃣ SCD2 Merge (Silver → Gold Dimension)
#     scd2_task = BigQueryInsertJobOperator(
#         task_id="run_scd2_merge",
#         configuration={
#             "query": {
#                 "query": """
#                 MERGE retail_dev_dw.dim_customer T
#                 USING retail_dev_staging.stg_customers_latest S
#                 ON T.customer_id = S.customer_id AND T.is_current = TRUE

#                 WHEN MATCHED AND (
#                     T.first_name != S.first_name OR
#                     T.last_name != S.last_name OR
#                     T.email != S.email OR
#                     T.city != S.city OR
#                     T.state != S.state
#                 )
#                 THEN UPDATE SET
#                     effective_end_date = CURRENT_DATE(),
#                     is_current = FALSE

#                 WHEN NOT MATCHED THEN
#                 INSERT (
#                     customer_sk,
#                     customer_id,
#                     first_name,
#                     last_name,
#                     email,
#                     city,
#                     state,
#                     effective_start_date,
#                     effective_end_date,
#                     is_current,
#                     created_at
#                 )
#                 VALUES (
#                     FARM_FINGERPRINT(S.customer_id),
#                     S.customer_id,
#                     S.first_name,
#                     S.last_name,
#                     S.email,
#                     S.city,
#                     S.state,
#                     CURRENT_DATE(),
#                     NULL,
#                     TRUE,
#                     CURRENT_TIMESTAMP()
#                 )
#                 """,
#                 "useLegacySql": False,
#             }
#         },
#         location="US",
#     )

#     # 3️⃣ Fact Load (FIXED VERSION)
#     fact_task = BigQueryInsertJobOperator(
#         task_id="load_fact_orders",
#         configuration={
#             "query": {
#                 "query": """
#                 INSERT INTO retail_dev_dw.fact_orders (
#                     order_id,
#                     customer_id,
#                     product_id,
#                     order_date,
#                     quantity,
#                     total_amount,
#                     created_at,
#                     customer_sk
#                 )
#                 SELECT
#                     order_id,
#                     customer_id,
#                     product_id,
#                     order_date,
#                     quantity,
#                     total_amount,
#                     CURRENT_TIMESTAMP(),
#                     FARM_FINGERPRINT(customer_id)
#                 FROM retail_dev_staging.stg_orders
#                 """,
#                 "useLegacySql": False,
#             }
#         },
#         location="US",
#     )

#     # Task Dependency
#     spark_task >> scd2_task >> fact_task




from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PROJECT_ID = "retail-batch-dev"
REGION = "us-central1"
CLUSTER_NAME = "retail-dev-cluster"

default_args = {
    "start_date": days_ago(1),
}

with DAG(
    dag_id="retail_lakehouse_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    lakehouse_spark_job = DataprocSubmitJobOperator(
        task_id="run_lakehouse_pipeline",
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": "gs://retail-chetan-dev-temp/scripts/lakehouse_pipeline.py",
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    lakehouse_spark_job
