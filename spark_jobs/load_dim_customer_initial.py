from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Initial Load dim_customer SCD2") \
    .getOrCreate()

# Read staging customers from BigQuery
stg_df = spark.read \
    .format("bigquery") \
    .option("table", "retail_dev_staging.stg_customers") \
    .load()

# Generate surrogate key using row_number
window_spec = Window.orderBy("customer_id")

dim_df = stg_df \
    .withColumn("customer_sk", row_number().over(window_spec)) \
    .withColumn("effective_start_date", col("load_date")) \
    .withColumn("effective_end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("created_at", current_timestamp())

# Reorder columns to match table
dim_df = dim_df.select(
    "customer_sk",
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "state",
    "effective_start_date",
    "effective_end_date",
    "is_current",
    "created_at"
)

# Write to curated dim_customer
dim_df.write \
    .format("bigquery") \
    .option("table", "retail_dev_dw.dim_customer") \
    .option("temporaryGcsBucket", "retail-chetan-dev-temp") \
    .mode("append") \
    .save()

spark.stop()