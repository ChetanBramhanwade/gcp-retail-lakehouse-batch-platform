from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date, col
from pyspark.sql.types import StructType, StructField, StringType

# Create Spark session
spark = SparkSession.builder \
    .appName("Load Customers to Staging") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

# Read from GCS raw zone
# input_path = "gs://retail-chetan-dev-raw/customers/load_date=2026-02-20/*.csv"
input_path = "gs://retail-chetan-dev-raw/customers/load_date=2026-02-21/*.csv"

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

# Add load_date as DATE (NOT STRING)
# df = df.withColumn(
#     "load_date",
#     to_date(lit("2026-02-20"), "yyyy-MM-dd")
# )

df = df.withColumn(
    "load_date",
    to_date(lit("2026-02-21"), "yyyy-MM-dd")
)

# Write to BigQuery staging table
df.write \
  .format("bigquery") \
  .option("table", "retail_dev_staging.stg_customers") \
  .option("temporaryGcsBucket", "retail-chetan-dev-temp") \
  .mode("append") \
  .save()

spark.stop()