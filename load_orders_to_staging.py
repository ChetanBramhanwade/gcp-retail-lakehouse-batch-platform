from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Load Orders to Staging") \
    .getOrCreate()

# Define schema (raw CSV schema)
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("total_amount", StringType(), True)
])

# Read raw CSV
input_path = "gs://retail-chetan-dev-raw/orders/load_date=2026-02-20/*.csv"

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

# Type casting (Enterprise rule: always enforce schema)
df = df \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("quantity", col("quantity").cast(IntegerType())) \
    .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
    .withColumn("load_date", to_date(lit("2026-02-20"), "yyyy-MM-dd"))

# Write to BigQuery staging
df.write \
    .format("bigquery") \
    .option("table", "retail_dev_staging.stg_orders") \
    .option("temporaryGcsBucket", "retail-chetan-dev-temp") \
    .mode("append") \
    .save()

spark.stop()