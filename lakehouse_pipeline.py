from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

spark = SparkSession.builder \
    .appName("Retail Lakehouse Pipeline") \
    .getOrCreate()

# ================================
# 1️⃣ BRONZE → SILVER (CUSTOMERS)
# ================================

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

customers_raw_path = "gs://retail-chetan-dev-raw/customers/load_date=2026-02-21/*.csv"

customers_df = spark.read \
    .option("header", "true") \
    .schema(customer_schema) \
    .csv(customers_raw_path)

customers_df = customers_df.withColumn("load_date", current_date())

# Write SILVER to GCS (Parquet)
customers_df.write \
    .mode("overwrite") \
    .parquet("gs://retail-chetan-dev-curated/silver/customers/")

# ================================
# 2️⃣ BRONZE → SILVER (ORDERS)
# ================================

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", DoubleType(), True)
])

orders_raw_path = "gs://retail-chetan-dev-raw/orders/load_date=2026-02-20/*.csv"

orders_df = spark.read \
    .option("header", "true") \
    .schema(order_schema) \
    .csv(orders_raw_path)

orders_df = orders_df.withColumn("created_at", current_timestamp())

orders_df.write \
    .mode("overwrite") \
    .parquet("gs://retail-chetan-dev-curated/silver/orders/")

# ================================
# 3️⃣ SILVER → GOLD (DIM CUSTOMER)
# ================================

silver_customers = spark.read.parquet(
    "gs://retail-chetan-dev-curated/silver/customers/"
)

dim_customer_df = silver_customers.select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("city"),
    col("state"),
    current_date().alias("effective_start_date"),
    lit(None).cast(DateType()).alias("effective_end_date"),
    lit(True).alias("is_current"),
    current_timestamp().alias("created_at")
)

dim_customer_df.write \
    .mode("overwrite") \
    .parquet("gs://retail-chetan-dev-curated/gold/dim_customer/")

# ================================
# 4️⃣ SILVER → GOLD (FACT ORDERS)
# ================================

silver_orders = spark.read.parquet(
    "gs://retail-chetan-dev-curated/silver/orders/"
)

fact_orders_df = silver_orders.select(
    col("order_id"),
    col("customer_id"),
    col("product_id"),
    col("order_date"),
    col("quantity"),
    col("total_amount"),
    col("created_at")
)

fact_orders_df.write \
    .mode("overwrite") \
    .parquet("gs://retail-chetan-dev-curated/gold/fact_orders/")

spark.stop()