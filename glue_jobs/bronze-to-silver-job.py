import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, to_date, when, lower, trim

sc = SparkContext()
spark = SparkSession(sc)

# ---------------------------
# Schemas
# ---------------------------
order_items_schema = StructType([
    StructField("brand", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_ts_raw", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("loyalty_id", StringType(), True),
    StructField("is_reward", BooleanType(), True),
    StructField("currency", StringType(), True),
    StructField("menu_item_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("price", DecimalType(10,2), True),
    StructField("qty", IntegerType(), True)
])

order_item_options_schema = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("menu_item_id", StringType(), True),
    StructField("option_group", StringType(), True),
    StructField("option_name", StringType(), True),
    StructField("option_price", DecimalType(10,2), True),
    StructField("option_qty", IntegerType(), True)
])

# For raw read, booleans as STRING (cast later)
date_dim_raw_schema = StructType([
    StructField("date_str", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("week_of_year", IntegerType(), True),
    StructField("weekday_name", StringType(), True),
    StructField("is_weekend", StringType(), True),
    StructField("is_holiday", StringType(), True),
    StructField("holiday_name", StringType(), True)
])

# ---------------------------
# Paths
# ---------------------------
bronze_prefix = "s3://restaurant-analytics-data/bronze/bronze/"
silver_prefix = "s3://restaurant-analytics-data/silver/"

# ---------------------------
# Read Bronze → Transform → Write Silver
# ---------------------------

# Order Items
order_items_df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv(
        bronze_prefix + "raw_order_items/",
        schema=order_items_schema,
        header=False,
        sep=","
    )

# Derive timestamp + date
order_items_df = order_items_df \
    .withColumn("order_ts", to_timestamp(col("order_ts_raw"))) \
    .withColumn("order_date", to_date(col("order_ts")))

order_items_df.write.mode("overwrite") \
    .partitionBy("order_date") \
    .parquet(silver_prefix + "order_items/")

# Order Item Options
order_item_options_df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv(
        bronze_prefix + "raw_order_item_options/",
        schema=order_item_options_schema,
        header=False,
        sep=","
    )

order_item_options_df.write.mode("overwrite") \
    .parquet(silver_prefix + "order_item_options/")

# Date Dim
date_dim_df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv(
        bronze_prefix + "raw_date_dim/",
        schema=date_dim_raw_schema,
        header=False,
        sep=","
    )

# ✅ Robust boolean casting (fix missing rows issue)
date_dim_df = date_dim_df \
    .withColumn("is_weekend", when(lower(trim(col("is_weekend"))) == "true", True).otherwise(False)) \
    .withColumn("is_holiday", when(lower(trim(col("is_holiday"))) == "true", True).otherwise(False))

date_dim_df.write.mode("overwrite") \
    .parquet(silver_prefix + "date_dim/")
