import sys
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("silver_to_gold_all_final").getOrCreate()

S3 = "s3://restaurant-analytics-data"
DEC = T.DecimalType(31, 2)

def d2(expr):  # money-safe rounding -> DECIMAL(31,2)
    return F.round(expr, 2).cast(DEC)

def log(df, name):
    print("\n" + "="*80)
    print(f"DF: {name}")
    df.printSchema()
    print(f"Rows: {df.count()}")
    df.show(10, truncate=False)
    print("="*80 + "\n")

# ------------------------------------------------------------------
# Load silver
# ------------------------------------------------------------------
order_items = spark.read.parquet(f"{S3}/silver/order_items/")
order_item_options = spark.read.parquet(f"{S3}/silver/order_item_options/")  # not used below but fine to keep
date_dim = spark.read.parquet(f"{S3}/silver/date_dim/")

# Required columns
required = {
    "brand","customer_id","order_id","menu_item_id","item_name","category",
    "price","qty","order_date"
}
missing = [c for c in required if c not in order_items.columns]
if missing:
    raise Exception(f"Missing in silver/order_items: {missing}")

# Normalize & clean (drop rows with nulls that break rollups)
oi = (
    order_items
      .filter(
          F.col("menu_item_id").isNotNull() &
          F.col("item_name").isNotNull() &
          F.col("category").isNotNull() &
          F.col("price").isNotNull() &
          F.col("qty").isNotNull()
      )
      .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
      .withColumn("unit_price", F.col("price").cast(DEC))
      .withColumn("quantity", F.col("qty").cast("bigint"))
      .withColumn("line_revenue", d2(F.col("price") * F.col("qty")))
)

log(oi, "silver.order_items (clean)")

# Join date attributes for other gold tables
dd = date_dim.select(
    F.to_date("date_str", "yyyy-MM-dd").alias("dd_date"),
    "year","month","week_of_year","weekday_name","is_weekend","is_holiday","holiday_name"
)
df = oi.join(dd, oi.order_date == dd.dd_date, "left")
log(df, "order_items + date_dim")

# ------------------------- Gold: fact_orders ----------------------
fact_orders = df.select(
    "order_id","customer_id", F.col("brand").alias("store_name"),
    F.col("menu_item_id").alias("item_id"), "item_name","category",
    F.col("unit_price").cast(DEC).alias("unit_price"),
    F.col("quantity").alias("quantity"),
    d2("line_revenue").alias("total_sales"),
    "order_date","year","month","week_of_year","weekday_name",
    "is_weekend","is_holiday","holiday_name"
)
fact_orders.write.mode("overwrite").parquet(f"{S3}/gold/fact_orders/")

# --------------------- Gold: sales_summary ------------------------
sales_summary = (
    df.groupBy("brand","order_date","item_name")
      .agg(
          F.sum("quantity").cast("bigint").alias("total_quantity"),
          d2(F.sum("line_revenue")).alias("total_sales")
      )
)
sales_summary.write.mode("overwrite").parquet(f"{S3}/gold/sales_summary/")

# ----------------- Gold: item_performance  ✅ ---------------------
item_performance = (
    oi.groupBy("menu_item_id","item_name","category")
      .agg(
          F.sum("quantity").cast("bigint").alias("total_quantity_sold"),
          d2(F.sum("line_revenue")).alias("total_revenue")
      )
      .withColumnRenamed("menu_item_id","item_id")
)
log(item_performance, "gold.item_performance (preview)")
item_performance.write.mode("overwrite").parquet(f"{S3}/gold/item_performance/")

# ------------------- Gold: customer_metrics -----------------------
customer_metrics = (
    df.groupBy("customer_id")
      .agg(
          F.countDistinct("order_id").alias("total_orders"),
          d2(F.sum("line_revenue")).alias("lifetime_value")
      )
      .withColumn(
          "avg_order_value",
          F.when(F.col("total_orders") > 0,
                 d2(F.col("lifetime_value") / F.col("total_orders"))).otherwise(d2(F.lit(0)))
      )
)
customer_metrics.write.mode("overwrite").parquet(f"{S3}/gold/customer_metrics/")

# --------------------- Gold: weekly_trends ------------------------
weekly_trends = (
    df.groupBy("brand","week_of_year")
      .agg(
          F.sum("quantity").cast("bigint").alias("weekly_quantity"),
          d2(F.sum("line_revenue")).alias("weekly_sales")
      )
)
weekly_trends.write.mode("overwrite").parquet(f"{S3}/gold/weekly_trends/")

# ----------------- Gold: holiday_vs_nonholiday --------------------
holiday_vs_nonholiday = (
    df.groupBy("is_holiday")
      .agg(
          F.sum("quantity").cast("bigint").alias("total_quantity"),
          d2(F.sum("line_revenue")).alias("total_sales")
      )
)
holiday_vs_nonholiday.write.mode("overwrite").parquet(f"{S3}/gold/holiday_vs_nonholiday/")

print("✅ GOLD LAYER ETL COMPLETE")
