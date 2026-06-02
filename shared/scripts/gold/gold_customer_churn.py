import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, BooleanType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# SPARK SESSION

spark = SparkSession.builder \
    .appName("Olist_Gold_Customer_Churn") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key",        "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

log.info("Spark session started — connected to MinIO")

# POSTGRESQL CONNECTION

PG_URL   = "jdbc:postgresql://postgres-dw:5432/sessiondb"
PG_PROPS = {
    "user":     "admin",
    "password": "admin",
    "driver":   "org.postgresql.Driver"
}
SCHEMA = "customer_churn"

# HELPER FUNCTIONS

def read_silver(name):
    path = f"s3a://silver/olist/{name}"
    log.info(f"Reading silver: {path}")
    return spark.read.parquet(path)

def write_pg(df, table):
    full_table = f"{SCHEMA}.{table}"
    count = df.count()
    log.info(f"Writing to PostgreSQL: {full_table}  rows={count}")
    df.write.jdbc(url=PG_URL, table=full_table, mode="append", properties=PG_PROPS)
    log.info(f"Done: {full_table}")

def write_gold_minio(df, name):
    path = f"s3a://gold/olist/customer_churn/{name}"
    df.write.mode("overwrite").parquet(path)
    log.info(f"Written to MinIO gold: {path}")

# AUTO TRUNCATE
# Fact tables first, then dims, respects foreign key constraints
# RESTART IDENTITY resets surrogate key counters back to 1

log.info("Truncating all customer_churn tables")
from py4j.java_gateway import java_import
java_import(spark._jvm, "java.sql.DriverManager")
conn = spark._jvm.DriverManager.getConnection(PG_URL, "admin", "admin")
stmt = conn.createStatement()
for table in [
    "fct_churn_summary",
    "fct_customer_orders",
    "dim_customer_profile",
    "dim_product",
    "dim_date",
]:
    stmt.execute(
        f"TRUNCATE TABLE customer_churn.{table} RESTART IDENTITY CASCADE"
    )
conn.close()
log.info("All customer_churn tables truncated successfully")

# READ SILVER

log.info("Reading silver tables")
customers   = read_silver("customers")
orders      = read_silver("orders")
order_items = read_silver("order_items")
payments    = read_silver("order_payments")
reviews     = read_silver("order_reviews")
products    = read_silver("products")
translation = read_silver("product_category_translation")
geo         = read_silver("geolocation")

# GEOLOCATION LOOKUP

geo_avg = geo.groupBy("geolocation_zip_code_prefix").agg(
    F.round(F.avg("geolocation_lat"), 6).alias("latitude"),
    F.round(F.avg("geolocation_lng"), 6).alias("longitude")
)

# DATASET END DATE — DYNAMIC
# Computed from data
# Churn threshold: 180 days of silence = churned

dataset_end = orders.select(
    F.max(F.to_date("order_purchase_timestamp")).alias("end_date")
).collect()[0]["end_date"]

log.info(f"Dataset end date: {dataset_end}")
CHURN_THRESHOLD_DAYS = 180

# DIM DATE
# Uses purchase_timestamp for both min and max
# churn datamart only cares about when orders were placed

log.info("Building dim_date")
date_bounds = orders.select(
    F.min(F.to_date("order_purchase_timestamp")).alias("min_date"),
    F.max(F.to_date("order_purchase_timestamp")).alias("max_date")
).collect()[0]

min_date = str(date_bounds["min_date"])
max_date = str(date_bounds["max_date"])

date_df = spark.sql(f"""
    SELECT sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    ) AS date_array
""").withColumn("full_date", F.explode(F.col("date_array"))).drop("date_array")

dim_date = date_df.select(
    F.col("full_date"),
    F.dayofmonth("full_date").alias("day_number"),
    F.date_format("full_date", "EEEE").alias("day_name"),
    F.weekofyear("full_date").alias("week_number"),
    F.month("full_date").alias("month_number"),
    F.date_format("full_date", "MMMM").alias("month_name"),
    F.quarter("full_date").alias("quarter_number"),
    F.year("full_date").alias("year_number"),
    F.when(F.dayofweek("full_date").isin(1, 7), True)
     .otherwise(False).alias("is_weekend"),
    F.when(F.dayofmonth("full_date") == 1, True)
     .otherwise(False).alias("is_month_start"),
    F.when(
        F.dayofmonth("full_date") == F.dayofmonth(F.last_day("full_date")), True
    ).otherwise(False).alias("is_month_end")
)

write_pg(dim_date, "dim_date")
write_gold_minio(dim_date, "dim_date")

dim_date_pg = spark.read.jdbc(
    url=PG_URL, table=f"{SCHEMA}.dim_date", properties=PG_PROPS
)
log.info(f"dim_date reloaded: {dim_date_pg.count()} rows")

# DIM PRODUCT
# Independent copy in customer_churn schema

log.info("Building dim_product")
dim_product = products.join(
    translation,
    products["product_category_name"] == translation["product_category_name"],
    how="left"
).select(
    products["product_id"],
    products["product_category_name"],
    translation["product_category_name_english"].alias("product_category_english"),
    products["product_weight_g"],
    products["product_length_cm"],
    products["product_height_cm"],
    products["product_width_cm"],
    products["product_volume_cm3"],
    products["product_photos_qty"],
    products["product_name_length"],
    products["product_description_length"],
    F.when(F.col("product_volume_cm3") <= 1000,  "small")
     .when(F.col("product_volume_cm3") <= 10000, "medium")
     .when(F.col("product_volume_cm3") <= 50000, "large")
     .otherwise("extra_large").alias("logistics_size_category"),
    F.when(F.col("product_weight_g") <= 500,   "light")
     .when(F.col("product_weight_g") <= 2000,  "medium")
     .when(F.col("product_weight_g") <= 10000, "heavy")
     .otherwise("very_heavy").alias("logistics_weight_category"),
    F.current_timestamp().alias("created_at")
)

write_pg(dim_product, "dim_product")
write_gold_minio(dim_product, "dim_product")

dim_product_pg = spark.read.jdbc(
    url=PG_URL, table=f"{SCHEMA}.dim_product", properties=PG_PROPS
)
log.info(f"dim_product reloaded: {dim_product_pg.count()} rows")

# ORDER LEVEL AGGREGATIONS
# Computed once upfront, reused across dim_customer_profile,
# fct_customer_orders, and fct_churn_summary

log.info("Computing order-level aggregations")

# Total payment value per order (an order can have multiple payment rows)
payments_agg = payments.groupBy("order_id").agg(
    F.round(F.sum("payment_value"), 2).alias("payment_value")
)

# Item count and distinct categories per order
items_with_cat = order_items.join(
    products.select("product_id", "product_category_name"),
    on="product_id", how="left"
)
items_agg = items_with_cat.groupBy("order_id").agg(
    F.count("order_item_id").cast(IntegerType()).alias("items_count"),
    F.countDistinct("product_category_name").cast(IntegerType()).alias("distinct_categories")
)

# Average review score per order
reviews_agg = reviews.groupBy("order_id").agg(
    F.round(F.avg("review_score"), 2).alias("review_score")
)

# Enriching orders with customer_unique_id, payment, items, reviews
orders_enriched = orders \
    .join(
        customers.select("customer_id", "customer_unique_id"),
        on="customer_id", how="left"
    ) \
    .join(payments_agg, on="order_id", how="left") \
    .join(items_agg,    on="order_id", how="left") \
    .join(reviews_agg,  on="order_id", how="left")

# Adding delivery_status per order
orders_enriched = orders_enriched \
    .withColumn("delay_days",
        F.when(F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date")
            )
        ).otherwise(None)) \
    .withColumn("delivery_status",
        F.when(F.col("order_delivered_customer_date").isNull(), "not_delivered")
         .when(F.col("delay_days") <= 0,  "on_time")
         .when(F.col("delay_days") <= 7,  "late")
         .otherwise("very_late"))

log.info("orders_enriched ready")

# CUSTOMER LEVEL METRICS
# Collapse all orders per unique customer into one row

log.info("Computing customer-level metrics")
customer_summary = orders_enriched.groupBy("customer_unique_id").agg(
    F.min("order_purchase_timestamp").alias("first_order_date"),
    F.max("order_purchase_timestamp").alias("last_order_date"),
    F.countDistinct("order_id").cast(IntegerType()).alias("total_orders"),
    F.round(F.sum("payment_value"), 2).alias("total_spend")
) \
.withColumn("avg_order_value",
    F.round(F.col("total_spend") / F.col("total_orders"), 2)) \
.withColumn("days_since_first_order",
    F.datediff(F.lit(dataset_end), F.to_date("first_order_date")).cast(IntegerType())) \
.withColumn("days_since_last_order",
    F.datediff(F.lit(dataset_end), F.to_date("last_order_date")).cast(IntegerType())) \
.withColumn("customer_segment",
    F.when(F.col("total_orders") == 1, "one_time")
     .when(F.col("days_since_last_order") <= CHURN_THRESHOLD_DAYS, "active")
     .when(F.col("days_since_last_order") >  CHURN_THRESHOLD_DAYS, "churned")
     .otherwise("churned")) \
.withColumn("churn_flag",
    F.when(F.col("customer_segment") == "churned", True)
     .otherwise(False).cast(BooleanType()))

# Second pass, upgrade active multi-buyers to loyal
customer_summary = customer_summary.withColumn(
    "customer_segment",
    F.when(
        (F.col("total_orders") > 1) &
        (F.col("days_since_last_order") <= CHURN_THRESHOLD_DAYS),
        "loyal"
    ).otherwise(F.col("customer_segment"))
)

log.info("customer_summary ready")

# DIM CUSTOMER PROFILE
# One row per customer_unique_id
# Deduplicates customers Silver table using row_number window function
# Silver has one row per customer_id, multiple IDs per unique customer

log.info("Building dim_customer_profile")
window_cust = Window.partitionBy("customer_unique_id").orderBy("customer_id")
customers_dedup = customers \
    .withColumn("rn", F.row_number().over(window_cust)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

customers_with_geo = customers_dedup.join(
    geo_avg,
    customers_dedup["customer_zip_code_prefix"] == geo_avg["geolocation_zip_code_prefix"],
    how="left"
)

dim_customer_profile = customers_with_geo.join(
    customer_summary,
    on="customer_unique_id",
    how="inner"
).select(
    F.col("customer_unique_id"),
    F.col("customer_city"),
    F.col("customer_state"),
    F.col("latitude"),
    F.col("longitude"),
    F.col("first_order_date"),
    F.col("last_order_date"),
    F.col("total_orders"),
    F.col("total_spend").alias("total_spend_brl"),
    F.col("avg_order_value").alias("avg_order_value_brl"),
    F.col("days_since_first_order"),
    F.col("days_since_last_order"),
    F.col("customer_segment"),
    F.col("churn_flag"),
    F.current_timestamp().alias("created_at")
)

write_pg(dim_customer_profile, "dim_customer_profile")
write_gold_minio(dim_customer_profile, "dim_customer_profile")

dim_customer_profile_pg = spark.read.jdbc(
    url=PG_URL, table=f"{SCHEMA}.dim_customer_profile", properties=PG_PROPS
)
log.info(f"dim_customer_profile reloaded: {dim_customer_profile_pg.count()} rows")

# FCT CUSTOMER ORDERS
# One row per order at the unique customer level
# Window functions compute order sequence and days between purchases

log.info("Building fct_customer_orders")
window_seq = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")
window_lag = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")

fct_orders = orders_enriched \
    .withColumn("order_sequence_number",
        F.row_number().over(window_seq).cast(IntegerType())) \
    .withColumn("prev_order_date",
        F.lag("order_purchase_timestamp", 1).over(window_lag)) \
    .withColumn("days_since_previous_order",
        F.when(
            F.col("prev_order_date").isNotNull(),
            F.datediff(
                F.col("order_purchase_timestamp"),
                F.col("prev_order_date")
            )
        ).otherwise(None).cast(IntegerType())) \
    .drop("prev_order_date")

# Join dim_customer_profile for surrogate key
fct_orders = fct_orders.join(
    dim_customer_profile_pg.select("customer_profile_sk", "customer_unique_id"),
    on="customer_unique_id", how="left"
).withColumnRenamed("customer_profile_sk", "customer_profile_sk_fk")

# Join dim_date for order_date_sk
fct_orders = fct_orders.join(
    dim_date_pg.select(
        F.col("date_sk").alias("order_date_sk"),
        F.col("full_date").alias("_match_date")
    ),
    F.to_date(F.col("order_purchase_timestamp")) == F.col("_match_date"),
    how="left"
).drop("_match_date")

fct_orders_final = fct_orders.select(
    F.col("order_id"),
    F.col("customer_profile_sk_fk"),
    F.col("order_date_sk"),
    F.col("order_purchase_timestamp"),
    F.col("order_status"),
    F.col("payment_value"),
    F.col("items_count"),
    F.col("distinct_categories"),
    F.col("review_score"),
    F.col("delivery_status"),
    F.col("order_sequence_number"),
    F.col("days_since_previous_order")
)

write_pg(fct_orders_final, "fct_customer_orders")
write_gold_minio(fct_orders_final, "fct_customer_orders")

# FCT CHURN SUMMARY
# One row per unique customer — behavioral fingerprint
# Aggregates avg gap between orders, avg review, and top category

log.info("Building fct_churn_summary")

# Average days between orders, excludes first orders (no previous order)
avg_gap = fct_orders_final \
    .filter(F.col("days_since_previous_order").isNotNull()) \
    .groupBy("customer_profile_sk_fk") \
    .agg(
        F.round(F.avg("days_since_previous_order"), 2).alias("avg_days_between_orders")
    )

# Average review score per customer
avg_review = fct_orders_final \
    .filter(F.col("review_score").isNotNull()) \
    .groupBy("customer_profile_sk_fk") \
    .agg(
        F.round(F.avg("review_score"), 2).alias("avg_review_score")
    )

# Top product category per customer
# Step 1: getting all categories per customer across all their orders
orders_with_items = orders_enriched.join(
    order_items.join(
        products.select("product_id", "product_category_name"),
        on="product_id", how="left"
    ).select("order_id", "product_category_name"),
    on="order_id", how="left"
)

# Step 2: counting purchases per category per customer
category_counts = orders_with_items \
    .filter(F.col("product_category_name").isNotNull()) \
    .groupBy("customer_unique_id", "product_category_name") \
    .agg(F.count("*").alias("cat_count"))

# Step 3: ranking categories per customer, top 1 = most purchased
window_top_cat = Window.partitionBy("customer_unique_id").orderBy(F.desc("cat_count"))
top_category = category_counts \
    .withColumn("rn", F.row_number().over(window_top_cat)) \
    .filter(F.col("rn") == 1) \
    .select("customer_unique_id", "product_category_name") \
    .withColumnRenamed("product_category_name", "top_category_raw")

# Step 4: joining translation for English category name
top_category_english = top_category.join(
    translation.select(
        F.col("product_category_name").alias("top_category_raw"),
        F.col("product_category_name_english").alias("top_category")
    ),
    on="top_category_raw", how="left"
).select("customer_unique_id", "top_category")

# Step 5: joining top_category back to customer_profile to get surrogate key
top_category_with_sk = top_category_english.join(
    dim_customer_profile_pg.select("customer_profile_sk", "customer_unique_id"),
    on="customer_unique_id", how="left"
).withColumnRenamed("customer_profile_sk", "customer_profile_sk_fk")

# Assembling fct_churn_summary
fct_churn = customer_summary.join(
    dim_customer_profile_pg.select("customer_profile_sk", "customer_unique_id"),
    on="customer_unique_id", how="left"
).withColumnRenamed("customer_profile_sk", "customer_profile_sk_fk")

fct_churn = fct_churn \
    .join(avg_gap,                  on="customer_profile_sk_fk", how="left") \
    .join(avg_review,               on="customer_profile_sk_fk", how="left") \
    .join(top_category_with_sk.select("customer_profile_sk_fk", "top_category"),
          on="customer_profile_sk_fk", how="left")

# Joining dim_date for first_order_date_sk
fct_churn = fct_churn.join(
    dim_date_pg.select(
        F.col("date_sk").alias("first_order_date_sk"),
        F.col("full_date").alias("_match_first")
    ),
    F.to_date(F.col("first_order_date")) == F.col("_match_first"),
    how="left"
).drop("_match_first")

# Joining dim_date for last_order_date_sk
fct_churn = fct_churn.join(
    dim_date_pg.select(
        F.col("date_sk").alias("last_order_date_sk"),
        F.col("full_date").alias("_match_last")
    ),
    F.to_date(F.col("last_order_date")) == F.col("_match_last"),
    how="left"
).drop("_match_last")

fct_churn_final = fct_churn.select(
    F.col("customer_profile_sk_fk"),
    F.col("first_order_date_sk"),
    F.col("last_order_date_sk"),
    F.col("customer_segment"),
    F.col("churn_flag"),
    F.col("total_orders"),
    F.col("total_spend").alias("total_spend_brl"),
    F.col("avg_order_value").alias("avg_order_value_brl"),
    F.col("avg_review_score"),
    F.col("avg_days_between_orders"),
    F.col("days_since_last_order"),
    F.col("top_category")
)

write_pg(fct_churn_final, "fct_churn_summary")
write_gold_minio(fct_churn_final, "fct_churn_summary")

log.info("Gold customer churn complete")
spark.stop()