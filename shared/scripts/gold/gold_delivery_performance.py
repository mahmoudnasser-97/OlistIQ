import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# SPARK SESSION

spark = SparkSession.builder \
    .appName("Olist_Gold_Delivery_Performance") \
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
SCHEMA = "delivery_performance"

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
    path = f"s3a://gold/olist/delivery_performance/{name}"
    df.write.mode("overwrite").parquet(path)
    log.info(f"Written to MinIO gold: {path}")

# AUTO TRUNCATE
# Fact tables first (they reference dims), then dims
# RESTART IDENTITY resets surrogate key counters back to 1

log.info("Truncating all delivery_performance tables")
from py4j.java_gateway import java_import
java_import(spark._jvm, "java.sql.DriverManager")
conn = spark._jvm.DriverManager.getConnection(PG_URL, "admin", "admin")
stmt = conn.createStatement()
for table in [
    "fct_seller_fulfillment",
    "fct_order_delivery",
    "dim_product",
    "dim_seller",
    "dim_customer",
    "dim_date",
]:
    stmt.execute(
        f"TRUNCATE TABLE delivery_performance.{table} RESTART IDENTITY CASCADE"
    )
conn.close()
log.info("All delivery_performance tables truncated successfully")

# ─── READ SILVER ──────────────────────────────────────────────────────────────

log.info("Reading silver tables")
customers   = read_silver("customers")
sellers     = read_silver("sellers")
products    = read_silver("products")
translation = read_silver("product_category_translation")
orders      = read_silver("orders")
order_items = read_silver("order_items")
geo         = read_silver("geolocation")

# GEOLOCATION LOOKUP
# Average lat/lon per zip code, joined onto customers and sellers

geo_avg = geo.groupBy("geolocation_zip_code_prefix").agg(
    F.round(F.avg("geolocation_lat"), 6).alias("latitude"),
    F.round(F.avg("geolocation_lng"), 6).alias("longitude")
)

# DIM DATE
# Date spine from earliest purchase to latest estimated delivery
# sequence() generates array of every date, explode() turns it into rows

log.info("Building dim_date")
date_bounds = orders.select(
    F.min(F.to_date("order_purchase_timestamp")).alias("min_date"),
    F.max(F.to_date("order_estimated_delivery_date")).alias("max_date")
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

# Reload from PostgreSQL to get auto-generated surrogate keys (date_sk)
dim_date_pg = spark.read.jdbc(
    url=PG_URL, table=f"{SCHEMA}.dim_date", properties=PG_PROPS
)
log.info(f"dim_date reloaded from PostgreSQL: {dim_date_pg.count()} rows")

# DIM CUSTOMER

log.info("Building dim_customer")
dim_customer = customers.join(
    geo_avg,
    customers["customer_zip_code_prefix"] == geo_avg["geolocation_zip_code_prefix"],
    how="left"
).select(
    F.col("customer_id"),
    F.col("customer_unique_id"),
    F.col("customer_zip_code_prefix"),
    F.col("customer_city"),
    F.col("customer_state"),
    F.col("latitude"),
    F.col("longitude"),
    F.current_timestamp().alias("created_at")
)

write_pg(dim_customer, "dim_customer")
write_gold_minio(dim_customer, "dim_customer")

dim_customer_pg = spark.read.jdbc(
    url=PG_URL, table=f"{SCHEMA}.dim_customer", properties=PG_PROPS
)
log.info(f"dim_customer reloaded: {dim_customer_pg.count()} rows")

# DIM SELLER

log.info("Building dim_seller")
dim_seller = sellers.join(
    geo_avg,
    sellers["seller_zip_code_prefix"] == geo_avg["geolocation_zip_code_prefix"],
    how="left"
).select(
    F.col("seller_id"),
    F.col("seller_zip_code_prefix"),
    F.col("seller_city"),
    F.col("seller_state"),
    F.col("latitude"),
    F.col("longitude"),
    F.current_timestamp().alias("created_at")
)

write_pg(dim_seller, "dim_seller")
write_gold_minio(dim_seller, "dim_seller")

dim_seller_pg = spark.read.jdbc(
    url=PG_URL, table=f"{SCHEMA}.dim_seller", properties=PG_PROPS
)
log.info(f"dim_seller reloaded: {dim_seller_pg.count()} rows")

# DIM PRODUCT

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

# DATE SK HELPER
# Joins any DataFrame to dim_date_pg on a date column
# Returns the surrogate key as sk_alias
# Temporary match column dropped after join

def get_date_sk(df, date_col, sk_alias):
    return df.join(
        dim_date_pg.select(
            F.col("date_sk").alias(sk_alias),
            F.col("full_date").alias(f"_match_{sk_alias}")
        ),
        F.to_date(F.col(date_col)) == F.col(f"_match_{sk_alias}"),
        how="left"
    ).drop(f"_match_{sk_alias}")

# HAVERSINE DISTANCE
# Computes great-circle distance in km between two lat/lon points

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_r = F.radians(lat1)
    lat2_r = F.radians(lat2)
    dlat   = F.radians(lat2 - lat1)
    dlon   = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(lat1_r) * F.cos(lat2_r) * F.sin(dlon / 2) ** 2
    )
    return F.round(R * 2 * F.asin(F.sqrt(a)), 2)

# FCT ORDER DELIVERY
# One row per order
# Starts from order_items aggregated to order level
# Inner join with orders

log.info("Building fct_order_delivery")
items_agg = order_items.groupBy("order_id").agg(
    F.round(F.sum("freight_value"), 2).alias("freight_total_value"),
    F.count("order_item_id").alias("total_items_count"),
    F.countDistinct("seller_id").alias("seller_count"),
    F.first("seller_id").alias("seller_id")
)

fct_delivery = orders.join(items_agg, on="order_id", how="inner")

# Joining dim_customer to get customer_sk and coordinates
fct_delivery = fct_delivery.join(
    dim_customer_pg.select(
        "customer_sk", "customer_id",
        F.col("latitude").alias("customer_lat"),
        F.col("longitude").alias("customer_lon")
    ),
    on="customer_id", how="left"
).withColumnRenamed("customer_sk", "customer_sk_fk")

# Joining dim_seller to get seller_sk and coordinates
fct_delivery = fct_delivery.join(
    dim_seller_pg.select(
        "seller_sk", "seller_id",
        F.col("latitude").alias("seller_lat"),
        F.col("longitude").alias("seller_lon")
    ),
    on="seller_id", how="left"
).withColumnRenamed("seller_sk", "seller_sk_fk")

# Joining dim_date three times for three date foreign keys
fct_delivery = get_date_sk(fct_delivery, "order_purchase_timestamp",       "purchase_date_sk")
fct_delivery = get_date_sk(fct_delivery, "order_estimated_delivery_date",  "estimated_delivery_date_sk")
fct_delivery = get_date_sk(fct_delivery, "order_delivered_customer_date",  "actual_delivery_date_sk")

# Compute delivery metrics
fct_delivery = fct_delivery \
    .withColumn("delivery_duration_days",
        F.when(F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp")
            )
        ).otherwise(None).cast(IntegerType())) \
    .withColumn("delay_days",
        F.when(F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date")
            )
        ).otherwise(None).cast(IntegerType())) \
    .withColumn("delivery_status",
        F.when(F.col("order_delivered_customer_date").isNull(), "not_delivered")
         .when(F.col("delay_days") <= 0,  "on_time")
         .when(F.col("delay_days") <= 7,  "late")
         .otherwise("very_late")) \
    .withColumn("on_time_flag",
        F.when(F.col("order_delivered_customer_date").isNull(), None)
         .when(F.col("delay_days") <= 0, True)
         .otherwise(False).cast(BooleanType())) \
    .withColumn("is_multi_seller_order",
        F.when(F.col("seller_count") > 1, True)
         .otherwise(False).cast(BooleanType())) \
    .withColumn("distance_km",
        haversine_km(
            F.col("seller_lat"),   F.col("seller_lon"),
            F.col("customer_lat"), F.col("customer_lon")
        )) \
    .withColumn("distance_bucket",
        F.when(F.col("distance_km").isNull(),  "unknown")
         .when(F.col("distance_km") <= 50,     "0-50km")
         .when(F.col("distance_km") <= 200,    "50-200km")
         .when(F.col("distance_km") <= 500,    "200-500km")
         .otherwise("500km+"))

# Selecting only columns matching PostgreSQL table definition
fct_delivery = fct_delivery.select(
    F.col("order_id"),
    F.col("customer_sk_fk"),
    F.col("seller_sk_fk"),
    F.col("purchase_date_sk"),
    F.col("estimated_delivery_date_sk"),
    F.col("actual_delivery_date_sk"),
    F.col("order_status"),
    F.col("freight_total_value"),
    F.col("total_items_count"),
    F.col("seller_count"),
    F.col("is_multi_seller_order"),
    F.col("delivery_duration_days"),
    F.col("delay_days"),
    F.col("delivery_status"),
    F.col("on_time_flag"),
    F.col("distance_km"),
    F.col("distance_bucket")
)

write_pg(fct_delivery, "fct_order_delivery")
write_gold_minio(fct_delivery, "fct_order_delivery")

# FCT SELLER FULFILLMENT
# One row per order line item
# Starts from order_items

log.info("Building fct_seller_fulfillment")
fct_fulfillment = order_items.join(
    orders.select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp"
    ),
    on="order_id", how="inner"
)

# Joining dim_seller
fct_fulfillment = fct_fulfillment.join(
    dim_seller_pg.select(
        "seller_sk", "seller_id",
        F.col("latitude").alias("seller_lat"),
        F.col("longitude").alias("seller_lon")
    ),
    on="seller_id", how="left"
).withColumnRenamed("seller_sk", "seller_sk_fk")

# Joining dim_product
fct_fulfillment = fct_fulfillment.join(
    dim_product_pg.select(
        "product_sk", "product_id",
        F.col("product_weight_g").alias("prod_weight"),
        F.col("product_volume_cm3").alias("prod_volume")
    ),
    on="product_id", how="left"
).withColumnRenamed("product_sk", "product_sk_fk")

# Joining dim_customer
fct_fulfillment = fct_fulfillment.join(
    dim_customer_pg.select(
        "customer_sk", "customer_id",
        F.col("latitude").alias("customer_lat"),
        F.col("longitude").alias("customer_lon")
    ),
    on="customer_id", how="left"
).withColumnRenamed("customer_sk", "customer_sk_fk")

# Joining dim_date twice
fct_fulfillment = get_date_sk(fct_fulfillment, "order_purchase_timestamp", "purchase_date_sk")
fct_fulfillment = get_date_sk(fct_fulfillment, "shipping_limit_date",      "shipping_limit_date_sk")

# Computuing fulfillment metrics
fct_fulfillment = fct_fulfillment \
    .withColumn("seller_preparation_days",
        F.datediff(
            F.col("shipping_limit_date"),
            F.col("order_purchase_timestamp")
        ).cast(IntegerType())) \
    .withColumn("freight_ratio",
        F.when(F.col("price") > 0,
            F.round(F.col("freight_value") / F.col("price"), 4)
        ).otherwise(None)) \
    .withColumn("heavy_product_flag",
        F.when(F.col("prod_weight") > 10000, True)
         .otherwise(False).cast(BooleanType())) \
    .withColumn("oversized_product_flag",
        F.when(F.col("prod_volume") > 50000, True)
         .otherwise(False).cast(BooleanType())) \
    .withColumn("seller_to_customer_distance_km",
        haversine_km(
            F.col("seller_lat"),   F.col("seller_lon"),
            F.col("customer_lat"), F.col("customer_lon")
        ))

# Selecting only columns matching PostgreSQL table definition
fct_fulfillment = fct_fulfillment.select(
    F.col("order_id"),
    F.col("order_item_id"),
    F.col("seller_sk_fk"),
    F.col("product_sk_fk"),
    F.col("customer_sk_fk"),
    F.col("purchase_date_sk"),
    F.col("shipping_limit_date_sk"),
    F.col("freight_value"),
    F.col("price").alias("item_price"),
    F.col("seller_preparation_days"),
    F.col("freight_ratio"),
    F.col("heavy_product_flag"),
    F.col("oversized_product_flag"),
    F.col("seller_to_customer_distance_km")
)

write_pg(fct_fulfillment, "fct_seller_fulfillment")
write_gold_minio(fct_fulfillment, "fct_seller_fulfillment")

log.info("Gold delivery performance complete")
spark.stop()