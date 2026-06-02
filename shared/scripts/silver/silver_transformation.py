import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Olist_Silver_Transformation") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key",        "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

log.info("Spark session started, connected to MinIO")

# HELPER FUNCTIONS

def read_bronze(name):
    path = f"s3a://bronze/olist/{name}"
    log.info(f"Reading bronze: {path}")
    return spark.read.parquet(path)

def write_silver(df, name):
    path = f"s3a://silver/olist/{name}"
    count = df.count()
    log.info(f"Writing silver: {path}  rows={count}")
    df.write.mode("overwrite").parquet(path)
    log.info(f"Done: {path}")

# CUSTOMERS

log.info("Processing customers")
customers = read_bronze("customers")
customers = (
    customers
    .withColumn("customer_city",  F.lower(F.trim(F.col("customer_city"))))
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    .dropDuplicates()
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("customer_unique_id").isNotNull())
)
write_silver(customers, "customers")

# GEOLOCATION

log.info("Processing geolocation")
geo = read_bronze("geolocation")
geo = (
    geo
    .withColumn("geolocation_city",  F.lower(F.trim(F.col("geolocation_city"))))
    .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    .filter(F.col("geolocation_lat").isNotNull())
    .filter(F.col("geolocation_lng").isNotNull())
    .dropDuplicates(["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])
)
write_silver(geo, "geolocation")

# SELLERS
log.info("Processing sellers")
sellers = read_bronze("sellers")
sellers = (
    sellers
    .withColumn("seller_city",  F.lower(F.trim(F.col("seller_city"))))
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    .dropDuplicates()
    .filter(F.col("seller_id").isNotNull())
)
write_silver(sellers, "sellers")

# PRODUCTS

log.info("Processing products")
products = read_bronze("products")

# Drop rows where the entire record is null
products = products.filter(F.col("product_category_name").isNotNull())

# Fixing column name typos inherited from original Olist dataset
products = (
    products
    .withColumnRenamed("product_name_lenght",        "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
)

# Filling remaining null numeric values with column medians
median_weight = products.approxQuantile("product_weight_g",  [0.5], 0.01)[0]
median_length = products.approxQuantile("product_length_cm", [0.5], 0.01)[0]
median_height = products.approxQuantile("product_height_cm", [0.5], 0.01)[0]
median_width  = products.approxQuantile("product_width_cm",  [0.5], 0.01)[0]
median_photos = products.approxQuantile("product_photos_qty",[0.5], 0.01)[0]

products = (
    products
    .withColumn("product_weight_g",
        F.when(F.col("product_weight_g").isNull(),  median_weight)
         .otherwise(F.col("product_weight_g")))
    .withColumn("product_length_cm",
        F.when(F.col("product_length_cm").isNull(), median_length)
         .otherwise(F.col("product_length_cm")))
    .withColumn("product_height_cm",
        F.when(F.col("product_height_cm").isNull(), median_height)
         .otherwise(F.col("product_height_cm")))
    .withColumn("product_width_cm",
        F.when(F.col("product_width_cm").isNull(),  median_width)
         .otherwise(F.col("product_width_cm")))
    .withColumn("product_photos_qty",
        F.when(F.col("product_photos_qty").isNull(), median_photos)
         .otherwise(F.col("product_photos_qty")))
    .withColumn("product_volume_cm3",
        F.round(
            F.col("product_length_cm") *
            F.col("product_height_cm") *
            F.col("product_width_cm"), 2))
    .withColumn("product_category_name",
        F.lower(F.trim(F.col("product_category_name"))))
    .dropDuplicates()
    .filter(F.col("product_id").isNotNull())
)
write_silver(products, "products")

# PRODUCT CATEGORY TRANSLATION

log.info("Processing product category translation")
translation = read_bronze("product_category_translation")

# Adding 2 missing categories found during EDA using Spark SQL UNION ALL

translation.createOrReplaceTempView("translation_raw")
missing_cats = spark.sql("""
    SELECT 'pc_gamer'                                          AS product_category_name,
           'pc_gamer'                                          AS product_category_name_english
    UNION ALL
    SELECT 'portateis_cozinha_e_preparadores_de_alimentos'     AS product_category_name,
           'portable_kitchen_food_preparers'                   AS product_category_name_english
""")

translation = translation.union(missing_cats)
translation = (
    translation
    .withColumn("product_category_name",
        F.lower(F.trim(F.col("product_category_name"))))
    .withColumn("product_category_name_english",
        F.lower(F.trim(F.col("product_category_name_english"))))
    .dropDuplicates()
)
write_silver(translation, "product_category_translation")

# ORDERS

log.info("Processing orders")
orders = read_bronze("orders")

# Casting all 5 timestamp columns from string to Timestamp Type
ts_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
for c in ts_cols:
    orders = orders.withColumn(c, F.to_timestamp(F.col(c)))

# Drop rows with null purchase timestamp
orders = orders.filter(F.col("order_purchase_timestamp").isNotNull())

# Nulling out chronological violations instead of dropping the entire order
# Violation 1: carrier date before approval date
orders = orders.withColumn(
    "order_delivered_carrier_date",
    F.when(
        F.col("order_delivered_carrier_date") < F.col("order_approved_at"),
        None
    ).otherwise(F.col("order_delivered_carrier_date"))
)
# Violation 2: customer delivery before carrier pickup
orders = orders.withColumn(
    "order_delivered_customer_date",
    F.when(
        F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date"),
        None
    ).otherwise(F.col("order_delivered_customer_date"))
)

orders = (
    orders
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .dropDuplicates()
    .filter(F.col("order_id").isNotNull())
)
write_silver(orders, "orders")

# ORDER ITEMS
log.info("Processing order items")
order_items = read_bronze("order_items")
order_items = (
    order_items
    .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
    .filter(F.col("price") > 0)
    .filter(F.col("freight_value") >= 0)
    .dropDuplicates()
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("seller_id").isNotNull())
)
write_silver(order_items, "order_items")

# ORDER PAYMENTS

log.info("Processing order payments")
payments = read_bronze("order_payments")
payments = (
    payments
    .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    .filter(F.col("payment_value") > 0)
    .dropDuplicates()
    .filter(F.col("order_id").isNotNull())
)
write_silver(payments, "order_payments")

# ORDER REVIEWS

log.info("Processing order reviews")
reviews = read_bronze("order_reviews")
reviews = (
    reviews
    .withColumn("review_creation_date",
        F.to_timestamp(F.col("review_creation_date")))
    .withColumn("review_answer_timestamp",
        F.to_timestamp(F.col("review_answer_timestamp")))
    .withColumn("review_comment_title",
        F.when(F.col("review_comment_title").isNull(), "No Title")
         .otherwise(F.trim(F.col("review_comment_title"))))
    .withColumn("review_comment_message",
        F.when(F.col("review_comment_message").isNull(), "No Comment")
         .otherwise(F.trim(F.col("review_comment_message"))))
    .filter(F.col("review_score").between(1, 5))
    .dropDuplicates()
    .filter(F.col("review_id").isNotNull())
    .filter(F.col("order_id").isNotNull())
)
write_silver(reviews, "order_reviews")

log.info("Silver transformation complete")
spark.stop()