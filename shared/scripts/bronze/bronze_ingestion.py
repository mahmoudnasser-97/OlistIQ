import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Olist_Bronze_Ingestion") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key",        "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

log.info("Spark session started — connected to MinIO")

files = [
    ("olist_customers_dataset.csv",               "s3a://bronze/olist/customers"),
    ("olist_geolocation_dataset.csv",             "s3a://bronze/olist/geolocation"),
    ("olist_order_items_dataset.csv",             "s3a://bronze/olist/order_items"),
    ("olist_order_payments_dataset.csv",          "s3a://bronze/olist/order_payments"),
    ("olist_order_reviews_dataset.csv",           "s3a://bronze/olist/order_reviews"),
    ("olist_orders_dataset.csv",                  "s3a://bronze/olist/orders"),
    ("product_category_name_translation.csv",     "s3a://bronze/olist/product_category_translation"),
    ("olist_products_dataset.csv",                "s3a://bronze/olist/products"),
    ("olist_sellers_dataset.csv",                 "s3a://bronze/olist/sellers"),
]

for csv_file, s3_path in files:
    local_path = f"/data/data/{csv_file}"
    log.info(f"Reading: {local_path}")

    df = spark.read \
        .option("header",      "true") \
        .option("inferSchema", "true") \
        .option("encoding",    "UTF-8") \
        .csv(local_path)

    row_count = df.count()
    log.info(f"Rows read: {row_count}")

    df.write \
        .mode("overwrite") \
        .parquet(s3_path)

    log.info(f"Written to MinIO: {s3_path}  rows={row_count}")

log.info("Bronze ingestion complete")
spark.stop()