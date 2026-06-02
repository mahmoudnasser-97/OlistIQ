import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    avg, count, sum as spark_sum,
    max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)

# CONFIGURATION

KAFKA_BROKER = "broker:29092"      # Internal Docker network — matches compose container name
KAFKA_TOPIC  = "olist_orders_stream"
REDIS_HOST   = "redis"             # Internal Docker network
REDIS_PORT   = 6379
CHECKPOINT_LOCATION = "/tmp/spark_checkpoints"

# SCHEMA DEFINITION
# Mirrors the simulator's output structure exactly

order_schema = StructType([
    StructField("order_id",                      StringType()),
    StructField("customer_id",                   StringType()),
    StructField("order_status",                  StringType()),
    StructField("order_purchase_timestamp",      StringType()),
    StructField("order_approved_at",             StringType()),
    StructField("order_delivered_carrier_date",  StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType())
])

customer_schema = StructType([
    StructField("customer_id",               StringType()),
    StructField("customer_unique_id",        StringType()),
    StructField("customer_zip_code_prefix",  StringType()),
    StructField("customer_city",             StringType()),
    StructField("customer_state",            StringType())
])

seller_schema = StructType([
    StructField("seller_id",               StringType()),
    StructField("seller_zip_code_prefix",  StringType()),
    StructField("seller_city",             StringType()),
    StructField("seller_state",            StringType())
])

product_schema = StructType([
    StructField("product_id",                   StringType()),
    StructField("product_category_name",        StringType()),
    StructField("product_name_length",          IntegerType()),
    StructField("product_description_length",   IntegerType()),
    StructField("product_photos_qty",           IntegerType()),
    StructField("product_weight_g",             IntegerType()),
    StructField("product_length_cm",            IntegerType()),
    StructField("product_height_cm",            IntegerType()),
    StructField("product_width_cm",             IntegerType())
])

order_item_schema = StructType([
    StructField("order_id",           StringType()),
    StructField("order_item_id",      IntegerType()),
    StructField("product_id",         StringType()),
    StructField("seller_id",          StringType()),
    StructField("shipping_limit_date",StringType()),
    StructField("price",              DoubleType()),
    StructField("freight_value",      DoubleType())
])

payment_schema = StructType([
    StructField("order_id",              StringType()),
    StructField("payment_sequential",    IntegerType()),
    StructField("payment_type",          StringType()),
    StructField("payment_installments",  IntegerType()),
    StructField("payment_value",         DoubleType())
])

review_schema = StructType([
    StructField("review_id",               StringType()),
    StructField("order_id",               StringType()),
    StructField("review_score",            IntegerType()),
    StructField("review_comment_title",    StringType()),
    StructField("review_comment_message",  StringType()),
    StructField("review_creation_date",    StringType()),
    StructField("review_answer_timestamp", StringType())
])

event_schema = StructType([
    StructField("event_timestamp", StringType()),
    StructField("order",      order_schema),
    StructField("customer",   customer_schema),
    StructField("seller",     seller_schema),
    StructField("product",    product_schema),
    StructField("order_item", order_item_schema),
    StructField("payment",    payment_schema),
    StructField("review",     review_schema)
])

# REDIS WRITER
# Called by Spark for every micro-batch (every 10 seconds)
# Writes pre-aggregated counters, Streamlit reads these directly

def write_to_redis(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch, skipping.")
        return

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pdf = batch_df.toPandas()
    print(f"[Batch {batch_id}] Processing {len(pdf)} events")

    for _, row in pdf.iterrows():

        # Raw event hash, 24hr TTL, powers the live feed table
        event_key  = f"event:{row['order_id']}"
        event_data = {
            "order_id":         row["order_id"],
            "order_status":     row["order_status"],
            "customer_state":   row["customer_state"],
            "product_category": row["product_category_name"],
            "payment_type":     row["payment_type"],
            "payment_value":    str(row["payment_value"]),
            "review_score":     str(row["review_score"]),
            "price":            str(row["price"]),
            "freight_value":    str(row["freight_value"]),
            "event_timestamp":  row["event_timestamp"]
        }
        r.hset(event_key, mapping=event_data)
        r.expire(event_key, 86400)

        # Capped list of recent order IDs — latest 200 only
        r.lpush("recent_events", row["order_id"])
        r.ltrim("recent_events", 0, 199)

        # Running counters per dimension
        r.incr(f"counters:status:{row['order_status']}")
        r.incr(f"counters:payment:{row['payment_type']}")
        r.incr(f"counters:category:{row['product_category_name']}")
        r.incr(f"counters:state:{row['customer_state']}")

        # Scalar metrics, incrbyfloat for revenue, separate sum+count for avg review
        r.incrbyfloat("metrics:total_revenue",     float(row["payment_value"]))
        r.incrbyfloat("metrics:total_freight",     float(row["freight_value"]))
        r.incrbyfloat("metrics:review_score_sum",  float(row["review_score"]))
        r.incr("metrics:review_score_count")
        r.incr("metrics:total_orders")

    print(f"[Batch {batch_id}] Written to Redis successfully")


# SPARK SESSION

def create_spark_session():
    return (
        SparkSession.builder
        .appName("OlistIQ_StreamingPipeline")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )


# MAIN STREAMING PIPELINE

def run_streaming():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Spark session created.")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")

    # Read raw binary messages from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # Parse JSON payload using defined schema
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    )

    # Flatten nested structure
    flat_stream = parsed_stream.select(
        col("data.event_timestamp"),
        col("data.order.order_id"),
        col("data.order.order_status"),
        col("data.order.order_purchase_timestamp"),
        col("data.customer.customer_state"),
        col("data.customer.customer_city"),
        col("data.product.product_category_name"),
        col("data.product.product_weight_g"),
        col("data.order_item.price"),
        col("data.order_item.freight_value"),
        col("data.payment.payment_type"),
        col("data.payment.payment_value"),
        col("data.payment.payment_installments"),
        col("data.review.review_score"),
        col("data.seller.seller_state")
    )

    # Write each micro-batch to Redis via foreachBatch
    query = (
        flat_stream.writeStream
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Streaming query started. Processing every 10 seconds")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming()