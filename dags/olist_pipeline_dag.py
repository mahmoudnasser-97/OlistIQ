from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner":           "olist_project",
    "depends_on_past": False,
    "start_date":      datetime(2024, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

# ENVIRONMENT SETUP

ENV_SETUP = "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64"

# MINIO S3A CONFIGS

MINIO_CONF = (
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
    "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
)

# SPARK SUBMIT BASE
# Used by Bronze and Silver

SPARK_BASE = (
    "spark-submit "
    "--master spark://spark-master:7077 "
    f"{MINIO_CONF}"
)

# SPARK SUBMIT GOLD
# Used by Gold scripts

SPARK_GOLD = (
    "spark-submit "
    "--master spark://spark-master:7077 "
    "--jars /data/jars/postgresql-42.7.3.jar,"
           "/data/jars/hadoop-aws-3.3.4.jar,"
           "/data/jars/aws-java-sdk-bundle-1.12.262.jar "
    "--conf spark.executor.extraClassPath="
           "/data/jars/postgresql-42.7.3.jar:"
           "/data/jars/hadoop-aws-3.3.4.jar:"
           "/data/jars/aws-java-sdk-bundle-1.12.262.jar "
    "--conf spark.driver.extraClassPath="
           "/data/jars/postgresql-42.7.3.jar:"
           "/data/jars/hadoop-aws-3.3.4.jar:"
           "/data/jars/aws-java-sdk-bundle-1.12.262.jar "
    f"{MINIO_CONF}"
)

# DAG DEFINITION

with DAG(
    dag_id="olist_batch_pipeline",
    default_args=default_args,
    description="OlistIQ batch pipeline: Bronze -> Silver -> Gold (MinIO + PostgreSQL)",
    schedule_interval="@daily",
    catchup=False,
    tags=["olist", "batch", "medallion"],
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            f"{ENV_SETUP} && "
            f"{SPARK_BASE} "
            "/data/scripts/bronze/bronze_ingestion.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            f"{ENV_SETUP} && "
            f"{SPARK_BASE} "
            "/data/scripts/silver/silver_transformation.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    gold_delivery_task = BashOperator(
        task_id="gold_delivery_performance",
        bash_command=(
            f"{ENV_SETUP} && "
            f"{SPARK_GOLD} "
            "/data/scripts/gold/gold_delivery_performance.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    gold_churn_task = BashOperator(
        task_id="gold_customer_churn",
        bash_command=(
            f"{ENV_SETUP} && "
            f"{SPARK_GOLD} "
            "/data/scripts/gold/gold_customer_churn.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    # Important note: The two sequential gold tasks must not run in parallel because
    # a single Spark worker cannot handle two concurrent heavy jobs
    bronze_task >> silver_task >> gold_delivery_task >> gold_churn_task