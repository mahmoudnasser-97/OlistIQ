# =============================================================================
# jupyter/Dockerfile
#
# Base  : jupyter/pyspark-notebook:spark-3.5.0
# Adds  : boto3 (MinIO access), kafka-python, redis, plotly
#
# Pre-configured with:
#   - Spark master URL via SPARK_MASTER env var
#   - MinIO credentials via AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
#   - Notebooks mounted from ./notebooks
#   - Token: olist
#
# Access: http://localhost:8888
# =============================================================================

FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    kafka-python==2.0.2 \
    redis==5.0.1 \
    plotly==5.17.0 \
    psycopg2-binary==2.9.9 \
    faker==22.0.0

USER jovyan