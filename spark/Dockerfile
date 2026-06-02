# =============================================================================
# spark/Dockerfile
#
# Base  : apache/spark:3.5.1-python3
# Adds  : - boto3 + hadoop-aws + aws-java-sdk  (MinIO / S3A support)
#         - Kafka connector JARs               (Structured Streaming)
#         - redis, psycopg2                    (streaming writer + Gold scripts)
#         - postgresql JDBC JAR                (Gold batch scripts)
#
# Used by: spark-master, spark-worker, spark-streaming containers
# =============================================================================

FROM apache/spark:3.5.1-python3

USER root

# ── Python packages ──────────────────────────────────────────────────────────
RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    redis==5.0.1 \
    psycopg2-binary==2.9.9 \
    kafka-python==2.0.2 \
    faker==22.0.0 \
    pandas==2.0.3

# ── Kafka JARs for Spark Structured Streaming ─────────────────────────────────
# Downloaded at build time — no Maven resolution at runtime
# Versions must match exactly: Spark 3.5.1, Scala 2.12, Kafka client 3.4.0

RUN curl -fL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL -o /opt/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -fL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ── S3A JARs for MinIO connectivity ──────────────────────────────────────────
# hadoop-aws provides the S3AFileSystem implementation
# aws-java-sdk-bundle is the full AWS SDK hadoop-aws depends on

RUN curl -fL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# ── PostgreSQL JDBC JAR ───────────────────────────────────────────────────────
RUN curl -fL -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

USER spark