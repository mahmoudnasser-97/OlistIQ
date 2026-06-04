# =============================================================================
# kafka-connect/Dockerfile
#
# Base  : confluentinc/cp-kafka-connect:7.6.1
# Adds  : S3 Sink Connector (to archive Kafka events into MinIO)
#
# The S3 Sink Connector allows Kafka Connect to automatically
# write Kafka topic messages into MinIO buckets as JSON files
# enabling long-term storage of the raw event stream
# =============================================================================

FROM confluentinc/cp-kafka-connect:7.6.1

# Install the S3 Sink Connector via Confluent Hub
# This is the official way to add connectors to cp-kafka-connect
RUN confluent-hub install --no-prompt \
    confluentinc/kafka-connect-s3:10.5.7