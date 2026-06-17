#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
PARTITIONS="${PARTITIONS:-3}"
RETENTION_MS="${RETENTION_MS:-86400000}"

topics=(
  raw_youtube_chat
  nlp_stream_results
  alerts_polarization
  nlp_batch_results
)

for topic in "${topics[@]}"; do
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config "retention.ms=$RETENTION_MS"
done

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list

