#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TOPIC="${TOPIC:-raw_youtube_chat}"
MAX_MESSAGES="${MAX_MESSAGES:-5}"

"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages "$MAX_MESSAGES"

