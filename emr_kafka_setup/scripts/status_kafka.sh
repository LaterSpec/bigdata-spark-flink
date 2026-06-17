#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/kafka}"
PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/bigdata-kafka}"
PID_FILE="$PROJECT_HOME/logs/kafka.pid"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Kafka process: running, PID $(cat "$PID_FILE")"
else
  echo "Kafka process: not running"
fi

if command -v ss >/dev/null 2>&1; then
  ss -ltnp 2>/dev/null | grep -E ':9092|:9093' || true
fi

"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list 2>/dev/null || true

