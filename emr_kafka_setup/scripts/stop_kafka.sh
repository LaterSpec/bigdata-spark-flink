#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/kafka}"
PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/bigdata-kafka}"
LOG_DIR="$PROJECT_HOME/logs"
PID_FILE="$LOG_DIR/kafka.pid"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  "$KAFKA_HOME/bin/kafka-server-stop.sh" || true
  for _ in $(seq 1 20); do
    if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "Kafka stopped"
      exit 0
    fi
    sleep 1
  done
  echo "Kafka process still appears to be running; PID $(cat "$PID_FILE")"
  exit 1
fi

echo "Kafka is not running"

