#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/kafka}"
PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/bigdata-kafka}"
CONFIG_FILE="$PROJECT_HOME/config/kraft-server.properties"
DATA_DIR="$PROJECT_HOME/kraft-combined-logs"
LOG_DIR="$PROJECT_HOME/logs"
PID_FILE="$LOG_DIR/kafka.pid"
RESET_STORAGE="${RESET_STORAGE:-0}"
WAIT_FOR_READY="${WAIT_FOR_READY:-1}"
CLUSTER_ID="${CLUSTER_ID:-}"

mkdir -p "$LOG_DIR" "$DATA_DIR"

if [[ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]]; then
  echo "Kafka binaries not found at $KAFKA_HOME" >&2
  exit 1
fi

if [[ "$RESET_STORAGE" == "1" ]]; then
  rm -rf "$DATA_DIR"
  rm -f "$PID_FILE"
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Kafka already running with PID $(cat "$PID_FILE")"
  exit 0
fi

if [[ ! -f "$DATA_DIR/meta.properties" ]]; then
  if [[ -z "$CLUSTER_ID" ]]; then
    CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  fi
  echo "$CLUSTER_ID" > "$LOG_DIR/kraft-cluster-id.txt"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONFIG_FILE"
fi

nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG_FILE" \
  </dev/null \
  > "$LOG_DIR/kafka-server.out" \
  2> "$LOG_DIR/kafka-server.err" &

echo $! > "$PID_FILE"
echo "Kafka starting with PID $(cat "$PID_FILE")"

if [[ "$WAIT_FOR_READY" == "0" ]]; then
  exit 0
fi

for _ in $(seq 1 30); do
  if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "Kafka is ready on localhost:9092"
    exit 0
  fi
  sleep 2
done

echo "Kafka did not become ready within 60 seconds. Check $LOG_DIR/kafka-server.err" >&2
exit 1
