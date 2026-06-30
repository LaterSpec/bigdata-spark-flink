#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/kafka}"
PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/bigdata-kafka}"
LOG_DIR="$PROJECT_HOME/logs"
PID_FILE="$LOG_DIR/kafka.pid"

pid_matches_kafka() {
  local pid="$1"
  local args
  args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ "$args" == *"kafka.Kafka"* ]]
}

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  pid="$(cat "$PID_FILE")"
  "$KAFKA_HOME/bin/kafka-server-stop.sh" || true
  for _ in $(seq 1 20); do
    if ! kill -0 "$pid" 2>/dev/null || ! pid_matches_kafka "$pid"; then
      rm -f "$PID_FILE"
      echo "Kafka stopped"
      exit 0
    fi
    sleep 1
  done
  kill -TERM "$pid" >/dev/null 2>&1 || true
  for _ in $(seq 1 10); do
    if ! kill -0 "$pid" 2>/dev/null || ! pid_matches_kafka "$pid"; then
      rm -f "$PID_FILE"
      echo "Kafka stopped"
      exit 0
    fi
    sleep 1
  done
  kill -KILL "$pid" >/dev/null 2>&1 || true
  sleep 1
  if ! kill -0 "$pid" 2>/dev/null || ! pid_matches_kafka "$pid"; then
    rm -f "$PID_FILE"
    echo "Kafka stopped"
    exit 0
  fi
  echo "Kafka process still appears to be running; PID $pid"
  exit 1
fi

rm -f "$PID_FILE" 2>/dev/null || true
echo "Kafka is not running"
