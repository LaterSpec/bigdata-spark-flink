#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
HostName=""
UserName="hadoop"
DataSize=""
SparkBatchSize="${SPARK_BATCH_SIZE:-1000}"
SparkMaxConcurrency="${SPARK_MAX_CONCURRENCY:-1}"

read_env_value() {
  local key="$1"
  local file="$ProjectRoot/.env"
  [[ -f "$file" ]] || return 0
  awk -F= -v key="$key" '
    $1 ~ "^[[:space:]]*" key "[[:space:]]*$" {
      value=$0
      sub(/^[^=]*=/, "", value)
      gsub(/^[[:space:]"'"'"']+|[[:space:]"'"'"']+$/, "", value)
      print value
      exit
    }
  ' "$file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HostName="${2:-}"; shift 2 ;;
    --pem) PemPath="${2:-}"; shift 2 ;;
    --user) UserName="${2:-hadoop}"; shift 2 ;;
    --data-size) DataSize="${2:-}"; shift 2 ;;
    --spark-batch-size) SparkBatchSize="${2:-1000}"; shift 2 ;;
    --spark-max-concurrency) SparkMaxConcurrency="${2:-1}"; shift 2 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$HostName" ]]; then HostName="$(read_env_value EMR_PRIMARY || true)"; fi
if [[ -z "$DataSize" ]]; then DataSize="$(read_env_value DATA_SIZE || true)"; fi
if [[ -z "$DataSize" ]]; then DataSize=7000; fi
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$HostName" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"error":"host o PEM no configurado"}'
  exit 0
fi

chmod 600 "$PemPath" 2>/dev/null || true

ssh -i "$PemPath" -o StrictHostKeyChecking=no "${UserName}@${HostName}" \
  "DATA_SIZE='$DataSize' SPARK_BATCH_SIZE='$SparkBatchSize' SPARK_MAX_CONCURRENCY='$SparkMaxConcurrency' python3 -" <<'PY'
import json
import os
import subprocess
from datetime import datetime, timezone

KAFKA_HOME = os.environ.get("KAFKA_HOME", "/home/hadoop/kafka")
BROKER = os.environ.get("BROKER", "localhost:9092")
TOPICS = ["raw_youtube_chat", "nlp_stream_results", "alerts_polarization"]

def count_topic(topic):
    try:
        output = subprocess.check_output(
            [f"{KAFKA_HOME}/bin/kafka-run-class.sh", "kafka.tools.GetOffsetShell", "--broker-list", BROKER, "--topic", topic],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
        total = 0
        partitions = {}
        for line in output.splitlines():
            parts = line.strip().split(":")
            if len(parts) == 3:
                partitions[parts[1]] = int(parts[2])
                total += int(parts[2])
        return {"total": total, "partitions": partitions}
    except Exception as exc:
        return {"total": 0, "partitions": {}, "error": str(exc)}

def committed_group_total(group_id, topic):
    try:
        output = subprocess.check_output(
            [
                f"{KAFKA_HOME}/bin/kafka-consumer-groups.sh",
                "--bootstrap-server", BROKER,
                "--describe",
                "--group", group_id,
            ],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
        total = 0
        partitions = {}
        for line in output.splitlines():
            columns = line.split()
            if len(columns) < 5 or columns[0] != group_id or columns[1] != topic:
                continue
            try:
                current_offset = int(columns[3])
                partition = columns[2]
            except (ValueError, IndexError):
                continue
            partitions[partition] = current_offset
            total += current_offset
        return {"total": total, "partitions": partitions}
    except Exception as exc:
        return {"total": 0, "partitions": {}, "error": str(exc)}

try:
    broker_probe = subprocess.run(
        [f"{KAFKA_HOME}/bin/kafka-topics.sh", "--bootstrap-server", BROKER, "--list"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=10,
    )
    broker_ok = broker_probe.returncode == 0
except Exception:
    broker_ok = False
if broker_ok:
    counts = {topic: count_topic(topic) for topic in TOPICS}
    counts["flink_output_total"] = dict(counts["nlp_stream_results"])
    counts["normalized_comments"] = committed_group_total("flink-job1-normalize", "raw_youtube_chat")
else:
    counts = {
        topic: {"total": 0, "partitions": {}, "error": "Kafka no responde"}
        for topic in TOPICS
    }
    counts["flink_output_total"] = dict(counts["nlp_stream_results"])
    counts["normalized_comments"] = {"total": 0, "partitions": {}}
raw_total = counts["raw_youtube_chat"]["total"]
raw_partitions = counts["raw_youtube_chat"].get("partitions", {})
normalized_partitions = counts["normalized_comments"].get("partitions", {})
counts["normalized_comments"]["partitions"] = {
    partition: min(int(raw_partitions.get(partition, 0)), int(offset))
    for partition, offset in normalized_partitions.items()
}
counts["normalized_comments"]["total"] = min(
    raw_total,
    sum(counts["normalized_comments"]["partitions"].values()),
)
batch_size = int(os.environ.get("SPARK_BATCH_SIZE", "1000"))
data_size = int(os.environ.get("DATA_SIZE", "7000"))
max_concurrency = max(1, int(os.environ.get("SPARK_MAX_CONCURRENCY", "1")))
producer_running = subprocess.run(
    ["pgrep", "-f", "produce_youtube_chat_from_s3.py"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
).returncode == 0
session_complete = raw_total >= data_size
eligible = (raw_total // batch_size) * batch_size
eligible = min(eligible, data_size)

print(json.dumps({
    "ok": broker_ok,
    "error": "" if broker_ok else "Kafka no responde en EMR_PRIMARY",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "data_size": data_size,
    "spark_batch_size": batch_size,
    "spark_max_concurrency": max_concurrency,
    "producer_running": producer_running,
    "session_complete": session_complete,
    "needs_resume": broker_ok and not session_complete and not producer_running,
    "counts": counts,
    "eligible_spark_target": eligible,
}, ensure_ascii=False))
PY
