#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
HostName=""
UserName="hadoop"
CursorB64=""
MaxRaw=80
MaxFlink=80
MaxAlerts=40

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
    --cursor-b64) CursorB64="${2:-}"; shift 2 ;;
    --max-raw) MaxRaw="${2:-80}"; shift 2 ;;
    --max-flink) MaxFlink="${2:-80}"; shift 2 ;;
    --max-alerts) MaxAlerts="${2:-40}"; shift 2 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$HostName" ]]; then
  HostName="$(read_env_value EMR_PRIMARY || true)"
fi

if [[ -z "$HostName" ]]; then
  echo "No se encontro HostName. Define EMR_PRIMARY en .env o usa --host." >&2
  exit 1
fi

if [[ "$PemPath" != /* ]]; then
  PemPath="$DashboardRoot/$PemPath"
fi

if [[ ! -f "$PemPath" ]]; then
  echo "No se encontro el archivo PEM en $PemPath" >&2
  exit 1
fi

chmod 600 "$PemPath" 2>/dev/null || true

RemotePython="$(mktemp)"
cleanup() {
  rm -f "$RemotePython"
}
trap cleanup EXIT

cat > "$RemotePython" <<'PY'
import base64
import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer, TopicPartition

BROKER = os.environ.get("BROKER", "localhost:9092")

TOPICS = {
    "raw_youtube_chat": ("raw_messages", int(os.environ.get("MAX_RAW", "80"))),
    "nlp_stream_results": ("flink_events", int(os.environ.get("MAX_FLINK", "80"))),
    "alerts_polarization": ("filtered_messages", int(os.environ.get("MAX_ALERTS", "40"))),
}


def load_cursor():
    raw = os.environ.get("CURSOR_B64", "").strip()
    if not raw:
        return {}
    try:
        return json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        return {}


def parse_event(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    try:
        return json.loads(value)
    except Exception:
        return {"raw_value": value}


def source_for_topic(topic):
    if topic == "raw_youtube_chat":
        return "raw"
    if topic == "alerts_polarization":
        return "alert"
    return "stream"


def is_filtered_flink_event(event):
    payload = event.get("payload") or {}
    has_text = bool(
        payload.get("stream_text")
        or payload.get("message_text")
        or payload.get("message_clean")
        or payload.get("message_raw")
        or event.get("message_clean")
        or event.get("message_raw")
    )
    has_category = bool(
        payload.get("local_rule_tags")
        or payload.get("local_risk_score_stream")
        or payload.get("actor")
        or payload.get("alert_type")
    )
    return has_text and has_category


cursor = load_cursor()
consumer = KafkaConsumer(
    bootstrap_servers=BROKER,
    enable_auto_commit=False,
    consumer_timeout_ms=600,
    request_timeout_ms=6000,
    api_version_auto_timeout_ms=4000,
    value_deserializer=lambda value: value,
)
payload = {
    "ok": True,
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "data_mode": "aws_delta",
    "counts": {},
    "delivered_counts": {},
    "lag": {},
    "cursor": {},
    "raw_messages": [],
    "flink_events": [],
    "filtered_messages": [],
}

for topic, (bucket, limit) in TOPICS.items():
    partitions = consumer.partitions_for_topic(topic) or set()
    topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    if not topic_partitions:
        payload["counts"][topic] = 0
        payload["cursor"][topic] = {}
        continue

    end_offsets = consumer.end_offsets(topic_partitions)
    offsets = {str(tp.partition): int(end_offsets.get(tp, 0)) for tp in topic_partitions}
    payload["counts"][topic] = sum(offsets.values())
    payload["cursor"][topic] = {}
    payload["delivered_counts"][topic] = 0

    if limit <= 0:
        payload["cursor"][topic] = {
            str(tp.partition): int(cursor.get(topic, {}).get(str(tp.partition), 0) or 0)
            for tp in topic_partitions
        }
        payload["lag"][topic] = sum(
            max(0, int(end_offsets.get(tp, 0)) - int(payload["cursor"][topic].get(str(tp.partition), 0)))
            for tp in topic_partitions
        )
        continue

    per_partition = max(1, (limit // len(topic_partitions)) + 1)
    consumed = 0
    for tp in topic_partitions:
        end = int(end_offsets.get(tp, 0))
        saved = cursor.get(topic, {}).get(str(tp.partition))
        if isinstance(saved, int):
            start = min(max(saved, 0), end)
        else:
            start = 0

        remaining = max(0, min(per_partition, limit - consumed, end - start))
        if remaining <= 0:
            payload["cursor"][topic][str(tp.partition)] = start
            continue

        consumer.assign([tp])
        consumer.seek(tp, start)
        partition_events = []
        while len(partition_events) < remaining:
            records = consumer.poll(timeout_ms=700, max_records=remaining - len(partition_events))
            batch = records.get(tp, [])
            if not batch:
                break
            for record in batch:
                event = parse_event(record.value)
                event["source_topic"] = topic
                event["source_partition"] = int(tp.partition)
                event["source_offset"] = int(record.offset)
                event["source"] = source_for_topic(topic)
                partition_events.append(event)
                if topic == "nlp_stream_results" and is_filtered_flink_event(event):
                    filtered_event = dict(event)
                    filtered_event["source"] = "filtered"
                    payload["filtered_messages"].append(filtered_event)
                if len(partition_events) >= remaining:
                    break
        payload[bucket].extend(partition_events)
        consumed += len(partition_events)
        next_offset = partition_events[-1].get("source_offset", start - 1) + 1 if partition_events else start
        payload["cursor"][topic][str(tp.partition)] = min(next_offset, end)
        if consumed >= limit:
            break

    for tp in topic_partitions:
        partition_key = str(tp.partition)
        payload["cursor"][topic].setdefault(partition_key, int(cursor.get(topic, {}).get(partition_key, 0) or 0))
    payload["delivered_counts"][topic] = len(payload[bucket])
    payload["lag"][topic] = sum(
        max(0, int(end_offsets.get(tp, 0)) - int(payload["cursor"][topic].get(str(tp.partition), 0)))
        for tp in topic_partitions
    )

for bucket in ("raw_messages", "flink_events", "filtered_messages"):
    payload[bucket].sort(key=lambda item: (item.get("source_partition", 0), item.get("source_offset", 0)))

consumer.close()
print(json.dumps(payload, ensure_ascii=False))
PY

ssh -i "$PemPath" -o StrictHostKeyChecking=no "${UserName}@${HostName}" \
  "CURSOR_B64='$CursorB64' MAX_RAW='$MaxRaw' MAX_FLINK='$MaxFlink' MAX_ALERTS='$MaxAlerts' python3 -" < "$RemotePython"
