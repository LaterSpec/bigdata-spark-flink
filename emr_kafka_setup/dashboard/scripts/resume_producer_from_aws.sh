#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
PrimaryHost=""
UserName="hadoop"
DataSize=""
S3Path="s3://figuretibucket/data/raw/youtube/youtube_lake.csv"
ProducerDelayMs=10

read_env_value() {
  local key="$1" file="$ProjectRoot/.env"
  [[ -f "$file" ]] || return 0
  awk -F= -v key="$key" '$1 ~ "^[[:space:]]*" key "[[:space:]]*$" {value=$0; sub(/^[^=]*=/,"",value); gsub(/^[[:space:]"'"'"']+|[[:space:]"'"'"']+$/,"",value); print value; exit}' "$file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) PrimaryHost="${2:-}"; shift 2 ;;
    --pem) PemPath="${2:-}"; shift 2 ;;
    --user) UserName="${2:-hadoop}"; shift 2 ;;
    --data-size) DataSize="${2:-}"; shift 2 ;;
    --s3-path) S3Path="${2:-}"; shift 2 ;;
    --producer-delay-ms) ProducerDelayMs="${2:-10}"; shift 2 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$PrimaryHost" ]] || PrimaryHost="$(read_env_value EMR_PRIMARY || true)"
[[ -n "$DataSize" ]] || DataSize="$(read_env_value DATA_SIZE || true)"
[[ -n "$DataSize" ]] || DataSize=7000
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$PrimaryHost" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"error":"EMR_PRIMARY o PEM no configurado"}'
  exit 0
fi
if [[ ! "$DataSize" =~ ^[0-9]+$ || "$DataSize" -le 0 ]]; then
  echo '{"ok":false,"error":"DATA_SIZE invalido"}'
  exit 0
fi

chmod 600 "$PemPath" 2>/dev/null || true
SSH_COMMON=(-i "$PemPath" -o BatchMode=yes -o ConnectTimeout=12 -o ConnectionAttempts=1 -o StrictHostKeyChecking=no)

ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" "mkdir -p /home/hadoop/bigdata-kafka/{producers,scripts,logs}" >/dev/null
scp "${SSH_COMMON[@]}" \
  "$ProjectRoot/emr_kafka_setup/producers/produce_youtube_chat_from_s3.py" \
  "${UserName}@${PrimaryHost}:/home/hadoop/bigdata-kafka/producers/" >/dev/null
scp "${SSH_COMMON[@]}" \
  "$ProjectRoot/emr_kafka_setup/scripts/monitor_kafka_flow.py" \
  "${UserName}@${PrimaryHost}:/home/hadoop/bigdata-kafka/scripts/" >/dev/null

ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" \
  "DATA_SIZE='$DataSize' S3_PATH='$S3Path' PRODUCER_DELAY_MS='$ProducerDelayMs' python3 -" <<'PY'
import json
import os
import subprocess
import time
from pathlib import Path

project_home = Path("/home/hadoop/bigdata-kafka")
kafka_home = Path("/home/hadoop/kafka")
logs = project_home / "logs"
data_size = int(os.environ["DATA_SIZE"])

broker_probe = subprocess.run(
    [str(kafka_home / "bin/kafka-topics.sh"), "--bootstrap-server", "localhost:9092", "--list"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    timeout=15,
)
if broker_probe.returncode != 0:
    print(json.dumps({"ok": False, "error": "Kafka no responde en EMR_PRIMARY"}))
    raise SystemExit(0)

offsets = subprocess.check_output(
    [
        str(kafka_home / "bin/kafka-run-class.sh"),
        "kafka.tools.GetOffsetShell",
        "--broker-list",
        "localhost:9092",
        "--topic",
        "raw_youtube_chat",
    ],
    stderr=subprocess.DEVNULL,
    text=True,
    timeout=15,
)
raw_total = 0
for line in offsets.splitlines():
    parts = line.strip().split(":")
    if len(parts) == 3:
        raw_total += int(parts[2])

def running(pattern):
    return subprocess.run(
        ["pgrep", "-f", pattern],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode == 0

if not running("monitor_kafka_flow.py"):
    with open(logs / "kafka_flow_monitor.log", "ab") as output:
        subprocess.Popen(
            [
                "python3",
                str(project_home / "scripts/monitor_kafka_flow.py"),
                "--follow",
                "--bootstrap-server",
                "localhost:9092",
            ],
            stdout=output,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )

remaining = max(0, data_size - raw_total)
producer_running = running("produce_youtube_chat_from_s3.py")
if remaining and not producer_running:
    start_row = raw_total + 1
    with open(logs / "producer_youtube_chat_stream.log", "wb") as output:
        subprocess.Popen(
            [
                "python3",
                str(project_home / "producers/produce_youtube_chat_from_s3.py"),
                "--s3-path",
                os.environ["S3_PATH"],
                "--bootstrap-server",
                "localhost:9092",
                "--topic",
                "raw_youtube_chat",
                "--start-row",
                str(start_row),
                "--limit",
                str(remaining),
                "--delay-ms",
                os.environ["PRODUCER_DELAY_MS"],
                "--log-every",
                "100",
            ],
            stdout=output,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    time.sleep(1)
    producer_running = running("produce_youtube_chat_from_s3.py")
else:
    start_row = raw_total + 1

print(json.dumps({
    "ok": remaining == 0 or producer_running,
    "error": "" if remaining == 0 or producer_running else "El producer no permanecio en ejecucion",
    "data_mode": "aws_streaming",
    "data_size": data_size,
    "raw_total": raw_total,
    "resumed_from": start_row,
    "remaining": remaining,
    "producer_running": producer_running,
    "session_complete": remaining == 0,
}, ensure_ascii=False))
PY
