#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"
PemPath="$ProjectRoot/final.pem"
PrimaryHost=""
WorkerHosts=""
UserName="hadoop"

read_env_value() {
  local key="$1" file="$ProjectRoot/.env"
  [[ -f "$file" ]] || return 0
  awk -F= -v key="$key" '$1 ~ "^[[:space:]]*" key "[[:space:]]*$" {value=$0; sub(/^[^=]*=/,"",value); gsub(/^[[:space:]"'"'"']+|[[:space:]"'"'"']+$/,"",value); print value; exit}' "$file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) PrimaryHost="${2:-}"; shift 2 ;;
    --workers) WorkerHosts="${2:-}"; shift 2 ;;
    --pem) PemPath="${2:-}"; shift 2 ;;
    --user) UserName="${2:-hadoop}"; shift 2 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$PrimaryHost" ]] || PrimaryHost="$(read_env_value EMR_PRIMARY || true)"
[[ -n "$WorkerHosts" ]] || WorkerHosts="$(read_env_value EMR_WORKERS || true)"
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$PrimaryHost" || -z "$WorkerHosts" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"status":"offline","error":"EMR_PRIMARY, EMR_WORKERS o PEM no configurado"}'
  exit 0
fi

SSH_COMMON=(-i "$PemPath" -o BatchMode=yes -o ConnectTimeout=12 -o StrictHostKeyChecking=no)
Temporary="$(mktemp)"
trap 'rm -f "$Temporary" "$Temporary.kafka"' EXIT

if ! timeout -k 2s 20s ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" "python3 -" > "$Temporary.kafka" <<'PY'
import json
import subprocess
from pathlib import Path

path = Path("/home/hadoop/bigdata-kafka/logs/kafka_flow_health.json")
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception as exc:
    payload = {"ok": False, "status": "offline", "error": str(exc)}
payload.setdefault("processes", {})
payload["processes"]["monitor_running"] = subprocess.run(
    ["pgrep", "-f", "monitor_kafka_flow.py"], capture_output=True
).returncode == 0
print(json.dumps(payload, ensure_ascii=False))
PY
then
  echo '{"ok":false,"status":"offline","error":"EMR_PRIMARY no responde"}' > "$Temporary.kafka"
fi

IFS=',; ' read -r -a Workers <<< "$WorkerHosts"
for index in "${!Workers[@]}"; do
  worker="${Workers[$index]}"
  if ! timeout -k 2s 20s ssh "${SSH_COMMON[@]}" "${UserName}@${worker}" "WORKER_HOST='$worker' FLINK_OWNER='$([[ "$index" -eq 0 ]] && echo 1 || echo 0)' python3 -" >> "$Temporary" <<'PY'
import json
import os
import subprocess
from pathlib import Path

def command(args, timeout=10):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout
    except Exception:
        return 1, ""

_, yarn_nodes_raw = command(["yarn", "node", "-list", "-all"])
yarn_nodes = sum(1 for line in yarn_nodes_raw.splitlines() if "RUNNING" in line)
_, apps_raw = command(["yarn", "application", "-list", "-appStates", "ALL"])
states = {"running": 0, "accepted": 0, "finished": 0, "failed": 0}
for line in apps_raw.splitlines():
    fields = line.split()
    if len(fields) < 6 or not fields[0].startswith("application_"):
        continue
    state = fields[5].lower()
    if state in states:
        states[state] += 1
_, flink_raw = command(["pgrep", "-af", "FlinkKafkaStreamingJobs"])
flink_jobs = len([
    line for line in flink_raw.splitlines()
    if line.strip() and "pgrep -af FlinkKafkaStreamingJobs" not in line
])
batch_counts = {"running": 0, "done": 0, "failed": 0}
for path in Path("/home/hadoop/bigdata-kafka/logs/spark_batches").glob("*.status.json"):
    try:
        status = json.loads(path.read_text(encoding="utf-8")).get("status", "")
        if status in batch_counts:
            batch_counts[status] += 1
    except Exception:
        batch_counts["failed"] += 1
print(json.dumps({
    "host": os.environ["WORKER_HOST"],
    "reachable": True,
    "flink_owner": os.environ["FLINK_OWNER"] == "1",
    "flink_jobs": flink_jobs,
    "yarn_nodes_running": yarn_nodes,
    "yarn_applications": states,
    "spark_batches": batch_counts,
}, ensure_ascii=False))
PY
  then
    printf '{"host":"%s","reachable":false,"flink_owner":%s,"flink_jobs":0,"yarn_nodes_running":0,"yarn_applications":{}}\n' \
      "$worker" "$([[ "$index" -eq 0 ]] && echo true || echo false)" >> "$Temporary"
  fi
done

python3 - "$Temporary.kafka" "$Temporary" <<'PY'
import json
import sys
from datetime import datetime, timezone

try:
    kafka = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception as exc:
    kafka = {"ok": False, "status": "offline", "error": str(exc)}
workers = []
for raw in open(sys.argv[2], encoding="utf-8"):
    try:
        workers.append(json.loads(raw))
    except Exception:
        pass
worker_ok = bool(workers) and all(item.get("reachable") and item.get("yarn_nodes_running", 0) > 0 for item in workers)
flink_owner = next((item for item in workers if item.get("flink_owner")), {})
session_complete = kafka.get("session", {}).get("complete", False)
flink_ok = flink_owner.get("flink_jobs", 0) >= 5 or session_complete
kafka_ok = kafka.get("status") == "healthy"
spark = {"running": 0, "done": 0, "failed": 0}
for worker in workers:
    for key in spark:
        spark[key] += worker.get("spark_batches", {}).get(key, 0)
status = "healthy" if kafka_ok and worker_ok and flink_ok else "degraded"
actions = []
if not kafka_ok:
    actions.append("Revisar quorum, ISR y logs de los brokers Kafka.")
if not worker_ok:
    actions.append("Verificar conectividad y nodos YARN de EMR_WORKERS.")
if not flink_ok:
    actions.append("Reiniciar los cinco jobs Flink en el primer worker.")
print(json.dumps({
    "ok": True,
    "status": status,
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "kafka": kafka,
    "workers": workers,
    "flink": {
        "healthy": flink_ok,
        "expected_jobs": 5,
        "running_jobs": flink_owner.get("flink_jobs", 0),
        "session_complete": session_complete,
    },
    "spark": spark,
    "actions": actions,
}, ensure_ascii=False))
PY
