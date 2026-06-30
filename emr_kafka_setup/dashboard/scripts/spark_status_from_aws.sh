#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
WorkerHosts=""
UserName="hadoop"
MaxConcurrency="${SPARK_MAX_CONCURRENCY:-1}"

read_env_value() {
  local key="$1" file="$ProjectRoot/.env"
  [[ -f "$file" ]] || return 0
  awk -F= -v key="$key" '$1 ~ "^[[:space:]]*" key "[[:space:]]*$" {value=$0; sub(/^[^=]*=/,"",value); gsub(/^[[:space:]"'"'"']+|[[:space:]"'"'"']+$/,"",value); print value; exit}' "$file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers) WorkerHosts="${2:-}"; shift 2 ;;
    --pem) PemPath="${2:-}"; shift 2 ;;
    --user) UserName="${2:-hadoop}"; shift 2 ;;
    --max-concurrency) MaxConcurrency="${2:-1}"; shift 2 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$WorkerHosts" ]] || WorkerHosts="$(read_env_value EMR_WORKERS || true)"
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$WorkerHosts" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"error":"EMR_WORKERS o PEM no configurado","batches":{}}'
  exit 0
fi

IFS=',; ' read -r -a Workers <<< "$WorkerHosts"
Temporary="$(mktemp)"
trap 'rm -f "$Temporary"' EXIT
SSH_COMMON=(-i "$PemPath" -o BatchMode=yes -o ConnectTimeout=12 -o StrictHostKeyChecking=no)

for worker in "${Workers[@]}"; do
  if ! timeout -k 2s 20s ssh "${SSH_COMMON[@]}" "${UserName}@${worker}" "WORKER_HOST='$worker' python3 -" >> "$Temporary" <<'PY'
import json
import os
from pathlib import Path

root = Path("/home/hadoop/bigdata-kafka/logs/spark_batches")
items = []

def process_is_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False

if root.exists():
    for path in sorted(root.glob("*.status.json")):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
            item.setdefault("worker", os.environ["WORKER_HOST"])
            lock_path = root / path.name.replace(".status.json", ".lock")
            pid_path = lock_path / "pid"
            try:
                pid = int(pid_path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                pid = 0
            item["active"] = bool(pid and process_is_alive(pid))
            if item.get("status") in {"queued", "running"} and not item["active"]:
                item["status"] = "failed"
                item["stale"] = True
                item["message"] = "La ejecución remota terminó sin conservar un proceso activo."
            items.append(item)
        except Exception as exc:
            items.append({"status": "failed", "worker": os.environ["WORKER_HOST"], "message": f"{path.name}: {exc}"})
print(json.dumps({"worker": os.environ["WORKER_HOST"], "reachable": True, "batches": items}, ensure_ascii=False))
PY
  then
    printf '{"worker":"%s","reachable":false,"batches":[]}\n' "$worker" >> "$Temporary"
  fi
done

python3 - "$Temporary" "$MaxConcurrency" <<'PY'
import json
import sys
from datetime import datetime, timezone

batches = {}
workers = []
max_concurrency = max(1, int(sys.argv[2]))
for raw in open(sys.argv[1], encoding="utf-8"):
    try:
        payload = json.loads(raw)
    except Exception:
        continue
    workers.append({"host": payload.get("worker", ""), "reachable": payload.get("reachable", False)})
    for batch in payload.get("batches", []):
        batch_id = batch.get("batch_id")
        if batch_id:
            batches[batch_id] = batch
latest = max(batches, key=lambda key: int(batches[key].get("range_end", 0)), default="")
active_batches = sum(1 for batch in batches.values() if batch.get("status") in {"queued", "running"})
print(json.dumps({
    "ok": all(item["reachable"] for item in workers),
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "latest_batch_id": latest,
    "batches": batches,
    "workers": workers,
    "active_batches": active_batches,
    "max_concurrency": max_concurrency,
}, ensure_ascii=False))
PY
