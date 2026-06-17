#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
HostName=""
UserName="hadoop"

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
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$HostName" ]]; then HostName="$(read_env_value EMR_PRIMARY || true)"; fi
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$HostName" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"error":"host o PEM no configurado"}'
  exit 0
fi

chmod 600 "$PemPath" 2>/dev/null || true

ssh -i "$PemPath" -o StrictHostKeyChecking=no "${UserName}@${HostName}" "python3 -" <<'PY'
import json
from pathlib import Path
from datetime import datetime, timezone

status_path = Path("/home/hadoop/bigdata-kafka/logs/spark_batch_status.json")
if not status_path.exists():
    print(json.dumps({
        "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batches": {},
        "latest_batch_id": "",
    }))
else:
    try:
        data = json.loads(status_path.read_text())
    except Exception as exc:
        data = {"ok": False, "error": str(exc), "batches": {}}
    data["ok"] = data.get("ok", True)
    data["generated_at"] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(data, ensure_ascii=False))
PY
