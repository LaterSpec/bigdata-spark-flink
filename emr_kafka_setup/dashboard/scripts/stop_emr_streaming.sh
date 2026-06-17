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

ssh -i "$PemPath" -o StrictHostKeyChecking=no "${UserName}@${HostName}" "bash -s" <<'EOF'
set -euo pipefail

export PROJECT_HOME=/home/hadoop/bigdata-kafka
export KAFKA_HOME=/home/hadoop/kafka

for pattern in \
  "produce_youtube_chat_from_s3.py" \
  "flink_job1_normalize_stream.sh" \
  "flink_job2_window_metrics.sh" \
  "flink_job3_political_signals.sh" \
  "flink_job4_actor_polarization.sh" \
  "flink_job5_risk_alerts.sh" \
  "spark_read_kafka_raw_youtube.py" \
  "spark_rules_from_kafka_parquet.py" \
  "spark_apply_offendes_from_kafka_parquet.py" \
  "spark_hybrid_scoring_from_kafka.py"; do
  pkill -f "$pattern" >/dev/null 2>&1 || true
done

if [[ -x "$KAFKA_HOME/bin/kafka-server-stop.sh" ]]; then
  "$KAFKA_HOME/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
fi

pkill -f "kafka.Kafka" >/dev/null 2>&1 || true
rm -f "$PROJECT_HOME/logs/spark_batch_status.json" >/dev/null 2>&1 || true
rm -rf "$PROJECT_HOME/logs/spark_batches"/*.lock >/dev/null 2>&1 || true

echo "Streaming EMR detenido."
EOF
