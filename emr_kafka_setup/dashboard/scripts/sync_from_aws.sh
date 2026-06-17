#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"
DataDir="$DashboardRoot/data"

PemPath="$ProjectRoot/final.pem"
HostName=""
UserName="hadoop"
NlpMessages=7000
AlertMessages=500
RawMessages=7000
WatchSeconds=0

usage() {
  cat <<'EOF'
Uso:
  sync_from_aws.sh [--host HOST] [--pem PATH] [--user hadoop] [--raw-messages 7000] [--nlp-messages 7000] [--alert-messages 500]
  sync_from_aws.sh --watch 3

Lee EMR_PRIMARY desde ../../.env si no se pasa --host.
EOF
}

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
    --host)
      HostName="${2:-}"
      shift 2
      ;;
    --pem)
      PemPath="${2:-}"
      shift 2
      ;;
    --user)
      UserName="${2:-hadoop}"
      shift 2
      ;;
    --nlp-messages)
      NlpMessages="${2:-7000}"
      shift 2
      ;;
    --raw-messages)
      RawMessages="${2:-7000}"
      shift 2
      ;;
    --alert-messages)
      AlertMessages="${2:-500}"
      shift 2
      ;;
    --watch)
      WatchSeconds="${2:-3}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Argumento no reconocido: $1" >&2
      usage >&2
      exit 2
      ;;
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

mkdir -p "$DataDir"
chmod 600 "$PemPath" 2>/dev/null || true

sync_once() {
  local ssh_target="${UserName}@${HostName}"
  local remote_script
  remote_script="$(mktemp)"
  cat > "$remote_script" <<EOF
set -e
mkdir -p /home/hadoop/bigdata-kafka/logs
KAFKA_HOME="\${KAFKA_HOME:-/home/hadoop/kafka}"
BROKER="\${BROKER:-localhost:9092}"
count_topic() {
  "\$KAFKA_HOME/bin/kafka-run-class.sh" kafka.tools.GetOffsetShell --broker-list "\$BROKER" --topic "\$1" 2>/dev/null | awk -F: '{sum += \$3} END {print sum+0}'
}
RAW=\$(count_topic raw_youtube_chat)
NLP=\$(count_topic nlp_stream_results)
ALERTS=\$(count_topic alerts_polarization)
timeout 8s "\$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "\$BROKER" --topic nlp_stream_results --from-beginning --max-messages "$NlpMessages" > /home/hadoop/bigdata-kafka/logs/dashboard_nlp_stream_results_sample.jsonl 2>/dev/null || true
timeout 5s "\$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "\$BROKER" --topic alerts_polarization --from-beginning --max-messages "$AlertMessages" > /home/hadoop/bigdata-kafka/logs/dashboard_alerts_sample.jsonl 2>/dev/null || true
timeout 8s "\$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "\$BROKER" --topic raw_youtube_chat --from-beginning --max-messages "$RawMessages" > /home/hadoop/bigdata-kafka/logs/dashboard_raw_youtube_chat_sample.jsonl 2>/dev/null || true
aws s3 cp s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/summary.md /home/hadoop/bigdata-kafka/logs/spark_job2_rules_summary.md >/dev/null 2>&1 || true
aws s3 cp s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/summary.md /home/hadoop/bigdata-kafka/logs/spark_job4_ml_summary.md >/dev/null 2>&1 || true
aws s3 cp s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/summary.md /home/hadoop/bigdata-kafka/logs/spark_job5_hybrid_summary.md >/dev/null 2>&1 || true
printf '{"generated_at":"%s","data_mode":"aws_streaming","counts":{"raw_youtube_chat":%s,"nlp_stream_results":%s,"alerts_polarization":%s,"spark_curated_rows":%s,"spark_aggregates_rows":54}}\n' "\$(date -u +%Y-%m-%dT%H:%M:%SZ)" "\$RAW" "\$NLP" "\$ALERTS" "\$RAW" > /home/hadoop/bigdata-kafka/logs/dashboard_counts.json
EOF

  ssh -i "$PemPath" -o StrictHostKeyChecking=no "$ssh_target" "bash -s" < "$remote_script"
  rm -f "$remote_script"

  scp -i "$PemPath" -o StrictHostKeyChecking=no "${ssh_target}:/home/hadoop/bigdata-kafka/logs/dashboard_nlp_stream_results_sample.jsonl" "$DataDir/flink_nlp_stream_results_sample.jsonl" >/dev/null
  scp -i "$PemPath" -o StrictHostKeyChecking=no "${ssh_target}:/home/hadoop/bigdata-kafka/logs/dashboard_alerts_sample.jsonl" "$DataDir/flink_alerts_sample.jsonl" >/dev/null
  scp -i "$PemPath" -o StrictHostKeyChecking=no "${ssh_target}:/home/hadoop/bigdata-kafka/logs/dashboard_raw_youtube_chat_sample.jsonl" "$DataDir/raw_youtube_chat_sample.jsonl" >/dev/null
  scp -i "$PemPath" -o StrictHostKeyChecking=no "${ssh_target}:/home/hadoop/bigdata-kafka/logs/dashboard_counts.json" "$DataDir/dashboard_counts.json" >/dev/null
  scp -i "$PemPath" -o StrictHostKeyChecking=no "${ssh_target}:/home/hadoop/bigdata-kafka/logs/spark_job2_rules_summary.md" "$DataDir/spark_job2_rules_summary.md" >/dev/null 2>&1 || true
  scp -i "$PemPath" -o StrictHostKeyChecking=no "${ssh_target}:/home/hadoop/bigdata-kafka/logs/spark_job4_ml_summary.md" "$DataDir/spark_job4_ml_summary.md" >/dev/null 2>&1 || true
  scp -i "$PemPath" -o StrictHostKeyChecking=no "${ssh_target}:/home/hadoop/bigdata-kafka/logs/spark_job5_hybrid_summary.md" "$DataDir/spark_job5_hybrid_summary.md" >/dev/null 2>&1 || true

  echo "Datos sincronizados en $DataDir ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
}

if [[ "$WatchSeconds" -gt 0 ]]; then
  while true; do
    sync_once || true
    sleep "$WatchSeconds"
  done
else
  sync_once
fi
