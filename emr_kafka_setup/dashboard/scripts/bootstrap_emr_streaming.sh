#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
HostName=""
UserName="hadoop"
Bucket="s3://figuretibucket"
S3Path="s3://figuretibucket/data/raw/youtube/youtube_lake.csv"
Limit=""
ProducerDelayMs=10
FlinkDelayMs=5
WindowSeconds=5
ResetTopics=1

usage() {
  cat <<'EOF'
Uso:
  bootstrap_emr_streaming.sh [--host HOST] [--pem PATH] [--bucket s3://figuretibucket] [--s3-path s3://...csv] [--no-reset-topics]

Prepara EMR para la prueba local desde macOS:
  S3 CSV -> producer con delay -> Kafka -> Flink jobs -> topics de dashboard.
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
    --host) HostName="${2:-}"; shift 2 ;;
    --pem) PemPath="${2:-}"; shift 2 ;;
    --user) UserName="${2:-hadoop}"; shift 2 ;;
    --bucket) Bucket="${2:-s3://figuretibucket}"; shift 2 ;;
    --s3-path) S3Path="${2:-}"; shift 2 ;;
    --limit) Limit="${2:-7000}"; shift 2 ;;
    --producer-delay-ms) ProducerDelayMs="${2:-10}"; shift 2 ;;
    --flink-delay-ms) FlinkDelayMs="${2:-5}"; shift 2 ;;
    --window-seconds) WindowSeconds="${2:-5}"; shift 2 ;;
    --no-reset-topics) ResetTopics=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Argumento no reconocido: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$HostName" ]]; then
  HostName="$(read_env_value EMR_PRIMARY || true)"
fi

if [[ -z "$Limit" ]]; then
  Limit="$(read_env_value DATA_SIZE || true)"
fi

if [[ -z "$Limit" ]]; then
  Limit=7000
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

ssh -i "$PemPath" -o StrictHostKeyChecking=no "${UserName}@${HostName}" "bash -s" <<EOF
set -euo pipefail

export PROJECT_HOME=/home/hadoop/bigdata-kafka
export KAFKA_HOME=/home/hadoop/kafka
export BUCKET="$Bucket"
export S3_PATH="$S3Path"
export LIMIT="$Limit"
export PRODUCER_DELAY_MS="$ProducerDelayMs"
export FLINK_DELAY_MS="$FlinkDelayMs"
export WINDOW_SECONDS="$WindowSeconds"
export RESET_TOPICS="$ResetTopics"

mkdir -p "\$PROJECT_HOME"/{config,scripts,producers,spark/jobs,flink/jobs,flink/scripts,docs,logs,flink/upload}

aws s3 sync "\$BUCKET/docs/kafka/" "\$PROJECT_HOME/docs/" || true
aws s3 sync "\$BUCKET/codes/kafka/scripts/" "\$PROJECT_HOME/scripts/"
aws s3 sync "\$BUCKET/codes/kafka/config/" "\$PROJECT_HOME/config/"
aws s3 sync "\$BUCKET/codes/kafka/producers/" "\$PROJECT_HOME/producers/"
aws s3 sync "\$BUCKET/codes/kafka/flink/" "\$PROJECT_HOME/flink/"
aws s3 cp "\$BUCKET/codes/kafka/spark_read_kafka_raw_youtube.py" "\$PROJECT_HOME/spark/jobs/" || true
aws s3 cp "\$BUCKET/codes/kafka/spark_rules_from_kafka_parquet.py" "\$PROJECT_HOME/spark/jobs/" || true
aws s3 cp "\$BUCKET/codes/kafka/spark_apply_offendes_from_kafka_parquet.py" "\$PROJECT_HOME/spark/jobs/" || true
aws s3 cp "\$BUCKET/codes/kafka/spark_hybrid_scoring_from_kafka.py" "\$PROJECT_HOME/spark/jobs/" || true

if [[ ! -x "\$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  cd /home/hadoop
  if [[ ! -f kafka_2.12-3.6.2.tgz ]]; then
    curl -fL -o kafka_2.12-3.6.2.tgz https://archive.apache.org/dist/kafka/3.6.2/kafka_2.12-3.6.2.tgz
  fi
  tar -xzf kafka_2.12-3.6.2.tgz
  ln -sfn /home/hadoop/kafka_2.12-3.6.2 "\$KAFKA_HOME"
fi

PRIVATE_DNS=\$(hostname -f)
sed "s/__ADVERTISED_HOST__/\${PRIVATE_DNS}/g" "\$PROJECT_HOME/config/kraft-server.properties.template" > "\$PROJECT_HOME/config/kraft-server.properties"
chmod +x "\$PROJECT_HOME"/scripts/*.sh "\$PROJECT_HOME"/flink/scripts/*.sh || true

for pattern in \
  "produce_youtube_chat_from_s3.py" \
  "flink_job1_normalize_stream.sh" \
  "flink_job2_window_metrics.sh" \
  "flink_job3_political_signals.sh" \
  "flink_job4_actor_polarization.sh" \
  "flink_job5_risk_alerts.sh"; do
  pkill -f "\$pattern" >/dev/null 2>&1 || true
done

if [[ -f "\$PROJECT_HOME/flink/FlinkKafkaStreamingJobs.java" && ! -f "\$PROJECT_HOME/flink/jobs/FlinkKafkaStreamingJobs.java" ]]; then
  cp "\$PROJECT_HOME/flink/FlinkKafkaStreamingJobs.java" "\$PROJECT_HOME/flink/jobs/FlinkKafkaStreamingJobs.java"
fi

if [[ -f "\$PROJECT_HOME/flink/flink-streaming-jobs.jar" && ! -f "\$PROJECT_HOME/flink/jobs/flink-streaming-jobs.jar" ]]; then
  cp "\$PROJECT_HOME/flink/flink-streaming-jobs.jar" "\$PROJECT_HOME/flink/jobs/flink-streaming-jobs.jar"
fi

python3 - <<'PY' || python3 -m pip install --user boto3 kafka-python
import boto3
import kafka
PY

"\$PROJECT_HOME/scripts/start_kafka.sh"
if [[ "\$RESET_TOPICS" == "1" ]]; then
  echo "Recreando topics streaming para iniciar la sesion en cero"
  for topic in raw_youtube_chat nlp_stream_results alerts_polarization nlp_batch_results; do
    "\$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --delete --topic "\$topic" >/dev/null 2>&1 || true
  done
  sleep 5
fi
"\$PROJECT_HOME/scripts/create_topics.sh"
"\$PROJECT_HOME/scripts/status_kafka.sh" || true

if [[ ! -f "\$PROJECT_HOME/flink/jobs/flink-streaming-jobs.jar" ]]; then
  "\$PROJECT_HOME/flink/scripts/build_flink_jobs.sh"
fi

BOOTSTRAP="\${PRIVATE_DNS}:9092"

launch_job() {
  local name="\$1"
  local script="\$2"
  shift 2
  if pgrep -f "\$script" >/dev/null 2>&1; then
    echo "\$name ya esta en ejecucion"
    return 0
  fi
  echo "Lanzando \$name"
  nohup env BOOTSTRAP_SERVER="\$BOOTSTRAP" MAX_MESSAGES="\$LIMIT" DELAY_MS="\$FLINK_DELAY_MS" WINDOW_SECONDS="\$WINDOW_SECONDS" IDLE_MS=15000 "\$script" "\$@" \
    > "\$PROJECT_HOME/logs/\${name}.log" 2>&1 &
}

launch_job flink_job1_normalize_stream "\$PROJECT_HOME/flink/scripts/flink_job1_normalize_stream.sh"
launch_job flink_job2_window_metrics "\$PROJECT_HOME/flink/scripts/flink_job2_window_metrics.sh"
launch_job flink_job3_political_signals "\$PROJECT_HOME/flink/scripts/flink_job3_political_signals.sh"
launch_job flink_job4_actor_polarization "\$PROJECT_HOME/flink/scripts/flink_job4_actor_polarization.sh"
launch_job flink_job5_risk_alerts "\$PROJECT_HOME/flink/scripts/flink_job5_risk_alerts.sh"

sleep 8

echo "Lanzando producer streaming desde S3 con limit=\$LIMIT delay=\${PRODUCER_DELAY_MS}ms"
nohup python3 "\$PROJECT_HOME/producers/produce_youtube_chat_from_s3.py" \
  --s3-path "\$S3_PATH" \
  --bootstrap-server localhost:9092 \
  --topic raw_youtube_chat \
  --limit "\$LIMIT" \
  --delay-ms "\$PRODUCER_DELAY_MS" \
  --log-every 100 \
  > "\$PROJECT_HOME/logs/producer_youtube_chat_7k_stream.log" 2>&1 &

echo "EMR listo para streaming."
echo "Dashboard: ejecuta en tu Mac ./start_dashboard.sh y pulsa Conectar AWS."
EOF
