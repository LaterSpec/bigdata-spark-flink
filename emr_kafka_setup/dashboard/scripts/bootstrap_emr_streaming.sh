#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
HostName=""
WorkerHosts=""
UserName="hadoop"
Bucket="s3://figuretibucket"
S3Path="s3://figuretibucket/data/raw/youtube/youtube_lake.csv"
Limit=""
ProducerDelayMs=10
FlinkDelayMs=5
WindowSeconds=5
ResetTopics=0

usage() {
  cat <<'EOF'
Uso:
  bootstrap_emr_streaming.sh [opciones]

Opciones:
  --host HOST                 Endpoint publico de EMR_PRIMARY.
  --workers HOST1,HOST2       Endpoints de clusters EMR de computo.
  --pem PATH                  Llave SSH local.
  --bucket S3_URI             Bucket base.
  --s3-path S3_URI            CSV del data lake.
  --limit N                   Eventos a publicar.
  --producer-delay-ms N       Pausa del producer.
  --flink-delay-ms N          Pausa de consumidores Flink.
  --window-seconds N          Ventana Flink.
  --reset-topics              Borra almacenamiento y offsets Kafka (destructivo).
  --no-reset-topics           Conserva almacenamiento y offsets Kafka (por defecto).

Sin argumentos lee EMR_PRIMARY, EMR_WORKERS y DATA_SIZE desde .env.
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
    --workers) WorkerHosts="${2:-}"; shift 2 ;;
    --pem) PemPath="${2:-}"; shift 2 ;;
    --user) UserName="${2:-hadoop}"; shift 2 ;;
    --bucket) Bucket="${2:-s3://figuretibucket}"; shift 2 ;;
    --s3-path) S3Path="${2:-}"; shift 2 ;;
    --limit) Limit="${2:-}"; shift 2 ;;
    --producer-delay-ms) ProducerDelayMs="${2:-10}"; shift 2 ;;
    --flink-delay-ms) FlinkDelayMs="${2:-5}"; shift 2 ;;
    --window-seconds) WindowSeconds="${2:-5}"; shift 2 ;;
    --reset-topics) ResetTopics=1; shift ;;
    --no-reset-topics) ResetTopics=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Argumento no reconocido: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$HostName" ]] || HostName="$(read_env_value EMR_PRIMARY || true)"
[[ -n "$WorkerHosts" ]] || WorkerHosts="$(read_env_value EMR_WORKERS || true)"
[[ -n "$Limit" ]] || Limit="$(read_env_value DATA_SIZE || true)"
[[ -n "$Limit" ]] || Limit=7000
SessionBatchSize="$(read_env_value SPARK_BATCH_SIZE || true)"
[[ -n "$SessionBatchSize" ]] || SessionBatchSize=1000
SessionMaxConcurrency="$(read_env_value SPARK_MAX_CONCURRENCY || true)"
[[ -n "$SessionMaxConcurrency" ]] || SessionMaxConcurrency=1

if [[ -z "$HostName" || -z "$WorkerHosts" ]]; then
  echo "Define EMR_PRIMARY y EMR_WORKERS en .env o usa --host/--workers." >&2
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

IFS=',; ' read -r -a ComputeHosts <<< "$WorkerHosts"
if [[ "${#ComputeHosts[@]}" -eq 0 ]]; then
  echo "EMR_WORKERS no contiene endpoints validos." >&2
  exit 1
fi

SSH_COMMON=(-i "$PemPath" -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=no)

ssh_primary() {
  ssh "${SSH_COMMON[@]}" "${UserName}@${HostName}" "$@"
}

scp_primary() {
  scp "${SSH_COMMON[@]}" "$@"
}

JOB_FLOW_ID="$(ssh_primary "grep -o 'j-[A-Z0-9]*' /mnt/var/lib/info/job-flow.json | head -1")"
PRIMARY_PRIVATE="$(ssh_primary "hostname -f")"
mapfile -t DiscoveredNodes < <(
  ssh_primary "aws emr list-instances --cluster-id '$JOB_FLOW_ID' --query 'Instances[?Status.State==\`RUNNING\`].PrivateDnsName' --output text" |
    tr '\t ' '\n' |
    awk 'NF'
)

BrokerNodes=("$PRIMARY_PRIVATE")
for node in "${DiscoveredNodes[@]}"; do
  [[ "$node" == "$PRIMARY_PRIVATE" ]] || BrokerNodes+=("$node")
done
if [[ "${#BrokerNodes[@]}" -ne 3 ]]; then
  echo "Kafka requiere exactamente 3 nodos RUNNING; se descubrieron ${#BrokerNodes[@]}." >&2
  exit 1
fi

PROXY_COMMAND="ssh -i $PemPath -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=no -W %h:%p ${UserName}@${HostName}"

ssh_broker() {
  local node="$1"
  shift
  if [[ "$node" == "$PRIMARY_PRIVATE" ]]; then
    ssh_primary "$@"
  else
    ssh "${SSH_COMMON[@]}" -o "ProxyCommand=$PROXY_COMMAND" "${UserName}@${node}" "$@"
  fi
}

scp_broker() {
  local node="$1"
  local source="$2"
  local destination="$3"
  if [[ "$node" == "$PRIMARY_PRIVATE" ]]; then
    scp_primary -r "$source" "${UserName}@${HostName}:$destination"
  else
    scp "${SSH_COMMON[@]}" -o "ProxyCommand=$PROXY_COMMAND" -r "$source" "${UserName}@${node}:$destination"
  fi
}

echo "Kafka inventory: ${BrokerNodes[*]}"
QUORUM=""
BROKER_LIST=""
for index in "${!BrokerNodes[@]}"; do
  node_id=$((index + 1))
  node="${BrokerNodes[$index]}"
  [[ -z "$QUORUM" ]] || QUORUM+=","
  [[ -z "$BROKER_LIST" ]] || BROKER_LIST+=","
  QUORUM+="${node_id}@${node}:9093"
  BROKER_LIST+="${node}:9092"
done

for index in "${!BrokerNodes[@]}"; do
  node_id=$((index + 1))
  node="${BrokerNodes[$index]}"
  echo "Preparando Kafka broker ${node_id} en ${node}"
  ssh_broker "$node" "mkdir -p /home/hadoop/bigdata-kafka/{config,scripts,logs}"
  scp_broker "$node" "$ProjectRoot/emr_kafka_setup/config/." "/home/hadoop/bigdata-kafka/config/"
  scp_broker "$node" "$ProjectRoot/emr_kafka_setup/scripts/." "/home/hadoop/bigdata-kafka/scripts/"
  ssh_broker "$node" "bash -s" <<EOF
set -euo pipefail
export KAFKA_HOME=/home/hadoop/kafka
export PROJECT_HOME=/home/hadoop/bigdata-kafka
if [[ ! -x "\$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  cd /home/hadoop
  if [[ ! -f kafka_2.12-3.6.2.tgz ]]; then
    aws s3 cp "$Bucket/bootstrap/kafka_2.12-3.6.2.tgz" kafka_2.12-3.6.2.tgz --only-show-errors ||
      curl -fL -o kafka_2.12-3.6.2.tgz https://archive.apache.org/dist/kafka/3.6.2/kafka_2.12-3.6.2.tgz
  fi
  tar -xzf kafka_2.12-3.6.2.tgz
  ln -sfn /home/hadoop/kafka_2.12-3.6.2 "\$KAFKA_HOME"
fi
sed \
  -e "s/__NODE_ID__/${node_id}/g" \
  -e "s|__CONTROLLER_QUORUM__|${QUORUM}|g" \
  -e "s/__ADVERTISED_HOST__/${node}/g" \
  "\$PROJECT_HOME/config/kraft-server.properties.template" > "\$PROJECT_HOME/config/kraft-server.properties"
chmod +x "\$PROJECT_HOME"/scripts/*.sh "\$PROJECT_HOME"/scripts/*.py
EOF
done

if [[ "$ResetTopics" == "1" ]]; then
  echo "Deteniendo brokers y reinicializando KRaft."
  for node in "${BrokerNodes[@]}"; do
    ssh_broker "$node" "/home/hadoop/bigdata-kafka/scripts/stop_kafka.sh" >/dev/null 2>&1 || true
  done
fi

if [[ "$ResetTopics" == "1" ]]; then
  CLUSTER_ID="$(ssh_broker "$PRIMARY_PRIVATE" "/home/hadoop/kafka/bin/kafka-storage.sh random-uuid")"
else
  CLUSTER_ID="$(ssh_broker "$PRIMARY_PRIVATE" "awk -F= '\$1 == \"cluster.id\" {print \$2}' /home/hadoop/bigdata-kafka/kraft-combined-logs/meta.properties 2>/dev/null || true")"
  [[ -n "$CLUSTER_ID" ]] || CLUSTER_ID="$(ssh_broker "$PRIMARY_PRIVATE" "/home/hadoop/kafka/bin/kafka-storage.sh random-uuid")"
fi
for node in "${BrokerNodes[@]}"; do
  ssh_broker "$node" "RESET_STORAGE='$ResetTopics' WAIT_FOR_READY=0 CLUSTER_ID='$CLUSTER_ID' /home/hadoop/bigdata-kafka/scripts/start_kafka.sh"
done

echo "Esperando quorum Kafka de tres brokers."
for _ in $(seq 1 45); do
  if ssh_primary "/home/hadoop/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
ssh_primary "REPLICATION_FACTOR=3 BOOTSTRAP_SERVER=localhost:9092 /home/hadoop/bigdata-kafka/scripts/create_topics.sh"
ssh_primary "/home/hadoop/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status"

# El producer se inicia apenas Kafka está listo. No debe esperar a que el
# despliegue de compute termine para comenzar a alimentar la muestra.
echo "Iniciando producer y monitor de streaming en EMR_PRIMARY."
ssh_primary "mkdir -p /home/hadoop/bigdata-kafka/producers"
scp_primary -r "$ProjectRoot/emr_kafka_setup/producers/." \
  "${UserName}@${HostName}:/home/hadoop/bigdata-kafka/producers/"

ssh_primary "bash -s" <<EOF
set -euo pipefail
PROJECT_HOME=/home/hadoop/bigdata-kafka
python3 - <<'PY'
import json
from pathlib import Path

Path("/home/hadoop/bigdata-kafka/logs/session_config.json").write_text(
    json.dumps({
        "expected_messages": int("$Limit"),
        "spark_batch_size": int("$SessionBatchSize"),
        "spark_max_concurrency": int("$SessionMaxConcurrency"),
    }),
    encoding="utf-8",
)
PY
python3 - <<'PY' || python3 -m pip install --user boto3 kafka-python
import boto3
import kafka
PY
pkill -f "monitor_kafka_flow.py" >/dev/null 2>&1 || true
nohup python3 "\$PROJECT_HOME/scripts/monitor_kafka_flow.py" --follow --bootstrap-server localhost:9092 \
  </dev/null > "\$PROJECT_HOME/logs/kafka_flow_monitor.log" 2>&1 &
sleep 5
pkill -f "produce_youtube_chat_from_s3.py" >/dev/null 2>&1 || true
ExistingRaw="\$(/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic raw_youtube_chat 2>/dev/null |
  awk -F: '{total += \$3} END {print total + 0}')"
Remaining=\$(( $Limit - ExistingRaw ))
if (( Remaining > 0 )); then
  StartRow=\$((ExistingRaw + 1))
  nohup python3 "\$PROJECT_HOME/producers/produce_youtube_chat_from_s3.py" \
    --s3-path "$S3Path" \
    --bootstrap-server localhost:9092 \
    --topic raw_youtube_chat \
    --start-row "\$StartRow" \
    --limit "\$Remaining" \
    --delay-ms "$ProducerDelayMs" \
    --log-every 100 \
    </dev/null > "\$PROJECT_HOME/logs/producer_youtube_chat_stream.log" 2>&1 &
else
  printf 'raw_youtube_chat ya contiene %s mensajes; no se relanza el producer.\\n' "\$ExistingRaw" \
    > "\$PROJECT_HOME/logs/producer_youtube_chat_stream.log"
fi
EOF

echo "Desplegando codigo en ${#ComputeHosts[@]} cluster(es) de computo."
for compute in "${ComputeHosts[@]}"; do
  ssh "${SSH_COMMON[@]}" "${UserName}@${compute}" \
    "mkdir -p /home/hadoop/bigdata-kafka/{spark/jobs,flink/jobs,flink/scripts,flink/upload,logs}"
  scp "${SSH_COMMON[@]}" -r "$ProjectRoot/emr_kafka_setup/spark/jobs/." \
    "${UserName}@${compute}:/home/hadoop/bigdata-kafka/spark/jobs/"
  scp "${SSH_COMMON[@]}" -r "$ProjectRoot/emr_kafka_setup/flink/jobs/." \
    "${UserName}@${compute}:/home/hadoop/bigdata-kafka/flink/jobs/"
  scp "${SSH_COMMON[@]}" -r "$ProjectRoot/emr_kafka_setup/flink/scripts/." \
    "${UserName}@${compute}:/home/hadoop/bigdata-kafka/flink/scripts/"
  ssh "${SSH_COMMON[@]}" "${UserName}@${compute}" "BUCKET='$Bucket' RESET_TOPICS='$ResetTopics' bash -s" <<'REMOTE'
set -euo pipefail
if [[ ! -f /home/hadoop/kafka/libs/kafka-clients-3.6.2.jar ]]; then
  cd /home/hadoop
  if [[ ! -f kafka_2.12-3.6.2.tgz ]]; then
    aws s3 cp "$BUCKET/bootstrap/kafka_2.12-3.6.2.tgz" kafka_2.12-3.6.2.tgz --only-show-errors ||
      curl -fL -o kafka_2.12-3.6.2.tgz https://archive.apache.org/dist/kafka/3.6.2/kafka_2.12-3.6.2.tgz
  fi
  tar -xzf kafka_2.12-3.6.2.tgz
  ln -sfn /home/hadoop/kafka_2.12-3.6.2 /home/hadoop/kafka
fi
chmod +x /home/hadoop/bigdata-kafka/flink/scripts/*.sh
pkill -f "FlinkKafkaStreamingJobs" >/dev/null 2>&1 || true
if [[ "$RESET_TOPICS" == "1" ]]; then
  rm -rf /home/hadoop/bigdata-kafka/logs/spark_batches
  mkdir -p /home/hadoop/bigdata-kafka/logs/spark_batches
fi
REMOTE
done

FlinkHost="${ComputeHosts[0]}"
ssh "${SSH_COMMON[@]}" "${UserName}@${FlinkHost}" "bash -s" <<EOF
set -euo pipefail
PROJECT_HOME=/home/hadoop/bigdata-kafka
"\$PROJECT_HOME/flink/scripts/build_flink_jobs.sh"
launch_job() {
  local name="\$1"
  local script="\$2"
  echo "Lanzando \$name en compute"
  nohup env BOOTSTRAP_SERVER="$BROKER_LIST" MAX_MESSAGES="$Limit" DELAY_MS="$FlinkDelayMs" WINDOW_SECONDS="$WindowSeconds" IDLE_MS=15000 "\$script" \
    </dev/null > "\$PROJECT_HOME/logs/\${name}.log" 2>&1 &
}
launch_job flink_job1_normalize_stream "\$PROJECT_HOME/flink/scripts/flink_job1_normalize_stream.sh"
launch_job flink_job2_window_metrics "\$PROJECT_HOME/flink/scripts/flink_job2_window_metrics.sh"
launch_job flink_job3_political_signals "\$PROJECT_HOME/flink/scripts/flink_job3_political_signals.sh"
launch_job flink_job4_actor_polarization "\$PROJECT_HOME/flink/scripts/flink_job4_actor_polarization.sh"
launch_job flink_job5_risk_alerts "\$PROJECT_HOME/flink/scripts/flink_job5_risk_alerts.sh"
EOF

echo "Arquitectura distribuida iniciada."
echo "Kafka brokers: ${#BrokerNodes[@]}; compute clusters: ${#ComputeHosts[@]}; limit: $Limit."
