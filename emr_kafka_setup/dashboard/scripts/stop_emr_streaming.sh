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
  echo "EMR_PRIMARY, EMR_WORKERS o PEM no configurado." >&2
  exit 1
fi

SSH_COMMON=(-i "$PemPath" -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=no)
StopFailed=0

discover_primary() {
  if ! PRIMARY_PRIVATE="$(timeout -k 5s 35s ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" "hostname -f")"; then
    echo "No se pudo acceder a EMR_PRIMARY." >&2
    return 1
  fi
  if ! JOB_FLOW_ID="$(timeout -k 5s 35s ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" "grep -o 'j-[A-Z0-9]*' /mnt/var/lib/info/job-flow.json | head -1")"; then
    echo "No se pudo descubrir el cluster id de EMR_PRIMARY." >&2
    return 1
  fi
  if ! DISCOVERED_NODES="$(timeout -k 5s 45s ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" \
    "aws emr list-instances --cluster-id '$JOB_FLOW_ID' --query 'Instances[?Status.State==\`RUNNING\`].PrivateDnsName' --output text")"; then
    echo "No se pudo descubrir el inventario de brokers Kafka." >&2
    return 1
  fi

  BrokerNodes=("$PRIMARY_PRIVATE")
  while IFS= read -r node; do
    [[ -z "$node" || "$node" == "$PRIMARY_PRIVATE" ]] || BrokerNodes+=("$node")
  done < <(printf '%s' "$DISCOVERED_NODES" | tr '\t ' '\n' | awk 'NF')
  if [[ "${#BrokerNodes[@]}" -ne 3 ]]; then
    echo "Se esperaban 3 nodos Kafka y se descubrieron ${#BrokerNodes[@]}; se intentará detener los encontrados." >&2
    StopFailed=1
  fi

  PROXY_COMMAND="ssh -i $PemPath -o BatchMode=yes -o StrictHostKeyChecking=no -W %h:%p ${UserName}@${PrimaryHost}"
}

ssh_broker() {
  local node="$1"
  shift
  if [[ "$node" == "$PRIMARY_PRIVATE" ]]; then
    timeout -k 5s 45s ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" "$@"
  else
    timeout -k 5s 45s ssh "${SSH_COMMON[@]}" -o "ProxyCommand=$PROXY_COMMAND" "${UserName}@${node}" "$@"
  fi
}

IFS=',; ' read -r -a Workers <<< "$WorkerHosts"
WorkerCount=0
for worker in "${Workers[@]}"; do
  [[ -n "$worker" ]] || continue
  WorkerCount=$((WorkerCount + 1))
  echo "Deteniendo compute EMR: $worker"
  if ! timeout -k 5s 90s ssh "${SSH_COMMON[@]}" "${UserName}@${worker}" "bash -s" <<'REMOTE'
set -u
set -o pipefail
PROJECT_HOME=/home/hadoop/bigdata-kafka
BATCH_DIR="$PROJECT_HOME/logs/spark_batches"

list_active_yarn_apps() {
  yarn application -list -appStates NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING 2>/dev/null |
    awk '$1 ~ /^application_/ {print $1}'
}

if ! ACTIVE_APPS="$(list_active_yarn_apps)"; then
  echo "No se pudo consultar YARN." >&2
  exit 1
fi
while IFS= read -r application_id; do
  [[ -n "$application_id" ]] || continue
  yarn application -kill "$application_id" >/dev/null 2>&1 || true
done <<< "$ACTIVE_APPS"

for pattern in \
  "FlinkKafkaStreamingJobs" \
  "flink_job" \
  "spark_batches/run_" \
  "spark_read_kafka_raw_youtube.py" \
  "spark_rules_from_kafka_parquet.py" \
  "spark_apply_offendes_from_kafka_parquet.py" \
  "spark_hybrid_scoring_from_kafka.py" \
  "spark_comment_extract"; do
  pkill -TERM -f "$pattern" >/dev/null 2>&1 || true
done

for _ in $(seq 1 20); do
  ACTIVE_APPS="$(list_active_yarn_apps)" || exit 1
  if [[ -z "$ACTIVE_APPS" ]] &&
    ! pgrep -f "FlinkKafkaStreamingJobs|flink_job|spark_batches/run_|spark_read_kafka_raw_youtube.py|spark_rules_from_kafka_parquet.py|spark_apply_offendes_from_kafka_parquet.py|spark_hybrid_scoring_from_kafka.py|spark_comment_extract" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

pkill -KILL -f "FlinkKafkaStreamingJobs|flink_job|spark_batches/run_|spark_read_kafka_raw_youtube.py|spark_rules_from_kafka_parquet.py|spark_apply_offendes_from_kafka_parquet.py|spark_hybrid_scoring_from_kafka.py|spark_comment_extract" >/dev/null 2>&1 || true
ACTIVE_APPS="$(list_active_yarn_apps)" || exit 1
if [[ -n "$ACTIVE_APPS" ]]; then
  echo "Persisten aplicaciones YARN activas: $ACTIVE_APPS" >&2
  exit 1
fi
if pgrep -f "FlinkKafkaStreamingJobs|flink_job|spark_batches/run_|spark_read_kafka_raw_youtube.py|spark_rules_from_kafka_parquet.py|spark_apply_offendes_from_kafka_parquet.py|spark_hybrid_scoring_from_kafka.py|spark_comment_extract" >/dev/null 2>&1; then
  echo "Persisten procesos Flink o Spark." >&2
  exit 1
fi

rm -rf "$BATCH_DIR"
mkdir -p "$BATCH_DIR"
REMOTE
  then
    echo "No se pudo confirmar la detención del compute $worker." >&2
    StopFailed=1
  fi
done
if [[ "$WorkerCount" -eq 0 ]]; then
  echo "EMR_WORKERS no contiene endpoints validos." >&2
  StopFailed=1
fi

PrimaryReady=1
if ! discover_primary; then
  PrimaryReady=0
  StopFailed=1
fi

if [[ "$PrimaryReady" -eq 1 ]]; then
echo "Deteniendo producer y monitor en EMR_PRIMARY."
if ! timeout -k 5s 60s ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" "bash -s" <<'REMOTE'
set -u
LOG_DIR=/home/hadoop/bigdata-kafka/logs
pkill -TERM -f "produce_youtube_chat_from_s3.py|monitor_kafka_flow.py" >/dev/null 2>&1 || true
for _ in $(seq 1 15); do
  if ! pgrep -f "produce_youtube_chat_from_s3.py|monitor_kafka_flow.py" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
pkill -KILL -f "produce_youtube_chat_from_s3.py|monitor_kafka_flow.py" >/dev/null 2>&1 || true
rm -f \
  "$LOG_DIR/kafka_flow_health.json" \
  "$LOG_DIR/kafka_flow_history.jsonl" \
  "$LOG_DIR/kafka_flow_monitor.log" \
  "$LOG_DIR/producer_youtube_chat_stream.log"
if pgrep -f "produce_youtube_chat_from_s3.py|monitor_kafka_flow.py" >/dev/null 2>&1; then
  echo "Persisten producer o monitor en EMR_PRIMARY." >&2
  exit 1
fi
REMOTE
then
  echo "No se pudo limpiar producer, monitor o salud en EMR_PRIMARY." >&2
  StopFailed=1
fi

for node in "${BrokerNodes[@]}"; do
  echo "Deteniendo broker Kafka: $node"
  if ! ssh_broker "$node" "if [[ -x /home/hadoop/bigdata-kafka/scripts/stop_kafka.sh ]]; then /home/hadoop/bigdata-kafka/scripts/stop_kafka.sh; else echo 'Kafka aun no fue desplegado'; fi" >/dev/null 2>&1; then
    echo "No se pudo detener Kafka en $node." >&2
    StopFailed=1
  fi
done
fi

if [[ "$StopFailed" -ne 0 ]]; then
  echo "La detención distribuida quedó incompleta; Conectar AWS debe permanecer bloqueado." >&2
  exit 1
fi

echo "EMR_PRIMARY y todos los EMR_WORKERS quedaron detenidos; salud y estados de batch fueron limpiados."
