#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
PrimaryHost=""
WorkerHosts=""
UserName="hadoop"
BatchId=""
TargetCount=""
SparkBatchSize="${SPARK_BATCH_SIZE:-1000}"
MaxConcurrency="${SPARK_MAX_CONCURRENCY:-1}"
Bucket="s3://figuretibucket"
Force=0

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
    --host) PrimaryHost="${2:-}"; shift 2 ;;
    --workers) WorkerHosts="${2:-}"; shift 2 ;;
    --pem) PemPath="${2:-}"; shift 2 ;;
    --user) UserName="${2:-hadoop}"; shift 2 ;;
    --batch-id) BatchId="${2:-}"; shift 2 ;;
    --target-count) TargetCount="${2:-}"; shift 2 ;;
    --spark-batch-size) SparkBatchSize="${2:-1000}"; shift 2 ;;
    --max-concurrency) MaxConcurrency="${2:-1}"; shift 2 ;;
    --bucket) Bucket="${2:-s3://figuretibucket}"; shift 2 ;;
    --force) Force=1; shift ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$PrimaryHost" ]] || PrimaryHost="$(read_env_value EMR_PRIMARY || true)"
[[ -n "$WorkerHosts" ]] || WorkerHosts="$(read_env_value EMR_WORKERS || true)"
if [[ -z "$TargetCount" || ! "$TargetCount" =~ ^[0-9]+$ || "$TargetCount" -le 0 ]]; then
  echo '{"ok":false,"error":"falta --target-count valido"}'
  exit 0
fi
if [[ -z "$SparkBatchSize" || ! "$SparkBatchSize" =~ ^[0-9]+$ || "$SparkBatchSize" -le 0 ]]; then
  echo '{"ok":false,"error":"SPARK_BATCH_SIZE invalido"}'
  exit 0
fi
if [[ -z "$MaxConcurrency" || ! "$MaxConcurrency" =~ ^[0-9]+$ || "$MaxConcurrency" -le 0 ]]; then
  echo '{"ok":false,"error":"SPARK_MAX_CONCURRENCY invalido"}'
  exit 0
fi
if (( TargetCount % SparkBatchSize != 0 )); then
  echo "{\"ok\":false,\"error\":\"target debe ser multiplo de SPARK_BATCH_SIZE=$SparkBatchSize\"}"
  exit 0
fi
ExpectedBatchId="$(printf 'batch_%07d' "$TargetCount")"
[[ -n "$BatchId" ]] || BatchId="$ExpectedBatchId"
if [[ "$BatchId" != "$ExpectedBatchId" ]]; then
  echo "{\"ok\":false,\"error\":\"batch-id debe ser $ExpectedBatchId\"}"
  exit 0
fi
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$PrimaryHost" || -z "$WorkerHosts" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"error":"EMR_PRIMARY, EMR_WORKERS o PEM no configurado"}'
  exit 0
fi

IFS=',; ' read -r -a Workers <<< "$WorkerHosts"
WorkerCount="${#Workers[@]}"
if [[ "$WorkerCount" -eq 0 ]]; then
  echo '{"ok":false,"error":"EMR_WORKERS no contiene endpoints validos"}'
  exit 0
fi
BatchIndex=$(( (TargetCount + SparkBatchSize - 1) / SparkBatchSize ))
WorkerIndex=$(( (BatchIndex - 1) % WorkerCount ))
WorkerHost="${Workers[$WorkerIndex]}"
RangeStart=$((TargetCount - SparkBatchSize + 1))
(( RangeStart < 1 )) && RangeStart=1

chmod 600 "$PemPath" 2>/dev/null || true
SSH_COMMON=(-i "$PemPath" -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=no)
PrimaryPrivate="$(ssh "${SSH_COMMON[@]}" "${UserName}@${PrimaryHost}" "hostname -f")"
BootstrapServer="${PrimaryPrivate}:9092"

ssh "${SSH_COMMON[@]}" "${UserName}@${WorkerHost}" \
  "mkdir -p /home/hadoop/bigdata-kafka/spark/jobs /home/hadoop/bigdata-kafka/logs/spark_batches" >/dev/null
scp "${SSH_COMMON[@]}" \
  "$ProjectRoot/emr_kafka_setup/spark/jobs/spark_read_kafka_raw_youtube.py" \
  "$ProjectRoot/emr_kafka_setup/spark/jobs/spark_rules_from_kafka_parquet.py" \
  "$ProjectRoot/emr_kafka_setup/spark/jobs/spark_apply_offendes_from_kafka_parquet.py" \
  "$ProjectRoot/emr_kafka_setup/spark/jobs/spark_hybrid_scoring_from_kafka.py" \
  "${UserName}@${WorkerHost}:/home/hadoop/bigdata-kafka/spark/jobs/" >/dev/null

ssh "${SSH_COMMON[@]}" "${UserName}@${WorkerHost}" \
  "BATCH_ID='$BatchId' RANGE_START='$RangeStart' RANGE_END='$TargetCount' SPARK_BATCH_SIZE='$SparkBatchSize' MAX_CONCURRENCY='$MaxConcurrency' BUCKET='$Bucket' BOOTSTRAP_SERVER='$BootstrapServer' WORKER_HOST='$WorkerHost' FORCE='$Force' bash -s" <<'REMOTE'
set -euo pipefail

PROJECT_HOME=/home/hadoop/bigdata-kafka
LOG_DIR="$PROJECT_HOME/logs"
BATCH_DIR="$LOG_DIR/spark_batches"
LOCK_DIR="$BATCH_DIR/${BATCH_ID}.lock"
STATUS_PATH="$BATCH_DIR/${BATCH_ID}.status.json"
RUN_SCRIPT="$BATCH_DIR/run_${BATCH_ID}.sh"
SCHEDULER_LOCK="$BATCH_DIR/.scheduler.lock"

mkdir -p "$BATCH_DIR"
if ! command -v flock >/dev/null 2>&1; then
  echo '{"ok":false,"error":"flock no esta disponible en el worker"}'
  exit 0
fi
exec 9>"$SCHEDULER_LOCK"
if ! flock -w 10 9; then
  echo '{"ok":false,"busy":true,"retryable":true,"error":"scheduler Spark ocupado"}'
  exit 0
fi
if [[ -d "$LOCK_DIR" ]]; then
  ExistingPid=""
  [[ -f "$LOCK_DIR/pid" ]] && ExistingPid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
  if [[ "$ExistingPid" =~ ^[0-9]+$ ]] && kill -0 "$ExistingPid" 2>/dev/null; then
    echo "{\"ok\":true,\"message\":\"$BATCH_ID ya esta en ejecucion\",\"worker\":\"$WORKER_HOST\"}"
    exit 0
  fi
  rm -rf "$LOCK_DIR"
fi
if [[ -f "$STATUS_PATH" && "$FORCE" != "1" ]]; then
  python3 - "$STATUS_PATH" "$WORKER_HOST" <<'PY'
import json
import sys

try:
    status = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception as exc:
    print(json.dumps({
        "ok": False,
        "error": f"El estado existente no es legible: {exc}",
        "worker": sys.argv[2],
    }, ensure_ascii=False))
    raise SystemExit(0)
print(json.dumps({
    "ok": True,
    "already_exists": True,
    "batch_id": status.get("batch_id", ""),
    "status": status.get("status", "unknown"),
    "worker": status.get("worker", sys.argv[2]),
    "message": "El batch ya tiene estado persistido; no se relanza.",
}, ensure_ascii=False))
PY
  exit 0
fi
if [[ "$FORCE" == "1" ]]; then
  rm -f "$STATUS_PATH"
fi

ActiveBatches=0
for CandidateLock in "$BATCH_DIR"/batch_*.lock; do
  [[ -d "$CandidateLock" ]] || continue
  CandidatePid=""
  [[ -f "$CandidateLock/pid" ]] && CandidatePid="$(cat "$CandidateLock/pid" 2>/dev/null || true)"
  if [[ "$CandidatePid" =~ ^[0-9]+$ ]] && kill -0 "$CandidatePid" 2>/dev/null; then
    ActiveBatches=$((ActiveBatches + 1))
  else
    rm -rf "$CandidateLock"
  fi
done
if (( ActiveBatches >= MAX_CONCURRENCY )); then
  echo "{\"ok\":false,\"busy\":true,\"retryable\":true,\"active\":$ActiveBatches,\"max_concurrency\":$MAX_CONCURRENCY,\"error\":\"capacidad Spark ocupada\"}"
  exit 0
fi
mkdir "$LOCK_DIR"

cat > "$RUN_SCRIPT" <<'JOB'
#!/usr/bin/env bash
set -euo pipefail

PROJECT_HOME=/home/hadoop/bigdata-kafka
LOG_DIR="$PROJECT_HOME/logs"
BATCH_DIR="$LOG_DIR/spark_batches"
LOCK_DIR="$BATCH_DIR/${BATCH_ID}.lock"
STATUS_PATH="$BATCH_DIR/${BATCH_ID}.status.json"

BASE_RAW="$BUCKET/output/kafka_to_spark/raw_youtube_chat/${BATCH_ID}"
BASE_BATCH="$BUCKET/output/batch/from_kafka/${BATCH_ID}"
RAW_OUTPUT="$BASE_RAW/parquet/"
RAW_REPORT="$BASE_RAW/report/"
RULES_OUTPUT="$BASE_BATCH/job2_rules/"
RULES_REPORT="$BASE_BATCH/job2_rules_report/"
ML_OUTPUT="$BASE_BATCH/job4_ml_inference/"
ML_REPORT="$BASE_BATCH/job4_ml_report/"
HYBRID_OUTPUT="$BASE_BATCH/job5_hybrid/"
HYBRID_AGG="$BASE_BATCH/job5_hybrid_aggregates/"
HYBRID_REPORT="$BASE_BATCH/job5_hybrid_report/"

update_status() {
  local status="$1" job="$2" rows="$3" message="$4"
  python3 - "$STATUS_PATH" "$BATCH_ID" "$RANGE_START" "$RANGE_END" "$SPARK_BATCH_SIZE" "$WORKER_HOST" "$status" "$job" "$rows" "$message" "$RAW_OUTPUT" "$RULES_OUTPUT" "$ML_OUTPUT" "$HYBRID_OUTPUT" "$HYBRID_AGG" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
batch_id, range_start, range_end, size, worker, status, job, rows, message = sys.argv[2:11]
outputs = {
    "raw_parquet": sys.argv[11],
    "rules": sys.argv[12],
    "ml": sys.argv[13],
    "hybrid": sys.argv[14],
    "aggregates": sys.argv[15],
}
try:
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
except Exception:
    data = {}
now = datetime.now(timezone.utc).isoformat()
data.setdefault("batch_id", batch_id)
data.setdefault("created_at", now)
data.setdefault("jobs", {})
batch_status = "running" if status == "done" and job != "complete" else status
data.update({
    "range_start": int(range_start),
    "range_end": int(range_end),
    "target_count": int(range_end),
    "spark_batch_size": int(size),
    "worker": worker,
    "status": batch_status,
    "current_job": job,
    "updated_at": now,
    "message": message,
    "outputs": outputs,
})
if rows:
    try:
        data["rows"] = int(rows)
    except ValueError:
        pass
if job:
    data["jobs"][job] = {
        "status": status,
        "rows": int(rows or 0),
        "message": message,
        "updated_at": now,
    }
temporary = path.with_suffix(path.suffix + ".tmp")
temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
os.replace(temporary, path)
PY
}

extract_value() {
  local key="$1" file="$2"
  awk -F= -v key="$key" '$1 == key {print $2}' "$file" | tail -1 | tr -d '\r'
}

finish() {
  local code="$?"
  if [[ "$code" -ne 0 ]]; then
    update_status "failed" "${CURRENT_JOB:-unknown}" "${CURRENT_ROWS:-0}" "${FAILURE_MESSAGE:-Spark batch fallo con codigo $code}"
  fi
  rm -rf "$LOCK_DIR"
  exit "$code"
}
trap finish EXIT

update_status "running" "queued" "0" "Batch disjunto iniciado"
aws s3 rm "$BASE_RAW/" --recursive >/dev/null 2>&1 || true
aws s3 rm "$BASE_BATCH/" --recursive >/dev/null 2>&1 || true

CURRENT_JOB="job1_raw_from_kafka"
LOG1="$LOG_DIR/spark_${BATCH_ID}_job1.log"
update_status "running" "$CURRENT_JOB" "0" "Leyendo filas ${RANGE_START}-${RANGE_END} desde Kafka"
spark-submit --master yarn \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  "$PROJECT_HOME/spark/jobs/spark_read_kafka_raw_youtube.py" \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic raw_youtube_chat \
  --starting-offsets earliest \
  --ending-offsets latest \
  --min-row-number "$RANGE_START" \
  --max-row-number "$RANGE_END" \
  --output-path "$RAW_OUTPUT" \
  --report-path "$RAW_REPORT" \
  --coalesce 1 > "$LOG1" 2>&1
CURRENT_ROWS="$(extract_value TOTAL_RECORDS "$LOG1" || echo 0)"
if [[ ! "$CURRENT_ROWS" =~ ^[0-9]+$ ]] || [[ "$CURRENT_ROWS" -ne "$SPARK_BATCH_SIZE" ]]; then
  FAILURE_MESSAGE="El rango ${RANGE_START}-${RANGE_END} produjo ${CURRENT_ROWS:-0} filas; se esperaban $SPARK_BATCH_SIZE"
  exit 1
fi
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "Rango Kafka persistido en parquet"

CURRENT_JOB="job2_rules"
LOG2="$LOG_DIR/spark_${BATCH_ID}_job2.log"
update_status "running" "$CURRENT_JOB" "$CURRENT_ROWS" "Aplicando reglas locales"
spark-submit --master yarn "$PROJECT_HOME/spark/jobs/spark_rules_from_kafka_parquet.py" \
  --input "$RAW_OUTPUT" --output "$RULES_OUTPUT" --report-output "$RULES_REPORT" --coalesce 1 > "$LOG2" 2>&1
CURRENT_ROWS="$(extract_value OUTPUT_ROWS "$LOG2" || echo "$CURRENT_ROWS")"
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "Reglas locales completadas"

CURRENT_JOB="job4_offendes"
LOG4="$LOG_DIR/spark_${BATCH_ID}_job4.log"
update_status "running" "$CURRENT_JOB" "$CURRENT_ROWS" "Aplicando OffendES Spark ML"
spark-submit --master yarn "$PROJECT_HOME/spark/jobs/spark_apply_offendes_from_kafka_parquet.py" \
  --input "$RAW_OUTPUT" \
  --binary-model "$BUCKET/output/batch/models/offendes_binary_sparkml/" \
  --multiclass-model "$BUCKET/output/batch/models/offendes_multiclass_sparkml/" \
  --output "$ML_OUTPUT" --report-output "$ML_REPORT" --coalesce 1 > "$LOG4" 2>&1
CURRENT_ROWS="$(extract_value OUTPUT_ROWS "$LOG4" || echo "$CURRENT_ROWS")"
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "OffendES completado"

CURRENT_JOB="job5_hybrid"
LOG5="$LOG_DIR/spark_${BATCH_ID}_job5.log"
update_status "running" "$CURRENT_JOB" "$CURRENT_ROWS" "Generando scoring hibrido y agregados"
spark-submit --master yarn "$PROJECT_HOME/spark/jobs/spark_hybrid_scoring_from_kafka.py" \
  --rules-input "$RULES_OUTPUT" --ml-input "$ML_OUTPUT" \
  --output "$HYBRID_OUTPUT" --aggregates-output "$HYBRID_AGG" \
  --report-output "$HYBRID_REPORT" --coalesce 1 > "$LOG5" 2>&1
CURRENT_ROWS="$(extract_value OUTPUT_ROWS "$LOG5" || echo "$CURRENT_ROWS")"
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "Scoring hibrido completado"
update_status "done" "complete" "$CURRENT_ROWS" "Batch ${RANGE_START}-${RANGE_END} completado"
JOB

chmod +x "$RUN_SCRIPT"
nohup env \
  BATCH_ID="$BATCH_ID" RANGE_START="$RANGE_START" RANGE_END="$RANGE_END" \
  SPARK_BATCH_SIZE="$SPARK_BATCH_SIZE" BUCKET="$BUCKET" BOOTSTRAP_SERVER="$BOOTSTRAP_SERVER" \
  WORKER_HOST="$WORKER_HOST" "$RUN_SCRIPT" </dev/null > "$LOG_DIR/spark_${BATCH_ID}_driver.log" 2>&1 &
DriverPid=$!
if [[ -d "$LOCK_DIR" ]]; then
  printf '%s\n' "$DriverPid" > "$LOCK_DIR/pid"
fi

echo "{\"ok\":true,\"batch_id\":\"$BATCH_ID\",\"range_start\":$RANGE_START,\"range_end\":$RANGE_END,\"worker\":\"$WORKER_HOST\",\"max_concurrency\":$MAX_CONCURRENCY,\"message\":\"Spark batch lanzado\"}"
REMOTE
