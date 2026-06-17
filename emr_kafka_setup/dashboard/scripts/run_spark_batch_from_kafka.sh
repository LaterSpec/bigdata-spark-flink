#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
HostName=""
UserName="hadoop"
BatchId=""
TargetCount=""
SparkBatchSize="${SPARK_BATCH_SIZE:-1000}"
Bucket="s3://figuretibucket"

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
    --batch-id) BatchId="${2:-}"; shift 2 ;;
    --target-count) TargetCount="${2:-}"; shift 2 ;;
    --spark-batch-size) SparkBatchSize="${2:-1000}"; shift 2 ;;
    --bucket) Bucket="${2:-s3://figuretibucket}"; shift 2 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$HostName" ]]; then HostName="$(read_env_value EMR_PRIMARY || true)"; fi
if [[ -z "$TargetCount" ]]; then echo "Falta --target-count" >&2; exit 2; fi
if [[ -z "$BatchId" ]]; then BatchId="$(printf 'batch_%07d' "$TargetCount")"; fi
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$HostName" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"error":"host o PEM no configurado"}'
  exit 0
fi

chmod 600 "$PemPath" 2>/dev/null || true

ssh -i "$PemPath" -o StrictHostKeyChecking=no "${UserName}@${HostName}" \
  "BATCH_ID='$BatchId' TARGET_COUNT='$TargetCount' SPARK_BATCH_SIZE='$SparkBatchSize' BUCKET='$Bucket' bash -s" <<'REMOTE'
set -euo pipefail

PROJECT_HOME=/home/hadoop/bigdata-kafka
KAFKA_HOME=/home/hadoop/kafka
LOG_DIR="$PROJECT_HOME/logs"
STATUS_PATH="$LOG_DIR/spark_batch_status.json"
BATCH_DIR="$LOG_DIR/spark_batches"
LOCK_DIR="$BATCH_DIR/${BATCH_ID}.lock"
RUN_SCRIPT="$BATCH_DIR/run_${BATCH_ID}.sh"

mkdir -p "$LOG_DIR" "$BATCH_DIR"

if [[ -d "$LOCK_DIR" ]]; then
  echo "{\"ok\":true,\"message\":\"$BATCH_ID ya esta en ejecucion\"}"
  exit 0
fi

mkdir "$LOCK_DIR"

cat > "$RUN_SCRIPT" <<'JOB'
#!/usr/bin/env bash
set -euo pipefail

PROJECT_HOME=/home/hadoop/bigdata-kafka
KAFKA_HOME=/home/hadoop/kafka
LOG_DIR="$PROJECT_HOME/logs"
STATUS_PATH="$LOG_DIR/spark_batch_status.json"
BATCH_ID="${BATCH_ID}"
TARGET_COUNT="${TARGET_COUNT}"
SPARK_BATCH_SIZE="${SPARK_BATCH_SIZE}"
BUCKET="${BUCKET}"
LOCK_DIR="$LOG_DIR/spark_batches/${BATCH_ID}.lock"

BOOTSTRAP_INTERNAL="$(hostname -f):9092"
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
  local status="$1"
  local job="$2"
  local rows="$3"
  local message="$4"
  python3 - "$STATUS_PATH" "$BATCH_ID" "$TARGET_COUNT" "$SPARK_BATCH_SIZE" "$status" "$job" "$rows" "$message" "$RAW_OUTPUT" "$RULES_OUTPUT" "$ML_OUTPUT" "$HYBRID_OUTPUT" "$HYBRID_AGG" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
batch_id, target, size, status, job, rows, message = sys.argv[2:9]
outputs = {
    "raw_parquet": sys.argv[9],
    "rules": sys.argv[10],
    "ml": sys.argv[11],
    "hybrid": sys.argv[12],
    "aggregates": sys.argv[13],
}
try:
    data = json.loads(path.read_text()) if path.exists() else {}
except Exception:
    data = {}
data.setdefault("batches", {})
batch = data["batches"].setdefault(batch_id, {
    "batch_id": batch_id,
    "target_count": int(target),
    "spark_batch_size": int(size),
    "jobs": {},
    "outputs": outputs,
})
batch["status"] = status
batch["current_job"] = job
batch["updated_at"] = datetime.now(timezone.utc).isoformat()
batch["message"] = message
batch["outputs"] = outputs
if rows:
    try:
        batch["rows"] = int(rows)
    except Exception:
        pass
if job:
    batch["jobs"][job] = {"status": status, "rows": int(rows or 0), "message": message, "updated_at": batch["updated_at"]}
data["latest_batch_id"] = batch_id
data["ok"] = True
path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
PY
}

extract_value() {
  local key="$1"
  local file="$2"
  awk -F= -v key="$key" '$1 == key {print $2}' "$file" | tail -1 | tr -d '\r'
}

finish() {
  local code="$?"
  if [[ "$code" -ne 0 ]]; then
    update_status "failed" "${CURRENT_JOB:-unknown}" "${CURRENT_ROWS:-0}" "Spark batch fallo con codigo $code"
  fi
  rm -rf "$LOCK_DIR"
  exit "$code"
}
trap finish EXIT

update_status "running" "queued" "0" "Spark batch iniciado"

CURRENT_JOB="job1_raw_from_kafka"
LOG1="$LOG_DIR/spark_${BATCH_ID}_job1.log"
update_status "running" "$CURRENT_JOB" "0" "Leyendo Kafka raw_youtube_chat hacia parquet"
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  "$PROJECT_HOME/spark/jobs/spark_read_kafka_raw_youtube.py" \
  --bootstrap-server "$BOOTSTRAP_INTERNAL" \
  --topic raw_youtube_chat \
  --starting-offsets earliest \
  --ending-offsets latest \
  --output-path "$RAW_OUTPUT" \
  --report-path "$RAW_REPORT" \
  --coalesce 1 > "$LOG1" 2>&1
CURRENT_ROWS="$(extract_value TOTAL_RECORDS "$LOG1" || echo 0)"
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "Kafka raw persistido en parquet"

CURRENT_JOB="job2_rules"
LOG2="$LOG_DIR/spark_${BATCH_ID}_job2.log"
update_status "running" "$CURRENT_JOB" "$CURRENT_ROWS" "Aplicando reglas locales"
spark-submit "$PROJECT_HOME/spark/jobs/spark_rules_from_kafka_parquet.py" \
  --input "$RAW_OUTPUT" \
  --output "$RULES_OUTPUT" \
  --report-output "$RULES_REPORT" \
  --coalesce 1 > "$LOG2" 2>&1
CURRENT_ROWS="$(extract_value OUTPUT_ROWS "$LOG2" || echo "$CURRENT_ROWS")"
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "Reglas locales completadas"

CURRENT_JOB="job4_offendes"
LOG4="$LOG_DIR/spark_${BATCH_ID}_job4.log"
update_status "running" "$CURRENT_JOB" "$CURRENT_ROWS" "Aplicando OffendES Spark ML"
spark-submit "$PROJECT_HOME/spark/jobs/spark_apply_offendes_from_kafka_parquet.py" \
  --input "$RAW_OUTPUT" \
  --binary-model "$BUCKET/output/batch/models/offendes_binary_sparkml/" \
  --multiclass-model "$BUCKET/output/batch/models/offendes_multiclass_sparkml/" \
  --output "$ML_OUTPUT" \
  --report-output "$ML_REPORT" \
  --coalesce 1 > "$LOG4" 2>&1
CURRENT_ROWS="$(extract_value OUTPUT_ROWS "$LOG4" || echo "$CURRENT_ROWS")"
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "OffendES completado"

CURRENT_JOB="job5_hybrid"
LOG5="$LOG_DIR/spark_${BATCH_ID}_job5.log"
update_status "running" "$CURRENT_JOB" "$CURRENT_ROWS" "Generando scoring hibrido y agregados"
spark-submit "$PROJECT_HOME/spark/jobs/spark_hybrid_scoring_from_kafka.py" \
  --rules-input "$RULES_OUTPUT" \
  --ml-input "$ML_OUTPUT" \
  --output "$HYBRID_OUTPUT" \
  --aggregates-output "$HYBRID_AGG" \
  --report-output "$HYBRID_REPORT" \
  --coalesce 1 > "$LOG5" 2>&1
CURRENT_ROWS="$(extract_value OUTPUT_ROWS "$LOG5" || echo "$CURRENT_ROWS")"
update_status "done" "$CURRENT_JOB" "$CURRENT_ROWS" "Spark batch completado"
update_status "done" "complete" "$CURRENT_ROWS" "Batch $BATCH_ID completado"
JOB

chmod +x "$RUN_SCRIPT"
nohup env BATCH_ID="$BATCH_ID" TARGET_COUNT="$TARGET_COUNT" SPARK_BATCH_SIZE="$SPARK_BATCH_SIZE" BUCKET="$BUCKET" "$RUN_SCRIPT" \
  > "$LOG_DIR/spark_${BATCH_ID}_driver.log" 2>&1 &

echo "{\"ok\":true,\"batch_id\":\"$BATCH_ID\",\"target_count\":$TARGET_COUNT,\"message\":\"Spark batch encolado\"}"
REMOTE
