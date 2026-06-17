#!/usr/bin/env bash
set -euo pipefail

ScriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DashboardRoot="$(cd "$ScriptDir/.." && pwd)"
ProjectRoot="$(cd "$DashboardRoot/../.." && pwd)"

PemPath="$ProjectRoot/final.pem"
HostName=""
UserName="hadoop"
BatchId=""
Bucket="s3://figuretibucket"
Limit=250

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
    --bucket) Bucket="${2:-s3://figuretibucket}"; shift 2 ;;
    --limit) Limit="${2:-250}"; shift 2 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$HostName" ]]; then HostName="$(read_env_value EMR_PRIMARY || true)"; fi
if [[ -z "$BatchId" ]]; then echo '{"ok":false,"error":"Falta --batch-id"}'; exit 0; fi
if [[ "$PemPath" != /* ]]; then PemPath="$DashboardRoot/$PemPath"; fi
if [[ -z "$HostName" || ! -f "$PemPath" ]]; then
  echo '{"ok":false,"error":"host o PEM no configurado"}'
  exit 0
fi

chmod 600 "$PemPath" 2>/dev/null || true

ssh -i "$PemPath" -o StrictHostKeyChecking=no "${UserName}@${HostName}" \
  "BATCH_ID='$BatchId' BUCKET='$Bucket' LIMIT='$Limit' bash -s" <<'REMOTE'
set -euo pipefail

PROJECT_HOME=/home/hadoop/bigdata-kafka
JOB_DIR="$PROJECT_HOME/logs/spark_comment_extract"
JOB_FILE="$JOB_DIR/extract_${BATCH_ID}.py"
OUTPUT_DIR="$JOB_DIR/${BATCH_ID}_json"
OUTPUT_FILE="$JOB_DIR/${BATCH_ID}.json"
INPUT="$BUCKET/output/batch/from_kafka/${BATCH_ID}/job5_hybrid/"

mkdir -p "$JOB_DIR"

cat > "$JOB_FILE" <<'PY'
import json
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

batch_id = os.environ["BATCH_ID"]
input_path = os.environ["INPUT"]
output_dir = os.environ["OUTPUT_DIR"]
output_file = os.environ["OUTPUT_FILE"]
limit = int(os.environ.get("LIMIT", "250"))

spark = SparkSession.builder.appName(f"spark-batch-comments-{batch_id}").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(input_path)
    offensive_multi = ["ofensivo_directo", "odio_agresion_grupal", "vulgaridad_contextual"]
    label = F.coalesce(F.col("pred_multiclass_sparkml_label").cast("string"), F.lit(""))
    binary = F.coalesce(F.col("pred_binary_sparkml_label").cast("string"), F.lit(""))
    risk = F.coalesce(F.col("hybrid_risk_level").cast("string"), F.lit(""))
    filtered = df.filter(
        (
            label.isin(offensive_multi)
            | ((binary == "ofensivo") & risk.isin(["medio", "alto"]))
        )
        & (label != "neutral_no_ofensivo")
    )

    selected = filtered.select(
        F.coalesce(F.col("event_id").cast("string"), F.lit("")).alias("event_id"),
        F.coalesce(F.col("row_number").cast("string"), F.lit("")).alias("row_number"),
        F.coalesce(F.col("author").cast("string"), F.lit("youtube_user")).alias("author"),
        F.coalesce(F.col("message_clean").cast("string"), F.col("message_raw").cast("string"), F.lit("")).alias("message"),
        F.coalesce(F.col("pred_binary_sparkml_label").cast("string"), F.lit("")).alias("binary_label"),
        F.coalesce(F.col("pred_multiclass_sparkml_label").cast("string"), F.lit("")).alias("multiclass_label"),
        F.coalesce(F.col("hybrid_risk_level").cast("string"), F.lit("")).alias("risk_level"),
        F.coalesce(F.col("hybrid_risk_reason").cast("string"), F.lit("")).alias("risk_reason"),
        F.coalesce(F.col("local_rule_tags").cast("string"), F.lit("")).alias("local_rule_tags"),
        F.coalesce(F.col("confidence_binary_sparkml").cast("double"), F.lit(0.0)).alias("confidence"),
        F.coalesce(F.col("kafka_partition").cast("string"), F.lit("")).alias("kafka_partition"),
        F.coalesce(F.col("kafka_offset").cast("string"), F.lit("")).alias("kafka_offset"),
    ).orderBy(F.desc("confidence"), "row_number").limit(limit)

    rows = [row.asDict(recursive=True) for row in selected.collect()]
    payload = {
        "ok": True,
        "batch_id": batch_id,
        "input": input_path,
        "count": len(rows),
        "comments": rows,
    }
except Exception as exc:
    payload = {"ok": False, "batch_id": batch_id, "input": input_path, "error": str(exc), "comments": []}
finally:
    spark.stop()

Path(output_file).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
print(json.dumps(payload, ensure_ascii=False))
PY

rm -rf "$OUTPUT_DIR" "$OUTPUT_FILE"
INPUT="$INPUT" OUTPUT_DIR="$OUTPUT_DIR" OUTPUT_FILE="$OUTPUT_FILE" \
  spark-submit "$JOB_FILE" 2>"$JOB_DIR/${BATCH_ID}.err" | tail -n 1
REMOTE
