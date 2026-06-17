#!/usr/bin/env bash
set -euo pipefail
BS="ip-172-31-11-3.ec2.internal:9092"
BUCKET="s3://figuretibucket"
LOG="/home/hadoop/bigdata-kafka/logs/spark_7k.log"

echo "SPARK_JOB1_START" | tee -a "$LOG"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py \
  --bootstrap-server "$BS" \
  --topic raw_youtube_chat \
  --starting-offsets earliest \
  --ending-offsets latest \
  --output-path "$BUCKET/output/kafka_to_spark/raw_youtube_chat_test/" \
  --coalesce 1 >> "$LOG" 2>&1 && echo "SPARK_JOB1_DONE" | tee -a "$LOG"

echo "SPARK_JOB2_START" | tee -a "$LOG"
spark-submit \
  /home/hadoop/bigdata-kafka/spark/jobs/spark_rules_from_kafka_parquet.py \
  --input "$BUCKET/output/kafka_to_spark/raw_youtube_chat_test/" \
  --output "$BUCKET/output/batch/from_kafka/job2_rules/run_rules_kafka_test/" \
  --report-output "$BUCKET/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/" \
  --coalesce 1 >> "$LOG" 2>&1 && echo "SPARK_JOB2_DONE" | tee -a "$LOG"

echo "SPARK_JOB4_START" | tee -a "$LOG"
spark-submit \
  /home/hadoop/bigdata-kafka/spark/jobs/spark_apply_offendes_from_kafka_parquet.py \
  --input "$BUCKET/output/kafka_to_spark/raw_youtube_chat_test/" \
  --binary-model "$BUCKET/output/batch/models/offendes_binary_sparkml/" \
  --multiclass-model "$BUCKET/output/batch/models/offendes_multiclass_sparkml/" \
  --output "$BUCKET/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/" \
  --report-output "$BUCKET/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/" \
  --coalesce 1 >> "$LOG" 2>&1 && echo "SPARK_JOB4_DONE" | tee -a "$LOG"

echo "SPARK_JOB5_START" | tee -a "$LOG"
spark-submit \
  /home/hadoop/bigdata-kafka/spark/jobs/spark_hybrid_scoring_from_kafka.py \
  --rules-input "$BUCKET/output/batch/from_kafka/job2_rules/run_rules_kafka_test/" \
  --ml-input "$BUCKET/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/" \
  --output "$BUCKET/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/" \
  --aggregates-output "$BUCKET/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/" \
  --report-output "$BUCKET/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/" \
  --coalesce 1 >> "$LOG" 2>&1 && echo "SPARK_JOB5_DONE" | tee -a "$LOG"

echo "ALL_SPARK_DONE" | tee -a "$LOG"
