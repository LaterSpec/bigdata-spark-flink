#!/usr/bin/env bash
set -euo pipefail
export BOOTSTRAP_SERVER="ip-172-31-11-3.ec2.internal:9092"
export MAX_MESSAGES=7000
export DELAY_MS=0
export WINDOW_SECONDS=5
export IDLE_MS=5000
LOG="/home/hadoop/bigdata-kafka/logs/flink_7k.log"

echo "JOB1_START" >> "$LOG"
bash /home/hadoop/bigdata-kafka/flink/scripts/flink_job1_normalize_stream.sh >> "$LOG" 2>&1 && echo "JOB1_DONE" >> "$LOG"

echo "JOB2_START" >> "$LOG"
bash /home/hadoop/bigdata-kafka/flink/scripts/flink_job2_window_metrics.sh >> "$LOG" 2>&1 && echo "JOB2_DONE" >> "$LOG"

echo "JOB3_START" >> "$LOG"
bash /home/hadoop/bigdata-kafka/flink/scripts/flink_job3_political_signals.sh >> "$LOG" 2>&1 && echo "JOB3_DONE" >> "$LOG"

echo "JOB4_START" >> "$LOG"
bash /home/hadoop/bigdata-kafka/flink/scripts/flink_job4_actor_polarization.sh >> "$LOG" 2>&1 && echo "JOB4_DONE" >> "$LOG"

echo "JOB5_START" >> "$LOG"
bash /home/hadoop/bigdata-kafka/flink/scripts/flink_job5_risk_alerts.sh >> "$LOG" 2>&1 && echo "JOB5_DONE" >> "$LOG"

echo "ALL_FLINK_DONE" >> "$LOG"
