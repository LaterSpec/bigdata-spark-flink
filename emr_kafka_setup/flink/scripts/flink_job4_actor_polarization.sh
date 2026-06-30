#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-$(hostname -f):9092}"
/usr/bin/flink run -Djobmanager.web.upload.dir=/home/hadoop/bigdata-kafka/flink/upload -t local -c FlinkKafkaStreamingJobs /home/hadoop/bigdata-kafka/flink/jobs/flink-streaming-jobs.jar \
  --job job4 --bootstrap-server "$BOOTSTRAP_SERVER" \
  --group-id flink-job4-actor-polarization \
  --input-topic raw_youtube_chat --output-topic nlp_stream_results \
  --max-messages "${MAX_MESSAGES:-105}" --delay-ms "${DELAY_MS:-20}" --idle-ms "${IDLE_MS:-3000}" \
  --window-seconds "${WINDOW_SECONDS:-2}"
