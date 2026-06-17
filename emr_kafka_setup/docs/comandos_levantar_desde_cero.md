# Comandos para levantar desde cero

Runbook copiables para iniciar desde cero o reconstruir desde S3.

## Variables locales

```powershell
$ROOT="C:\Users\itsma\Documents\BigData\Proyectofinal"
$PEM="$ROOT\final.pem"
$BUCKET="s3://figuretibucket"
$MASTER_DNS="ec2-3-226-239-186.compute-1.amazonaws.com"
Set-Content -Path "$ROOT\.env" -Value "EMR_PRIMARY=$MASTER_DNS"
```

## Si S3 estuviera vacio, subir base

```powershell
aws s3 cp "$ROOT\youtube_lake.csv" "$BUCKET/data/raw/youtube/youtube_lake.csv"
aws s3 sync "$ROOT\emr_kafka_setup\scripts" "$BUCKET/codes/kafka/scripts/"
aws s3 sync "$ROOT\emr_kafka_setup\config" "$BUCKET/codes/kafka/config/"
aws s3 sync "$ROOT\emr_kafka_setup\producers" "$BUCKET/codes/kafka/producers/"
aws s3 sync "$ROOT\emr_kafka_setup\flink" "$BUCKET/codes/kafka/flink/"
aws s3 sync "$ROOT\emr_kafka_setup\docs" "$BUCKET/docs/kafka/"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_read_kafka_raw_youtube.py" "$BUCKET/codes/kafka/spark_read_kafka_raw_youtube.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_rules_from_kafka_parquet.py" "$BUCKET/codes/kafka/spark_rules_from_kafka_parquet.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_apply_offendes_from_kafka_parquet.py" "$BUCKET/codes/kafka/spark_apply_offendes_from_kafka_parquet.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_hybrid_scoring_from_kafka.py" "$BUCKET/codes/kafka/spark_hybrid_scoring_from_kafka.py"
```

## Restaurar EMR desde S3

```powershell
ssh -i $PEM -o StrictHostKeyChecking=no hadoop@$MASTER_DNS "hostname; hostname -f; date"
ssh -i $PEM hadoop@$MASTER_DNS "mkdir -p /home/hadoop/bigdata-kafka/{config,scripts,producers,spark/jobs,flink/jobs,flink/scripts,docs,logs}"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 sync s3://figuretibucket/codes/kafka/scripts/ /home/hadoop/bigdata-kafka/scripts/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 sync s3://figuretibucket/codes/kafka/config/ /home/hadoop/bigdata-kafka/config/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 sync s3://figuretibucket/codes/kafka/producers/ /home/hadoop/bigdata-kafka/producers/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 sync s3://figuretibucket/codes/kafka/flink/ /home/hadoop/bigdata-kafka/flink/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 cp s3://figuretibucket/codes/kafka/spark_read_kafka_raw_youtube.py /home/hadoop/bigdata-kafka/spark/jobs/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 cp s3://figuretibucket/codes/kafka/spark_rules_from_kafka_parquet.py /home/hadoop/bigdata-kafka/spark/jobs/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 cp s3://figuretibucket/codes/kafka/spark_apply_offendes_from_kafka_parquet.py /home/hadoop/bigdata-kafka/spark/jobs/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 cp s3://figuretibucket/codes/kafka/spark_hybrid_scoring_from_kafka.py /home/hadoop/bigdata-kafka/spark/jobs/"
ssh -i $PEM hadoop@$MASTER_DNS "chmod +x /home/hadoop/bigdata-kafka/scripts/*.sh /home/hadoop/bigdata-kafka/flink/scripts/*.sh"
```

## Instalar y configurar Kafka

```powershell
ssh -i $PEM hadoop@$MASTER_DNS 'cd /home/hadoop; wget -q https://archive.apache.org/dist/kafka/3.6.2/kafka_2.12-3.6.2.tgz; tar -xzf kafka_2.12-3.6.2.tgz; ln -sfn /home/hadoop/kafka_2.12-3.6.2 /home/hadoop/kafka'
ssh -i $PEM hadoop@$MASTER_DNS 'PRIVATE_DNS=$(hostname -f); sed "s/__ADVERTISED_HOST__/${PRIVATE_DNS}/g" /home/hadoop/bigdata-kafka/config/kraft-server.properties.template > /home/hadoop/bigdata-kafka/config/kraft-server.properties'
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/start_kafka.sh"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/create_topics.sh"
```

## Producer

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "python3 -m pip install --user boto3 kafka-python"
ssh -i $PEM hadoop@$MASTER_DNS "python3 /home/hadoop/bigdata-kafka/producers/produce_youtube_chat_from_s3.py --s3-path s3://figuretibucket/data/raw/youtube/youtube_lake.csv --bootstrap-server localhost:9092 --topic raw_youtube_chat --limit 105 --delay-ms 20"
```

## Spark Batch

```powershell
ssh -i $PEM hadoop@$MASTER_DNS 'export BOOTSTRAP_INTERNAL=$(hostname -f):9092; spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py --bootstrap-server ${BOOTSTRAP_INTERNAL} --topic raw_youtube_chat --starting-offsets earliest --ending-offsets latest --output-path s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ --coalesce 1'
ssh -i $PEM hadoop@$MASTER_DNS "spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_rules_from_kafka_parquet.py --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ --output s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ --report-output s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/ --coalesce 1"
ssh -i $PEM hadoop@$MASTER_DNS "spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_apply_offendes_from_kafka_parquet.py --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ --binary-model s3://figuretibucket/output/batch/models/offendes_binary_sparkml/ --multiclass-model s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/ --output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ --report-output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/ --coalesce 1"
ssh -i $PEM hadoop@$MASTER_DNS "spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_hybrid_scoring_from_kafka.py --rules-input s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ --ml-input s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ --output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/ --aggregates-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/ --report-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/ --coalesce 1"
```

## Flink Streaming

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "BOOTSTRAP_SERVER=\$(hostname -f):9092 MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job1_normalize_stream.sh"
ssh -i $PEM hadoop@$MASTER_DNS "BOOTSTRAP_SERVER=\$(hostname -f):9092 MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job2_window_metrics.sh"
ssh -i $PEM hadoop@$MASTER_DNS "BOOTSTRAP_SERVER=\$(hostname -f):9092 MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job3_political_signals.sh"
ssh -i $PEM hadoop@$MASTER_DNS "BOOTSTRAP_SERVER=\$(hostname -f):9092 MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job4_actor_polarization.sh"
ssh -i $PEM hadoop@$MASTER_DNS "BOOTSTRAP_SERVER=\$(hostname -f):9092 MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job5_risk_alerts.sh"
```

## Dashboard

```powershell
cd "$ROOT\emr_kafka_setup\dashboard"
powershell -ExecutionPolicy Bypass -File .\scripts\sync_from_aws.ps1 -PemPath "$PEM"
powershell -ExecutionPolicy Bypass -File .\start_dashboard.ps1
```

Abrir `http://localhost:8787`.
