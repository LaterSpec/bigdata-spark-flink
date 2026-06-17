# Comandos para levantar todo desde cero

Este archivo es un runbook de comandos. Sirve para dos escenarios:

- Escenario A: empezar desde cero, subiendo codigo/datos/docs a S3 y luego creando EMR.
- Escenario B: S3 ya tiene la data necesaria y solo necesitas recrear EMR, Kafka y procesos.

Usar los comandos en orden. Reemplazar los placeholders antes de ejecutar.

## 0. Variables locales en Windows PowerShell

```powershell
$ROOT="C:\Users\itsma\Documents\BigData\Proyectofinal"
$PEM="$ROOT\final.pem"
$BUCKET="s3://figuretibucket"
$MASTER_DNS="ec2-REEMPLAZAR.compute-1.amazonaws.com"
```

## 1. Escenario A - subir todo a S3 desde cero

Ejecutar desde tu maquina local si S3 estuviera vacio o quieres refrescar codigos/docs.

### 1.1 Subir dataset raw

```powershell
aws s3 cp "$ROOT\youtube_lake.csv" "$BUCKET/data/raw/youtube/youtube_lake.csv"
```

### 1.2 Subir scripts Kafka base

```powershell
aws s3 sync "$ROOT\emr_kafka_setup\scripts" "$BUCKET/codes/kafka/scripts/"
aws s3 sync "$ROOT\emr_kafka_setup\config" "$BUCKET/codes/kafka/config/"
aws s3 sync "$ROOT\emr_kafka_setup\producers" "$BUCKET/codes/kafka/producers/"
```

### 1.3 Subir scripts Spark adaptados a Kafka

```powershell
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_read_kafka_raw_youtube.py" "$BUCKET/codes/kafka/spark_read_kafka_raw_youtube.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_rules_from_kafka_parquet.py" "$BUCKET/codes/kafka/spark_rules_from_kafka_parquet.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_apply_offendes_from_kafka_parquet.py" "$BUCKET/codes/kafka/spark_apply_offendes_from_kafka_parquet.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\spark_hybrid_scoring_from_kafka.py" "$BUCKET/codes/kafka/spark_hybrid_scoring_from_kafka.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\validate_parquet_path.py" "$BUCKET/codes/kafka/validate_parquet_path.py"
aws s3 cp "$ROOT\emr_kafka_setup\spark\jobs\validate_kafka_to_spark_output.py" "$BUCKET/codes/kafka/validate_kafka_to_spark_output.py"
```

### 1.4 Subir Flink

```powershell
aws s3 sync "$ROOT\emr_kafka_setup\flink" "$BUCKET/codes/kafka/flink/"
```

### 1.5 Subir docs

```powershell
aws s3 sync "$ROOT\emr_kafka_setup\docs" "$BUCKET/docs/kafka/"
```

### 1.6 Verificar S3

```powershell
aws s3 ls "$BUCKET/data/raw/youtube/"
aws s3 ls "$BUCKET/codes/kafka/"
aws s3 ls "$BUCKET/docs/kafka/"
```

## 2. Crear nuevo EMR

Crear cluster EMR desde consola AWS Academy con:

- Aplicaciones: Hadoop, Spark, Flink
- Key pair: el que corresponde a `final.pem`
- Auto-termination: 2 horas si quieres cuidar cuota
- Security group: permitir SSH desde tu IP

Cuando este `WAITING` o `RUNNING`, actualizar:

```powershell
$MASTER_DNS="ec2-NUEVO.compute-1.amazonaws.com"
```

Probar SSH:

```powershell
ssh -i $PEM -o StrictHostKeyChecking=no hadoop@$MASTER_DNS "hostname; hostname -f; date"
```

## 3. Restaurar estructura local en EMR desde S3

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "mkdir -p /home/hadoop/bigdata-kafka/{config,scripts,producers,spark/jobs,flink/jobs,flink/scripts,docs,logs}"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 sync s3://figuretibucket/docs/kafka/ /home/hadoop/bigdata-kafka/docs/"
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

Si S3 no tuviera esos scripts, subirlos por `scp`:

```powershell
scp -i $PEM -r "$ROOT\emr_kafka_setup\scripts" hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
scp -i $PEM -r "$ROOT\emr_kafka_setup\config" hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
scp -i $PEM -r "$ROOT\emr_kafka_setup\producers" hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
scp -i $PEM -r "$ROOT\emr_kafka_setup\spark" hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
scp -i $PEM -r "$ROOT\emr_kafka_setup\flink" hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
```

## 4. Instalar Kafka 3.6.2 en el master

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "cd /home/hadoop && wget -q https://archive.apache.org/dist/kafka/3.6.2/kafka_2.12-3.6.2.tgz && tar -xzf kafka_2.12-3.6.2.tgz && ln -sfn /home/hadoop/kafka_2.12-3.6.2 /home/hadoop/kafka"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/kafka/bin/kafka-topics.sh --version"
```

## 5. Configurar Kafka para el nuevo private DNS

```powershell
ssh -i $PEM hadoop@$MASTER_DNS 'export PRIVATE_DNS=$(hostname -f); sed "s/__ADVERTISED_HOST__/${PRIVATE_DNS}/g" /home/hadoop/bigdata-kafka/config/kraft-server.properties.template > /home/hadoop/bigdata-kafka/config/kraft-server.properties; grep advertised.listeners /home/hadoop/bigdata-kafka/config/kraft-server.properties'
```

## 6. Iniciar Kafka y topics

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/start_kafka.sh"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/create_topics.sh"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/status_kafka.sh"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/list_topics.sh"
```

## 7. Instalar dependencias Python si faltan

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "python3 -m pip install --user boto3 kafka-python"
```

## 8. Enviar muestra desde S3 a Kafka

No ejecutar full todavia. Usar 105 o maximo 1000 para demo.

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "python3 /home/hadoop/bigdata-kafka/producers/produce_youtube_chat_from_s3.py --s3-path s3://figuretibucket/data/raw/youtube/youtube_lake.csv --bootstrap-server localhost:9092 --topic raw_youtube_chat --limit 105 --delay-ms 20"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic raw_youtube_chat"
```

## 9. Spark Job 1 - Kafka Raw Ingest

```powershell
ssh -i $PEM hadoop@$MASTER_DNS 'export BOOTSTRAP_INTERNAL=$(hostname -f):9092; spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py --bootstrap-server ${BOOTSTRAP_INTERNAL} --topic raw_youtube_chat --starting-offsets earliest --ending-offsets latest --output-path s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/'
```

## 10. Spark Job 2 - Reglas locales

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_rules_from_kafka_parquet.py --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ --output s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ --report-output s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/ --coalesce 1"
```

## 11. Spark Job 3 - Modelos OffendES

No reentrenar si existen:

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 ls s3://figuretibucket/output/batch/models/offendes_binary_sparkml/ && aws s3 ls s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/"
```

Si no existen, usar el entrenamiento historico `spark_train_offendes.py` segun `README_SPARK_BATCH_PLAN.md`.

## 12. Spark Job 4 - Inferencia OffendES

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_apply_offendes_from_kafka_parquet.py --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ --binary-model s3://figuretibucket/output/batch/models/offendes_binary_sparkml/ --multiclass-model s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/ --output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ --report-output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/ --coalesce 1"
```

## 13. Spark Job 5 - Hibrido

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_hybrid_scoring_from_kafka.py --rules-input s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ --ml-input s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ --output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/ --aggregates-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/ --report-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/ --coalesce 1"
```

## 14. Flink - compilar si hace falta

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "ls -lh /home/hadoop/bigdata-kafka/flink/jobs/flink-streaming-jobs.jar || /home/hadoop/bigdata-kafka/flink/scripts/build_flink_jobs.sh"
```

## 15. Flink - ejecutar 5 jobs

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job1_normalize_stream.sh > /home/hadoop/bigdata-kafka/logs/flink_job1_normalize_stream.log 2>&1"
ssh -i $PEM hadoop@$MASTER_DNS "MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job2_window_metrics.sh > /home/hadoop/bigdata-kafka/logs/flink_job2_window_metrics.log 2>&1"
ssh -i $PEM hadoop@$MASTER_DNS "MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job3_political_signals.sh > /home/hadoop/bigdata-kafka/logs/flink_job3_political_signals.log 2>&1"
ssh -i $PEM hadoop@$MASTER_DNS "MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job4_actor_polarization.sh > /home/hadoop/bigdata-kafka/logs/flink_job4_actor_polarization.log 2>&1"
ssh -i $PEM hadoop@$MASTER_DNS "MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job5_risk_alerts.sh > /home/hadoop/bigdata-kafka/logs/flink_job5_risk_alerts.log 2>&1"
```

## 16. Validar Flink outputs

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic nlp_stream_results"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic alerts_polarization"
ssh -i $PEM hadoop@$MASTER_DNS "timeout 20s /home/hadoop/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nlp_stream_results --from-beginning --max-messages 5"
ssh -i $PEM hadoop@$MASTER_DNS "timeout 20s /home/hadoop/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts_polarization --from-beginning --max-messages 3"
```

## 17. Dashboard local

```powershell
cd "$ROOT\emr_kafka_setup\dashboard"
powershell -ExecutionPolicy Bypass -File .\start_dashboard.ps1
```

Abrir:

```text
http://localhost:8787
```

Sincronizar desde el nuevo EMR:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync_from_aws.ps1 -HostName $MASTER_DNS -PemPath "$PEM"
```

## 18. Validacion final completa

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/status_kafka.sh"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/bigdata-kafka/scripts/list_topics.sh"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic raw_youtube_chat"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic nlp_stream_results"
ssh -i $PEM hadoop@$MASTER_DNS "/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic alerts_polarization"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 ls s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/"
ssh -i $PEM hadoop@$MASTER_DNS "aws s3 ls s3://figuretibucket/output/batch/from_kafka/"
```

## 19. Comando full preparado, no ejecutar sin confirmar

Solo cuando quieras cargar los 160,464 comentarios:

```powershell
ssh -i $PEM hadoop@$MASTER_DNS "python3 /home/hadoop/bigdata-kafka/producers/produce_youtube_chat_from_s3.py --s3-path s3://figuretibucket/data/raw/youtube/youtube_lake.csv --bootstrap-server localhost:9092 --topic raw_youtube_chat --delay-ms 1"
```

No correr full si estas cuidando cuota o si solo necesitas demo.
