# Levantar la plataforma desde cero

Este es el runbook canónico para la arquitectura distribuida vigente. Los comandos antiguos que ejecutaban Kafka, Flink y Spark en el mismo primary ya no aplican.

## 1. Prerrequisitos

- Un clúster `EMR_PRIMARY` con tres instancias `RUNNING`.
- Uno o más clústeres EMR de cómputo con Spark 3.4.1 y Flink 1.17.1.
- Los clústeres dentro de la misma VPC.
- Acceso SSH al primary de cada clúster mediante `final.pem`.
- S3 Raw y modelos OffendES disponibles.
- Node.js, Python 3, SSH y SCP en la máquina local.

```dotenv
EMR_PRIMARY=ec2-primary.compute-1.amazonaws.com
EMR_WORKERS=ec2-compute-1.compute-1.amazonaws.com,ec2-compute-2.compute-1.amazonaws.com
DATA_SIZE=30000
SPARK_BATCH_SIZE=1000
SPARK_MAX_CONCURRENCY=1
```

## 2. Verificación previa

```bash
ssh -i final.pem hadoop@$EMR_PRIMARY "hostname -f"
ssh -i final.pem hadoop@${EMR_WORKERS%%,*} "spark-submit --version && flink --version"
aws s3 ls s3://figuretibucket/data/raw/youtube/youtube_lake.csv
aws s3 ls s3://figuretibucket/output/batch/models/offendes_binary_sparkml/
aws s3 ls s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/
```

## 3. Arranque completo

Desde `emr_kafka_setup/dashboard`:

```bash
./scripts/bootstrap_emr_streaming.sh
```

El script:

1. Descubre las tres instancias de `EMR_PRIMARY`.
2. Accede a los core nodes mediante salto SSH por el primary.
3. Instala Kafka 3.6.2 si falta.
4. Reutiliza KRaft y sus offsets cuando existen; genera un quorum nuevo solo en el primer arranque.
5. Crea topics con tres particiones y réplica tres.
6. Despliega Spark y Flink en `EMR_WORKERS`.
7. Inicia cinco jobs Flink en el primer compute.
8. Inicia el monitor Kafka en el primary.
9. Publica `DATA_SIZE` eventos desde S3.

El arranque estándar conserva topics y reanuda el producer desde la siguiente fila pendiente. Para borrar Kafka explícitamente:

```bash
./scripts/bootstrap_emr_streaming.sh --reset-topics
```

## 4. Prueba controlada de dos batches

```bash
./scripts/bootstrap_emr_streaming.sh --limit 2000 --producer-delay-ms 10
```

Con `SPARK_BATCH_SIZE=1000`, el dashboard lanzará:

- `batch_0001000`: filas `1–1000`.
- `batch_0002000`: filas `1001–2000`.

Con `SPARK_MAX_CONCURRENCY=1` los pipelines se ejecutan en orden. Configura `2` solo si YARN dispone de memoria suficiente para dos aplicaciones completas.

## 5. Validar Kafka

En `EMR_PRIMARY`:

```bash
/home/hadoop/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status

/home/hadoop/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe

python3 /home/hadoop/bigdata-kafka/scripts/monitor_kafka_flow.py \
  --once --bootstrap-server localhost:9092
```

La salida debe indicar tres voters, réplica tres, cero particiones offline y cero particiones under-replicated.

## 6. Validar Flink y Spark

En el primer compute:

```bash
pgrep -af FlinkKafkaStreamingJobs
yarn node -list -all
yarn application -list -appStates ALL
find /home/hadoop/bigdata-kafka/logs/spark_batches -name "*.status.json" -maxdepth 1 -print
```

Validar S3:

```bash
aws s3 ls s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat/batch_0001000/ --recursive
aws s3 ls s3://figuretibucket/output/batch/from_kafka/batch_0001000/ --recursive
```

## 7. Dashboard

```bash
cd emr_kafka_setup/dashboard
./start_dashboard.sh
```

Abrir `http://127.0.0.1:8787`. El panel inferior consulta:

```text
GET /api/pipeline/health
```

## 8. Detener servicios

```bash
./scripts/stop_emr_streaming.sh
```

El comando aplica a ambos lados: cancela aplicaciones YARN y procesos Flink/Spark en todos los `EMR_WORKERS`; después detiene producer, monitor y los tres brokers de `EMR_PRIMARY`. También elimina snapshots de salud y estados locales de batches. No termina las instancias EMR ni elimina datos S3.

En el dashboard, **Detener plataforma** ejecuta este mismo stop. **Conectar AWS** solo vuelve a habilitarse si la detención completa fue confirmada; si falla algún clúster permanece bloqueado para evitar sesiones superpuestas.

## 9. Diagnóstico rápido

```bash
./scripts/aws_status_from_aws.sh --data-size 30000 --spark-batch-size 1000
./scripts/spark_status_from_aws.sh
./scripts/pipeline_health_from_aws.sh
./scripts/live_delta_from_aws.sh --max-raw 20 --max-flink 20 --max-alerts 10
```

## 10. Reinicio de sesión

Si solo se cerró la terminal o se reinició la máquina local, inicia nuevamente el plano de control:

```powershell
cd emr_kafka_setup\dashboard
.\start_dashboard.ps1
```

Si los procesos remotos también se perdieron pero Kafka conserva almacenamiento y offsets:

```powershell
.\scripts\restart_services_after_session.ps1
```

En Git Bash:

```bash
./scripts/restart_services_after_session.sh
```

Estos scripts invocan el bootstrap con `--no-reset-topics`. Para una sesión totalmente limpia usa primero **Detener plataforma** y luego **Conectar AWS**.
