# Recuperar la plataforma desde S3

S3 es la fuente durable. Kafka, sus offsets y los estados locales de EMR pueden reconstruirse sin alterar el CSV original, modelos ni outputs históricos.

La reconstrucción respeta el flujo vigente: S3 alimenta al producer de `EMR_PRIMARY`, Kafka distribuye `raw_youtube_chat` tanto a Flink como a Spark en `EMR_WORKERS`, y cada motor conserva su salida independiente.

## Cuándo usar este procedimiento

- Se reemplazó `EMR_PRIMARY`.
- Cambiaron DNS públicos o privados.
- Se perdió el almacenamiento KRaft.
- Se recrearon los clústeres de cómputo.
- Kafka quedó con un quorum o ISR inconsistente.

## 1. Actualizar endpoints

```dotenv
EMR_PRIMARY=<DNS público del nuevo clúster Kafka>
EMR_WORKERS=<DNS compute 1>,<DNS compute 2>
DATA_SIZE=30000
SPARK_BATCH_SIZE=1000
```

No agregues DNS privados: el bootstrap los descubre mediante EMR.

## 2. Confirmar datos durables

```bash
aws s3 ls s3://figuretibucket/data/raw/youtube/youtube_lake.csv
aws s3 ls s3://figuretibucket/output/batch/models/offendes_binary_sparkml/
aws s3 ls s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/
```

## 3. Reconstruir Kafka y compute

```bash
cd emr_kafka_setup/dashboard
./scripts/bootstrap_emr_streaming.sh
```

Este comando reinicializa los tres nodos Kafka con un mismo `cluster.id`, vuelve a desplegar código, crea topics y arranca monitor, Flink y producer.

## 4. Validar recuperación

```bash
./scripts/pipeline_health_from_aws.sh
./scripts/spark_status_from_aws.sh
```

La salud esperada es:

- Quorum: `3 / 3`.
- Under-replicated partitions: `0`.
- Offline partitions: `0`.
- Flink jobs: `5`.
- Workers: accesibles y con nodos YARN `RUNNING`.

## 5. Reprocesar un rango Spark

Cada batch es idempotente dentro de su propia ruta. Para recalcular las primeras mil filas:

```bash
./scripts/run_spark_batch_from_kafka.sh \
  --batch-id batch_0001000 \
  --target-count 1000 \
  --spark-batch-size 1000 \
  --force
```

El script limpia únicamente:

```text
s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat/batch_0001000/
s3://figuretibucket/output/batch/from_kafka/batch_0001000/
```

`--force` es obligatorio para recalcular un batch con estado persistido. No interrumpe un batch que todavía conserva su lock, no modifica modelos ni toca otros rangos.

## 6. Recuperar observabilidad

El monitor se inicia con el bootstrap. Para regenerar un snapshot inmediato:

```bash
ssh -i final.pem hadoop@$EMR_PRIMARY \
  "python3 /home/hadoop/bigdata-kafka/scripts/monitor_kafka_flow.py --once"
```

Los archivos operativos son:

```text
/home/hadoop/bigdata-kafka/logs/kafka_flow_health.json
/home/hadoop/bigdata-kafka/logs/kafka_flow_history.jsonl
/home/hadoop/bigdata-kafka/logs/kafka_flow_monitor.log
```

## 7. Condiciones que requieren intervención

- Menos de tres instancias `RUNNING` en `EMR_PRIMARY`: no iniciar Kafka.
- Security groups bloqueando `9092` o `9093`: corregir red antes del bootstrap.
- Modelos OffendES ausentes: restaurarlos antes de lanzar Spark.
- Aplicaciones YARN en `ACCEPTED`: agregar capacidad o más endpoints a `EMR_WORKERS`.
