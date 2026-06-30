# Spark Kafka consumer report - Proyecto Big Data

> Documento histórico de la primera validación Kafka→Spark. La ejecución vigente usa rangos `batch_XXXXXXX`, validación exacta de filas e idempotencia, descritos en `spark_batch_from_kafka_full_report.md`.

> **Evidencia histórica con runtime actualizado:** se conservan los valores de la prueba de 105 registros. Actualmente Spark se ejecuta en `EMR_WORKERS`, consume el Kafka distribuido de `EMR_PRIMARY` y limita cada batch mediante `--min-row-number` y `--max-row-number`.

## Resumen

- Fecha de validacion: 2026-06-17 01:18:25 UTC
- Nodo de la prueba histórica: ip-172-31-14-56.ec2.internal
- Spark: 3.4.1-amzn-2
- Scala: 2.12.15
- Kafka de la prueba histórica: 3.6.2 single-node
- Conector usado: org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1
- Topic leido: raw_youtube_chat
- Bootstrap usado por Spark distribuido: ip-172-31-14-56.ec2.internal:9092
- Output Parquet: s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/
- Total validado: 105 registros

## Objetivo validado

Esta prueba validó la rama Spark. La arquitectura completa vigente es:

S3 -> Producer en EMR_PRIMARY -> Kafka KRaft -> Flink y Spark en EMR_WORKERS -> Kafka Results / S3 Curated

Spark ya puede consumir mensajes JSON desde Kafka y persistirlos como dataset Parquet en S3. Este job queda como base para conectar reglas peruanas, modelo OffendES Spark ML, scoring hibrido y agregados historicos.

## Estado Kafka y topics

```text
__consumer_offsets
alerts_polarization
nlp_batch_results
nlp_stream_results
raw_youtube_chat
```

## Offsets de raw_youtube_chat

```text
raw_youtube_chat:0:34
raw_youtube_chat:1:31
raw_youtube_chat:2:40
```

Interpretacion:

- Particion 0: offsets 0 a 33, 34 registros.
- Particion 1: offsets 0 a 30, 31 registros.
- Particion 2: offsets 0 a 39, 40 registros.
- Total aproximado: 105 mensajes.

## Script implementado

Ruta local EMR:

```text
/home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py
```

Ruta S3:

```text
s3://figuretibucket/codes/kafka/spark_read_kafka_raw_youtube.py
```

## Comando ejecutado correctamente

Nota: para Spark distribuido en YARN se uso el bootstrap interno del master. `localhost:9092` funciona para procesos locales en el master, pero no para ejecutores que corren en workers.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py \
  --bootstrap-server ip-172-31-14-56.ec2.internal:9092 \
  --topic raw_youtube_chat \
  --starting-offsets earliest \
  --ending-offsets latest \
  --output-path s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/
```

## Resultado del job

```text
TOTAL_RECORDS=105
OUTPUT_PATH=s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/
COLUMNS=event_id,ingestion_ts,source_s3_path,row_number,source_file,video_id,timestamp_text,timestamp_usec,video_offset_msec,author,author_channel_id,message_raw,message_clean,kafka_topic,kafka_partition,kafka_offset,kafka_timestamp,kafka_key,json_value,raw
OFFSET_SUMMARY partition=0 min=0 max=33 records=34
OFFSET_SUMMARY partition=1 min=0 max=30 records=31
OFFSET_SUMMARY partition=2 min=0 max=39 records=40
```

## Output S3 generado

```text
2026-06-17 01:16:39          0 output/kafka_to_spark/raw_youtube_chat_test/_SUCCESS
2026-06-17 01:16:38      68782 output/kafka_to_spark/raw_youtube_chat_test/part-00000-b5be0881-e3a6-4d65-b9cf-015901bb0bfe-c000.snappy.parquet
```

## Validacion Parquet con Spark

Comando ejecutado:

```bash
spark-submit /home/hadoop/bigdata-kafka/spark/jobs/validate_kafka_to_spark_output.py \
  --input-path s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ \
  --sample-size 5
```

Extracto de validacion:

```text
VALIDATION_COUNT=105
VALIDATION_SCHEMA_BEGIN
root
 |-- event_id: string (nullable = true)
 |-- ingestion_ts: string (nullable = true)
 |-- source_s3_path: string (nullable = true)
 |-- row_number: string (nullable = true)
 |-- source_file: string (nullable = true)
 |-- video_id: string (nullable = true)
 |-- timestamp_text: string (nullable = true)
 |-- timestamp_usec: string (nullable = true)
 |-- video_offset_msec: string (nullable = true)
 |-- author: string (nullable = true)
 |-- author_channel_id: string (nullable = true)
 |-- message_raw: string (nullable = true)
 |-- message_clean: string (nullable = true)
 |-- kafka_topic: string (nullable = true)
 |-- kafka_partition: integer (nullable = true)
 |-- kafka_offset: long (nullable = true)
 |-- kafka_timestamp: timestamp (nullable = true)
 |-- kafka_key: string (nullable = true)
 |-- json_value: string (nullable = true)
 |-- raw: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
VALIDATION_SCHEMA_END
VALIDATION_SAMPLE_BEGIN
event_id=e943da2ad514330035b006e9e5f24e42 row_number=1 video_id=N78mgkUSeOc timestamp_text=21:52 author=@erickvaldivieso2030 kafka_partition=0 kafka_offset=0
event_id=f1f52e70b0cfdb14c123af85f2d13a06 row_number=3 video_id=N78mgkUSeOc timestamp_text=25:46 author=@kooji91 kafka_partition=0 kafka_offset=1
event_id=1ce1036f1d679ad822bb4ba5450af56c row_number=4 video_id=N78mgkUSeOc timestamp_text=26:22 author=@wapo8508 kafka_partition=0 kafka_offset=2
event_id=184734e62e853be56c0aaa3a8a8f2821 row_number=5 video_id=N78mgkUSeOc timestamp_text=26:42 author=@RobloxForUGC kafka_partition=0 kafka_offset=3
event_id=0e2dbc0f98fe35b65f29a1a1765bdb09 row_number=6 video_id=N78mgkUSeOc timestamp_text=27:35 author=@RobloxForUGC kafka_partition=0 kafka_offset=4
VALIDATION_SAMPLE_END
```

## Columnas disponibles

- event_id
- ingestion_ts
- source_s3_path
- row_number
- source_file
- video_id
- timestamp_text
- timestamp_usec
- video_offset_msec
- author
- author_channel_id
- message_raw
- message_clean
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_timestamp
- kafka_key
- json_value
- raw

## Limitaciones y decisiones tecnicas

- Durante esta prueba histórica Kafka estaba instalado en un único master. El runtime vigente usa los tres nodos de `EMR_PRIMARY`.
- Spark en YARN debe usar `ip-172-31-14-56.ec2.internal:9092`, porque `localhost:9092` solo apunta al broker cuando el proceso corre en el master.
- Se usaron los 105 mensajes ya existentes en Kafka; no se ejecuto la carga completa de 160,464 comentarios.
- El output de esta prueba se escribe con modo `overwrite` solo sobre el prefijo de prueba `output/kafka_to_spark/raw_youtube_chat_test/`.
- El job conserva `json_value` y `raw` para trazabilidad, ademas de columnas curadas.

## Estado posterior

La conexión con reglas, OffendES, scoring híbrido y Flink Streaming ya está implementada en la arquitectura distribuida vigente.
