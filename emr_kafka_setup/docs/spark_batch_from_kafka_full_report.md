# Spark Batch desde Kafka - Reporte final

## Arquitectura validada

Flujo implementado:

```text
S3 Raw -> Python Producer -> Kafka -> Spark Batch -> S3 Curated
```

Esta fase adapta el bloque Spark Batch para que ya no dependa solo de leer el CSV historico directamente desde S3. Ahora Spark puede consumir datos que pasaron por Kafka y producir datasets curados en S3 bajo rutas nuevas.

## Entorno

- EMR master: `ip-172-31-14-56.ec2.internal`
- Kafka: `3.6.2` self-managed en master EMR
- Spark: `3.4.1-amzn-2`
- Scala: `2.12.15`
- Bootstrap para Spark/YARN: `ip-172-31-14-56.ec2.internal:9092`
- Topic fuente: `raw_youtube_chat`
- Muestra usada: `105` mensajes ya presentes en Kafka

Nota importante: `localhost:9092` sirve para procesos locales en el master, pero Spark en YARN debe usar `ip-172-31-14-56.ec2.internal:9092`, porque los ejecutores pueden correr en workers.

## Job 1 - Kafka Raw Ingest

- Objetivo: leer mensajes JSON desde Kafka, parsearlos y guardar un Parquet curado en S3.
- Entrada: `raw_youtube_chat`
- Salida: `s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/`
- Script: `/home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py`
- Script S3: `s3://figuretibucket/codes/kafka/spark_read_kafka_raw_youtube.py`
- Count validado: `105`
- Por que Spark Batch: convierte un snapshot de Kafka en dataset historico reproducible para analisis batch.

Comando:

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

Evidencia:

```text
VALIDATION_COUNT=105
_SUCCESS
part-00000-b5be0881-e3a6-4d65-b9cf-015901bb0bfe-c000.snappy.parquet
```

## Job 2 - Reglas Locales desde Kafka Parquet

- Objetivo: aplicar reglas peruanas multilabel sobre el Parquet generado desde Kafka.
- Entrada: `s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/`
- Salida: `s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/`
- Reporte: `s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/summary.md`
- Script: `/home/hadoop/bigdata-kafka/spark/jobs/spark_rules_from_kafka_parquet.py`
- Script S3: `s3://figuretibucket/codes/kafka/spark_rules_from_kafka_parquet.py`
- Input rows: `105`
- Output rows: `105`
- Por que Spark Batch: aplica reglas a un lote historico/snapshot ya persistido desde Kafka.

Comando:

```bash
spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_rules_from_kafka_parquet.py \
  --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ \
  --output s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ \
  --report-output s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/ \
  --coalesce 1
```

Columnas generadas:

```text
has_terruqueo
has_fraude
has_electoral_institution
has_political_mention
has_polarization_signal
has_discriminatory_language
has_ethnic_racial_slur
has_homophobic_slur
has_general_insult
is_spam_noise
local_risk_score
local_rule_tags
```

Evidencia:

```text
VALIDATION_COUNT=105
_SUCCESS
part-00000-f15835f2-2211-47a9-8d91-63466aeaa792-c000.snappy.parquet
```

## Job 3 - Entrenamiento OffendES Spark ML

- Objetivo: disponer de modelos Spark ML para ofensividad y multiclase.
- Estado: reutilizado, ya estaba completado previamente.
- Script historico: `spark_train_offendes.py`
- Modelo binario: `s3://figuretibucket/output/batch/models/offendes_binary_sparkml/`
- Modelo multiclase: `s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/`
- Reporte entrenamiento: `s3://figuretibucket/output/batch/reports/spark_ml_training/summary.md`
- Por que Spark Batch: entrenamiento supervisado offline con dataset etiquetado OffendES.

Metricas documentadas:

```text
Modelo binario: accuracy=0.838968, macro_f1=0.749030
Modelo multiclase: accuracy=0.771939, macro_f1=0.548677
```

Decision:

No se reentreno porque los modelos ya existen en S3 y fueron validados en la fase Spark Batch anterior. Esto evita modificar modelos previos y mantiene reproducibilidad.

## Job 4 - Inferencia OffendES sobre datos Kafka

- Objetivo: aplicar los modelos OffendES Spark ML existentes sobre el dataset generado desde Kafka.
- Entrada: `s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/`
- Salida: `s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/`
- Reporte: `s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/summary.md`
- Script: `/home/hadoop/bigdata-kafka/spark/jobs/spark_apply_offendes_from_kafka_parquet.py`
- Script S3: `s3://figuretibucket/codes/kafka/spark_apply_offendes_from_kafka_parquet.py`
- Input rows: `105`
- Output rows: `105`
- Por que Spark Batch: inferencia ML sobre un lote Kafka persistido para analisis historico.

Comando:

```bash
spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_apply_offendes_from_kafka_parquet.py \
  --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ \
  --binary-model s3://figuretibucket/output/batch/models/offendes_binary_sparkml/ \
  --multiclass-model s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/ \
  --output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ \
  --report-output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/ \
  --coalesce 1
```

Columnas principales:

```text
pred_binary_sparkml
pred_binary_sparkml_label
confidence_binary_sparkml
confidence_binary_bucket
pred_multiclass_sparkml
pred_multiclass_sparkml_label
confidence_multiclass_sparkml
```

Evidencia:

```text
VALIDATION_COUNT=105
_SUCCESS
part-00000-7d5a2cb7-46b7-4076-8e38-a74b640d9d68-c000.snappy.parquet
```

## Job 5 - Scoring Hibrido + Agregados + Reporte

- Objetivo: unir reglas locales y predicciones ML, generar riesgo hibrido, agregados por minuto y reporte interpretativo.
- Rules input: `s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/`
- ML input: `s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/`
- Output: `s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/`
- Aggregates: `s3://figuretibucket/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/`
- Reporte: `s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/summary.md`
- Script: `/home/hadoop/bigdata-kafka/spark/jobs/spark_hybrid_scoring_from_kafka.py`
- Script S3: `s3://figuretibucket/codes/kafka/spark_hybrid_scoring_from_kafka.py`
- Rules rows: `105`
- ML rows: `105`
- Output rows: `105`
- Aggregates rows: `54`
- Por que Spark Batch: combina señales y genera salidas consolidadas historicas desde un snapshot Kafka.

Comando:

```bash
spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_hybrid_scoring_from_kafka.py \
  --rules-input s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ \
  --ml-input s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ \
  --output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/ \
  --aggregates-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/ \
  --report-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/ \
  --coalesce 1
```

Columnas hibridas:

```text
has_any_local_rule
has_strong_local_rule
ml_offensive_high_confidence
hybrid_risk_level
hybrid_risk_reason
confidence_binary_bucket
```

Evidencia:

```text
VALIDATION_COUNT=105
_SUCCESS
part-00000-2540a80b-80af-447a-af56-7676ce7adafd-c000.snappy.parquet
```

Agregados:

```text
VALIDATION_COUNT=54
_SUCCESS
part-00000-a7c5de83-8b5f-4a6d-b366-6b6209fdf83f-c000.snappy.parquet
```

## Rutas S3 generadas

```text
s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/
s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/
s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/summary.md
s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/
s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/summary.md
s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/
s3://figuretibucket/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/
s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/summary.md
```

## Scripts copiados a S3

```text
s3://figuretibucket/codes/kafka/spark_read_kafka_raw_youtube.py
s3://figuretibucket/codes/kafka/spark_rules_from_kafka_parquet.py
s3://figuretibucket/codes/kafka/spark_apply_offendes_from_kafka_parquet.py
s3://figuretibucket/codes/kafka/spark_hybrid_scoring_from_kafka.py
s3://figuretibucket/codes/kafka/validate_parquet_path.py
```

## Interpretacion metodologica

El resultado final defendible es `hybrid_risk_level`, no la prediccion cruda de OffendES. OffendES detecta ofensividad general en espanol, pero no entiende por si solo terruqueo, fraude electoral, ONPE/JNE, polarizacion peruana ni contexto local. Las reglas locales complementan el modelo y explican por que un comentario puede elevarse a riesgo medio o alto.

## Limitaciones

- La prueba usa una muestra pequena de `105` mensajes ya publicados en Kafka.
- No se ejecuto la carga completa de `160,464` comentarios.
- Kafka esta self-managed en el master EMR por restricciones academicas de AWS Academy Learner Lab; no es configuracion productiva.
- Job 3 reutiliza modelos previamente entrenados porque ya existian en S3.
- El dataset peruano de YouTube no tiene labels humanos.
- OffendES es una senal general de ofensividad, no una verdad final politica.
- La union de Job 5 usa `kafka_topic + kafka_partition + kafka_offset` como llave principal para evitar duplicados cuando `event_id` se repite por replay.

## Proximo paso

Implementar Flink Streaming como consumidor de `raw_youtube_chat` para cubrir la capa de baja latencia: limpieza streaming, throughput, keywords politicas, polarizacion y alertas.
