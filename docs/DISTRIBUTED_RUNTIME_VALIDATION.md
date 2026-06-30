# Validación de la arquitectura distribuida

Fecha: 2026-06-29  
Entorno: AWS EMR / S3  
Muestra: 2,000 eventos  
Batch size: 1,000

> Evidencia histórica de la validación con dos aplicaciones coexistentes. La política operativa vigente usa `SPARK_MAX_CONCURRENCY=1`, por lo que los rangos se ejecutan secuencialmente salvo que el operador aumente el límite de forma explícita.

## Resultado

La arquitectura distribuida fue desplegada y validada de extremo a extremo.

Flujo validado: `S3 Raw → producer en EMR_PRIMARY → Kafka KRaft → Flink y Spark en EMR_WORKERS`. Flink devolvió resultados a Kafka y Spark escribió rangos separados en S3 Curated.

| Componente | Resultado |
|---|---|
| Kafka KRaft | 3 voters, leader activo |
| Replicación | Factor 3, minimum ISR 2 |
| Particiones under-replicated | 0 |
| Particiones offline | 0 |
| `raw_youtube_chat` | 2,000 eventos |
| `nlp_stream_results` | 4,021 eventos |
| `alerts_polarization` | 10 eventos |
| Lag total Flink | 0 |
| Nodos YARN compute | 2 |
| Batches Spark completos | 2 |
| Batches Spark fallidos | 0 |
| Salud global | `healthy` |

## Concurrencia Spark

Los batches se iniciaron con aproximadamente tres segundos de diferencia:

| Batch | Rango | Filas | Resultado |
|---|---:|---:|---|
| `batch_0001000` | 1–1000 | 1,000 | `done` |
| `batch_0002000` | 1001–2000 | 1,000 | `done` |

Durante esta prueba ambos pipelines coexistieron en estado `running` para validar que los rangos eran disjuntos. Después de la prueba se fijó concurrencia segura `1`; actualmente el dashboard espera que el batch anterior termine correctamente.

## Validación de no solapamiento

Se ejecutó `validate_disjoint_batches.py` sobre los parquet Kafka→Spark:

```text
BATCH_A_COUNT=1000
BATCH_A_RANGE=1-1000
BATCH_B_COUNT=1000
BATCH_B_RANGE=1001-2000
EVENT_ID_OVERLAP=0
DISJOINT_VALID=true
```

## Outputs

Cada batch generó rutas independientes:

```text
s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat/<batch_id>/
s3://figuretibucket/output/batch/from_kafka/<batch_id>/job2_rules/
s3://figuretibucket/output/batch/from_kafka/<batch_id>/job4_ml_inference/
s3://figuretibucket/output/batch/from_kafka/<batch_id>/job5_hybrid/
s3://figuretibucket/output/batch/from_kafka/<batch_id>/job5_hybrid_aggregates/
```

Los dos prefijos `job5_hybrid` contienen parquet y marcador `_SUCCESS`.

## Observabilidad

`GET /api/pipeline/health` reportó:

- Quorum Kafka `3/3`.
- ISR completo.
- Sesión streaming finalizada.
- Cinco grupos Flink con lag cero.
- Monitor Kafka activo.
- Un clúster compute accesible con dos nodos YARN.
- Dos batches Spark terminados y ninguno fallido.

Esta validación usa una muestra controlada. `.env` conserva `DATA_SIZE=30000` y `SPARK_BATCH_SIZE=1000`; no se lanzaron automáticamente los 30 batches de la configuración completa.
