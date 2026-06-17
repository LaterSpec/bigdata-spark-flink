# Job 4 - Inferencia OffendES sobre datos Kafka

- Fecha: 2026-06-17 05:18:53 UTC
- Input: `s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/`
- Output: `s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/`
- Binary model: `s3://figuretibucket/output/batch/models/offendes_binary_sparkml/`
- Multiclass model: `s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/`
- Input rows: `7105`
- Output rows: `7105`

## Distribucion binaria

| label | count | percentage |
|---|---:|---:|
| no_ofensivo | 3522 | 49.5707% |
| ofensivo | 3583 | 50.4293% |

## Distribucion multiclase

| label | count | percentage |
|---|---:|---:|
| neutral_no_ofensivo | 2778 | 39.0992% |
| odio_agresion_grupal | 51 | 0.7178% |
| ofensivo_directo | 3114 | 43.8283% |
| vulgaridad_contextual | 1162 | 16.3547% |

## Buckets confidence_binary_sparkml

| bucket | count |
|---|---:|
| 0.50-0.60 | 2045 |
| 0.60-0.70 | 2590 |
| 0.70-0.80 | 1031 |
| 0.80-0.90 | 729 |
| 0.90-1.00 | 710 |
