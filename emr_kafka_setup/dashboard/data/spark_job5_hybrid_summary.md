# Job 5 - Scoring Hibrido desde Kafka

- Fecha: 2026-06-17 05:19:39 UTC
- Rules input: `s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/`
- ML input: `s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/`
- Output: `s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/`
- Aggregates: `s3://figuretibucket/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/`
- Rules rows: `7105`
- ML rows: `7105`
- Output rows: `7105`
- ML offensive high confidence: `683`

## Distribucion hybrid_risk_level

| level | count | percentage |
|---|---:|---:|
| alto | 59 | 0.8304% |
| bajo | 4622 | 65.0528% |
| medio | 2424 | 34.1168% |

## Distribucion ML binaria

| label | count | percentage |
|---|---:|---:|
| no_ofensivo | 3522 | 49.5707% |
| ofensivo | 3583 | 50.4293% |

## Interpretacion

El resultado defendible es `hybrid_risk_level`, no la prediccion ML cruda. OffendES aporta una senal general de ofensividad en espanol, mientras que las reglas locales aportan contexto peruano como terruqueo, fraude, instituciones electorales y polarizacion.
