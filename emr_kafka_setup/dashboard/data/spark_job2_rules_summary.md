# Job 2 - Reglas Locales desde Kafka Parquet

- Fecha: 2026-06-17 05:17:51 UTC
- Input: `s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/`
- Output: `s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/`
- Input rows: `7105`
- Output rows: `7105`

| flag | count | percentage |
|---|---:|---:|
| has_terruqueo | 65 | 0.9148% |
| has_fraude | 50 | 0.7037% |
| has_electoral_institution | 297 | 4.1802% |
| has_political_mention | 900 | 12.6671% |
| has_polarization_signal | 66 | 0.9289% |
| has_discriminatory_language | 11 | 0.1548% |
| has_ethnic_racial_slur | 10 | 0.1407% |
| has_homophobic_slur | 1 | 0.0141% |
| has_general_insult | 57 | 0.8023% |
| is_spam_noise | 130 | 1.8297% |
