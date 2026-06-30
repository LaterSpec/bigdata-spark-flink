# Recuperación rápida desde S3

El flujo reconstruido es `S3 Raw → producer en EMR_PRIMARY → Kafka → Flink y Spark en EMR_WORKERS`.

1. Actualiza `EMR_PRIMARY` y `EMR_WORKERS` en `.env`.
2. Verifica el CSV Raw y los dos modelos OffendES en S3.
3. Ejecuta:

```bash
cd emr_kafka_setup/dashboard
./scripts/bootstrap_emr_streaming.sh
```

4. Confirma:

```bash
./scripts/pipeline_health_from_aws.sh
./scripts/spark_status_from_aws.sh
```

El bootstrap reconstruye el quorum KRaft de tres brokers, topics, monitor y runtime de cómputo. La reinicialización elimina offsets Kafka, no datos S3.

Consulta `docs/revivir_emr_desde_s3.md` en la raíz para el procedimiento detallado.
