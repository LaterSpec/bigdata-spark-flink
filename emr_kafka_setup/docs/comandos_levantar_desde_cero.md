# Comandos operativos en EMR

La orquestación se ejecuta desde la máquina local porque necesita acceder al primary y saltar hacia los core nodes Kafka.

```bash
cd emr_kafka_setup/dashboard
./scripts/bootstrap_emr_streaming.sh
```

Prueba de dos batches:

```bash
./scripts/bootstrap_emr_streaming.sh --limit 2000
```

Validación:

```bash
./scripts/pipeline_health_from_aws.sh
./scripts/spark_status_from_aws.sh
./scripts/live_delta_from_aws.sh --max-raw 20 --max-flink 20 --max-alerts 10
```

Parada:

```bash
./scripts/stop_emr_streaming.sh
```

La parada cancela YARN y los procesos Flink/Spark de todos los `EMR_WORKERS`, detiene producer, monitor y los tres brokers de `EMR_PRIMARY`, y limpia snapshots de salud y estados locales de batches. No elimina S3 ni termina las instancias.

Variables requeridas:

```dotenv
EMR_PRIMARY=<endpoint del cluster Kafka>
EMR_WORKERS=<endpoint compute 1>,<endpoint compute 2>
DATA_SIZE=30000
SPARK_BATCH_SIZE=1000
```

Consulta el runbook completo en `docs/comandos_levantar_desde_cero.md` de la raíz.
