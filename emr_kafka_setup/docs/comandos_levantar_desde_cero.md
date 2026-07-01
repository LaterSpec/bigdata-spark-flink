# Comandos operativos en EMR

## Setupeo

- **`EMR_PRIMARY`**: Kafka KRaft (3 nodos), producer Python y monitor. Kafka **no** se instala en los workers.
- **`EMR_WORKERS`**: Flink (5 jobs) y Spark batch. Se conectan al bootstrap `9092` del primary por red privada.

La orquestación se ejecuta desde la máquina local porque necesita SSH al primary y salto hacia los core nodes Kafka.

Runbook completo: [docs/comandos_levantar_desde_cero.md](../../docs/comandos_levantar_desde_cero.md).

## Arranque

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

