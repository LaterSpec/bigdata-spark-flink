# AWS Setup - Kafka, Flink y Spark en EMR

## Arquitectura

- `EMR_PRIMARY`: tres instancias dedicadas al quorum Kafka KRaft.
- `EMR_WORKERS`: uno o más clústeres EMR para Flink y Spark.
- S3: Raw durable, modelos Spark ML y resultados Curated.

El flujo operativo siempre entra primero a `EMR_PRIMARY`: `S3 Raw → producer → Kafka`. Desde `raw_youtube_chat`, Flink y Spark consumen en paralelo en `EMR_WORKERS`; Flink devuelve resultados a Kafka y Spark escribe S3 Curated.
- Dashboard local: orquestación, streaming visual y salud.
- Máquina local con Node.js 18+, Git Bash/SSH y PowerShell 7 en Windows.

Consulta `architecture.md` para los diagramas y decisiones completas.

## Recursos requeridos

### EMR_PRIMARY

- Tres instancias `RUNNING`: primary y dos core.
- Java 8 o superior.
- IAM del primary con permiso `elasticmapreduce:ListInstances`.
- Acceso saliente para descargar Kafka 3.6.2.
- Comunicación privada entre nodos por `9092`, `9093` y SSH.

### EMR_WORKERS

- EMR con Hadoop, Spark 3.4.1 y Flink 1.17.1.
- Al menos un primary accesible por SSH.
- Nodos YARN `RUNNING`.
- Acceso privado a los brokers Kafka de `EMR_PRIMARY`.
- Acceso de lectura/escritura al bucket S3.

## S3

Estructura utilizada:

```text
s3://figuretibucket/
├── data/raw/youtube/youtube_lake.csv
└── output/
    ├── batch/models/
    │   ├── offendes_binary_sparkml/
    │   └── offendes_multiclass_sparkml/
    ├── kafka_to_spark/raw_youtube_chat/<batch_id>/
    └── batch/from_kafka/<batch_id>/
```

## Configuración local

```dotenv
EMR_PRIMARY=ec2-primary.compute-1.amazonaws.com
EMR_WORKERS=ec2-compute-1.compute-1.amazonaws.com,ec2-compute-2.compute-1.amazonaws.com
DATA_SIZE=30000
SPARK_BATCH_SIZE=1000
SPARK_MAX_CONCURRENCY=1
```

La llave `final.pem` y `.env` están ignorados por Git.

## Security groups

- SSH `22`: solo desde la máquina autorizada hacia los endpoints públicos.
- Kafka `9092`: entre security groups Kafka y compute.
- KRaft controller `9093`: solo entre nodos de `EMR_PRIMARY`.
- YARN y servicios internos: mantener reglas privadas propias de EMR.
- No exponer `9092` o `9093` a `0.0.0.0/0`.

## IAM

El perfil de `EMR_PRIMARY` necesita consultar su inventario EMR. Los perfiles compute necesitan leer Raw/modelos y escribir los prefijos de output. No se almacenan claves AWS en scripts.

## Despliegue y arranque

```bash
cd emr_kafka_setup/dashboard
./scripts/bootstrap_emr_streaming.sh
```

El script descubre nodos, configura brokers, despliega código desde el repositorio local, compila Flink e inicia producer y monitor.

La vía recomendada para operación normal es levantar `start_dashboard.ps1` o `start_dashboard.sh`, abrir `http://127.0.0.1:8787` y pulsar **Conectar AWS**. Tras reiniciar una sesión sin querer borrar offsets:

```bash
./scripts/restart_services_after_session.sh
```

## Validación

```bash
./scripts/pipeline_health_from_aws.sh
./scripts/aws_status_from_aws.sh --data-size 30000 --spark-batch-size 1000
./scripts/spark_status_from_aws.sh
```

Los criterios de aceptación son quorum `3/3`, ISR completo, cinco jobs Flink y todos los workers con YARN activo.

## Costos y apagado

Detener procesos no termina instancias:

```bash
./scripts/stop_emr_streaming.sh
```

La parada abarca ambos tipos de clúster: cancela YARN/Flink/Spark en compute, detiene producer/monitor/Kafka en primary y limpia el estado operativo de salud y batches.

Cuando finalicen las pruebas, termina manualmente ambos tipos de clúster EMR para detener cargos. S3 conserva los artefactos.
