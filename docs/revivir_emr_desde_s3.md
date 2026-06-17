# Revivir el proyecto en un nuevo EMR usando S3 como fuente

Este documento explica como reconstruir el entorno cuando el cluster EMR anterior quedo `TERMINATED`.

La idea es no empezar el proyecto desde cero: S3 queda como fuente durable. El nuevo EMR solo debe volver a levantar la capa computacional:

```text
S3 Raw / Codes / Docs / Models / Outputs
        -> nuevo EMR
        -> Kafka self-managed
        -> Producer Python
        -> Spark Batch
        -> Flink Streaming
        -> Dashboard local
```

## 1. Que se conserva aunque EMR muera

El cluster EMR es efimero. Si termina, se pierden los archivos locales del nodo master, procesos Kafka, logs locales y cualquier instalacion manual. Pero S3 conserva:

- Dataset raw: `s3://figuretibucket/data/raw/youtube/youtube_lake.csv`
- Codigos Kafka/Spark/Flink: `s3://figuretibucket/codes/kafka/`
- Documentacion: `s3://figuretibucket/docs/kafka/`
- Outputs Spark desde Kafka:
  - `s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/`
  - `s3://figuretibucket/output/batch/from_kafka/`
- Modelos Spark ML OffendES:
  - `s3://figuretibucket/output/batch/models/offendes_binary_sparkml/`
  - `s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/`

## 2. Que se debe reconstruir en cada EMR nuevo

Cada vez que crees un EMR nuevo debes reconstruir:

- Directorios locales `/home/hadoop/bigdata-kafka/`
- Kafka 3.6.2 en `/home/hadoop/kafka/`
- Configuracion KRaft con el nuevo hostname privado del master
- Topics Kafka
- Producer Python local
- Scripts Spark/Flink locales
- JAR Flink si no se descarga desde S3
- Muestras Kafka necesarias para validar Spark/Flink

## 3. Crear un nuevo cluster EMR

Crear el cluster desde AWS Academy / EMR con estas aplicaciones:

- Hadoop
- Spark
- Flink

Recomendado para demo academica:

- 1 master + 1 o mas core/task nodes si la cuota lo permite.
- Mantener auto-termination en 2 horas si necesitas cuidar cuota.
- Usar el key pair cuyo `.pem` tienes localmente como `final.pem`.
- Verificar que el security group permita SSH desde tu IP.

Cuando el cluster este en `WAITING` o `RUNNING`, copiar estos dos datos:

- Master public DNS, ejemplo: `ec2-xx-xx-xx-xx.compute-1.amazonaws.com`
- Master private DNS, ejemplo: `ip-172-31-xx-xx.ec2.internal`

El public DNS se usa para SSH desde tu maquina. El private DNS se usa para procesos distribuidos dentro de EMR, especialmente Spark en YARN.

## 4. Variables recomendadas

En tu maquina local PowerShell:

```powershell
$MASTER_DNS="ec2-REEMPLAZAR.compute-1.amazonaws.com"
$PEM="C:\Users\itsma\Documents\BigData\Proyectofinal\final.pem"
```

Dentro del master EMR:

```bash
export BUCKET=s3://figuretibucket
export PROJECT_HOME=/home/hadoop/bigdata-kafka
export KAFKA_HOME=/home/hadoop/kafka
export PRIVATE_DNS=$(hostname -f)
export BOOTSTRAP_LOCAL=localhost:9092
export BOOTSTRAP_INTERNAL=${PRIVATE_DNS}:9092
```

## 5. Restaurar codigo y docs desde S3

En el nuevo master:

```bash
mkdir -p /home/hadoop/bigdata-kafka/{config,scripts,producers,spark/jobs,flink/jobs,flink/scripts,docs,logs}

aws s3 sync s3://figuretibucket/docs/kafka/ /home/hadoop/bigdata-kafka/docs/
aws s3 sync s3://figuretibucket/codes/kafka/flink/ /home/hadoop/bigdata-kafka/flink/
aws s3 cp s3://figuretibucket/codes/kafka/spark_read_kafka_raw_youtube.py /home/hadoop/bigdata-kafka/spark/jobs/
aws s3 cp s3://figuretibucket/codes/kafka/spark_rules_from_kafka_parquet.py /home/hadoop/bigdata-kafka/spark/jobs/
aws s3 cp s3://figuretibucket/codes/kafka/spark_apply_offendes_from_kafka_parquet.py /home/hadoop/bigdata-kafka/spark/jobs/
aws s3 cp s3://figuretibucket/codes/kafka/spark_hybrid_scoring_from_kafka.py /home/hadoop/bigdata-kafka/spark/jobs/
```

Si algunos scripts Kafka o producer no estuvieran en S3, copiarlos desde el workspace local con `scp`:

```powershell
scp -i $PEM -r C:\Users\itsma\Documents\BigData\Proyectofinal\emr_kafka_setup\scripts hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
scp -i $PEM -r C:\Users\itsma\Documents\BigData\Proyectofinal\emr_kafka_setup\producers hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
scp -i $PEM -r C:\Users\itsma\Documents\BigData\Proyectofinal\emr_kafka_setup\config hadoop@${MASTER_DNS}:/home/hadoop/bigdata-kafka/
```

## 6. Instalar Kafka otra vez

Kafka vive en el disco local del master, asi que se pierde al terminar EMR. Reinstalar:

```bash
cd /home/hadoop
wget -q https://archive.apache.org/dist/kafka/3.6.2/kafka_2.12-3.6.2.tgz
tar -xzf kafka_2.12-3.6.2.tgz
ln -sfn /home/hadoop/kafka_2.12-3.6.2 /home/hadoop/kafka
```

Verificar Java:

```bash
java -version
```

EMR normalmente trae Java listo. Si Java no aparece, revisar la version de EMR o instalar Corretto segun permisos del lab.

## 7. Configurar Kafka KRaft con el hostname nuevo

Crear `kraft-server.properties` usando el template:

```bash
export PRIVATE_DNS=$(hostname -f)
sed "s/__ADVERTISED_HOST__/${PRIVATE_DNS}/g" \
  /home/hadoop/bigdata-kafka/config/kraft-server.properties.template \
  > /home/hadoop/bigdata-kafka/config/kraft-server.properties
```

La parte importante es:

```text
advertised.listeners=PLAINTEXT://NUEVO_PRIVATE_DNS:9092
```

No reutilizar el private DNS viejo (`ip-172-31-14-56.ec2.internal`) si el cluster nuevo tiene otro nombre.

## 8. Iniciar Kafka y crear topics

```bash
chmod +x /home/hadoop/bigdata-kafka/scripts/*.sh
/home/hadoop/bigdata-kafka/scripts/start_kafka.sh
/home/hadoop/bigdata-kafka/scripts/create_topics.sh
/home/hadoop/bigdata-kafka/scripts/status_kafka.sh
/home/hadoop/bigdata-kafka/scripts/list_topics.sh
```

Topics esperados:

```text
raw_youtube_chat
nlp_stream_results
alerts_polarization
nlp_batch_results
```

## 9. Reinstalar dependencias Python si faltan

```bash
python3 - <<'PY'
import importlib.util
missing=[m for m in ["boto3","kafka"] if importlib.util.find_spec(m) is None]
print("missing=", missing)
PY
```

Si falta `kafka-python`:

```bash
python3 -m pip install --user boto3 kafka-python
```

No guardar claves AWS en archivos. EMR debe leer S3 con su IAM role.

## 10. Poblar Kafka con muestra desde S3

Kafka nuevo empieza vacio. Para validar no se necesita cargar los 160,464 comentarios. Usar 100 o 105:

```bash
python3 /home/hadoop/bigdata-kafka/producers/produce_youtube_chat_from_s3.py \
  --s3-path s3://figuretibucket/data/raw/youtube/youtube_lake.csv \
  --bootstrap-server localhost:9092 \
  --topic raw_youtube_chat \
  --limit 105 \
  --delay-ms 20
```

Validar offsets:

```bash
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic raw_youtube_chat
```

## 11. Re-ejecutar Spark Batch desde Kafka

Para Spark en YARN usar el private DNS nuevo:

```bash
export BOOTSTRAP_INTERNAL=$(hostname -f):9092
```

Job 1 Kafka -> Spark Parquet:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /home/hadoop/bigdata-kafka/spark/jobs/spark_read_kafka_raw_youtube.py \
  --bootstrap-server ${BOOTSTRAP_INTERNAL} \
  --topic raw_youtube_chat \
  --starting-offsets earliest \
  --ending-offsets latest \
  --output-path s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/
```

Jobs 2, 4 y 5:

```bash
spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_rules_from_kafka_parquet.py \
  --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ \
  --output s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ \
  --report-output s3://figuretibucket/output/batch/from_kafka/job2_rules/reports/run_rules_kafka_test/ \
  --coalesce 1

spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_apply_offendes_from_kafka_parquet.py \
  --input s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/ \
  --binary-model s3://figuretibucket/output/batch/models/offendes_binary_sparkml/ \
  --multiclass-model s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/ \
  --output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ \
  --report-output s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/reports/run_sparkml_offendes_kafka_test/ \
  --coalesce 1

spark-submit /home/hadoop/bigdata-kafka/spark/jobs/spark_hybrid_scoring_from_kafka.py \
  --rules-input s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/ \
  --ml-input s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/ \
  --output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/ \
  --aggregates-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/aggregates_by_minute/run_hybrid_kafka_test/ \
  --report-output s3://figuretibucket/output/batch/from_kafka/job5_hybrid/reports/run_hybrid_kafka_test/ \
  --coalesce 1
```

Job 3 OffendES no necesita reentrenarse si los modelos siguen en S3.

## 12. Re-ejecutar Flink Streaming

Si el JAR existe descargado desde S3:

```bash
ls -lh /home/hadoop/bigdata-kafka/flink/jobs/flink-streaming-jobs.jar
```

Si no existe, compilar:

```bash
chmod +x /home/hadoop/bigdata-kafka/flink/scripts/*.sh
/home/hadoop/bigdata-kafka/flink/scripts/build_flink_jobs.sh
```

Ejecutar jobs con muestra acotada:

```bash
MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job1_normalize_stream.sh
MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job2_window_metrics.sh
MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job3_political_signals.sh
MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job4_actor_polarization.sh
MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job5_risk_alerts.sh
```

Validar salidas:

```bash
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic nlp_stream_results
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic alerts_polarization
```

## 13. Dashboard local

El dashboard corre en tu maquina, no en EMR:

```powershell
cd C:\Users\itsma\Documents\BigData\Proyectofinal\emr_kafka_setup\dashboard
powershell -ExecutionPolicy Bypass -File .\start_dashboard.ps1
```

Abrir:

```text
http://localhost:8787
```

Actualizar el host nuevo en el script si cambio el master:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync_from_aws.ps1 -HostName $MASTER_DNS
```

## 14. Checklist final de salud

```bash
/home/hadoop/bigdata-kafka/scripts/status_kafka.sh
/home/hadoop/bigdata-kafka/scripts/list_topics.sh
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic raw_youtube_chat
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic nlp_stream_results
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic alerts_polarization
aws s3 ls s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/
aws s3 ls s3://figuretibucket/output/batch/from_kafka/
```

## 15. Errores comunes

- `Permission denied` por SSH: revisar que el cluster este activo, el DNS sea nuevo, el security group permita tu IP y el `.pem` corresponda al key pair.
- Spark no conecta a Kafka: usar `${PRIVATE_DNS}:9092`, no `localhost:9092`.
- Kafka no inicia: revisar `advertised.listeners` y que `kraft-server.properties` fue regenerado para el nuevo hostname.
- Producer no lee S3: revisar IAM role del EMR y ruta `s3://figuretibucket/data/raw/youtube/youtube_lake.csv`.
- Flink no encuentra clases Kafka: reconstruir JAR con `build_flink_jobs.sh` despues de instalar Kafka.
- Dashboard no sincroniza: EMR puede estar terminado o rechazando SSH; el dashboard mantiene snapshot local hasta que se actualice el host.
