# Revivir EMR desde S3

Guia para reconstruir el proyecto cuando el EMR anterior termino. S3 queda como fuente durable; el EMR nuevo solo reconstruye computo local.

## Fuente durable

- Dataset raw: `s3://figuretibucket/data/raw/youtube/youtube_lake.csv`
- Codigos: `s3://figuretibucket/codes/kafka/`
- Docs: `s3://figuretibucket/docs/kafka/`
- Modelos OffendES: `s3://figuretibucket/output/batch/models/`
- Outputs Spark/Flink historicos: `s3://figuretibucket/output/`

## Variable local del nodo

Guardar el primary actual en `.env`:

```text
EMR_PRIMARY=ec2-REEMPLAZAR.compute-1.amazonaws.com
```

Al cambiar de cluster, solo cambiar ese valor. El dashboard y `sync_from_aws.ps1` lo leen automaticamente.

## Orden de reconstruccion

1. Crear EMR nuevo con Hadoop, Spark y Flink.
2. Confirmar SSH al nuevo primary.
3. Restaurar `/home/hadoop/bigdata-kafka` desde S3.
4. Instalar Kafka 3.6.2 en `/home/hadoop/kafka`.
5. Regenerar `kraft-server.properties` con `$(hostname -f)`.
6. Iniciar Kafka y crear topics.
7. Instalar dependencias Python `boto3` y `kafka-python`.
8. Ejecutar producer con muestra desde S3 hacia `raw_youtube_chat`.
9. Ejecutar Spark Batch desde Kafka.
10. Ejecutar Flink Streaming desde Kafka.
11. Sincronizar dashboard local.

## Comandos clave dentro del EMR

```bash
mkdir -p /home/hadoop/bigdata-kafka/{config,scripts,producers,spark/jobs,flink/jobs,flink/scripts,docs,logs}
aws s3 sync s3://figuretibucket/codes/kafka/scripts/ /home/hadoop/bigdata-kafka/scripts/
aws s3 sync s3://figuretibucket/codes/kafka/config/ /home/hadoop/bigdata-kafka/config/
aws s3 sync s3://figuretibucket/codes/kafka/producers/ /home/hadoop/bigdata-kafka/producers/
aws s3 sync s3://figuretibucket/codes/kafka/flink/ /home/hadoop/bigdata-kafka/flink/
aws s3 sync s3://figuretibucket/docs/kafka/ /home/hadoop/bigdata-kafka/docs/
aws s3 cp s3://figuretibucket/codes/kafka/spark_read_kafka_raw_youtube.py /home/hadoop/bigdata-kafka/spark/jobs/
aws s3 cp s3://figuretibucket/codes/kafka/spark_rules_from_kafka_parquet.py /home/hadoop/bigdata-kafka/spark/jobs/
aws s3 cp s3://figuretibucket/codes/kafka/spark_apply_offendes_from_kafka_parquet.py /home/hadoop/bigdata-kafka/spark/jobs/
aws s3 cp s3://figuretibucket/codes/kafka/spark_hybrid_scoring_from_kafka.py /home/hadoop/bigdata-kafka/spark/jobs/
chmod +x /home/hadoop/bigdata-kafka/scripts/*.sh /home/hadoop/bigdata-kafka/flink/scripts/*.sh
```

```bash
cd /home/hadoop
wget -q https://archive.apache.org/dist/kafka/3.6.2/kafka_2.12-3.6.2.tgz
tar -xzf kafka_2.12-3.6.2.tgz
ln -sfn /home/hadoop/kafka_2.12-3.6.2 /home/hadoop/kafka
PRIVATE_DNS=$(hostname -f)
sed "s/__ADVERTISED_HOST__/${PRIVATE_DNS}/g" \
  /home/hadoop/bigdata-kafka/config/kraft-server.properties.template \
  > /home/hadoop/bigdata-kafka/config/kraft-server.properties
```

```bash
/home/hadoop/bigdata-kafka/scripts/start_kafka.sh
/home/hadoop/bigdata-kafka/scripts/create_topics.sh
python3 -m pip install --user boto3 kafka-python
python3 /home/hadoop/bigdata-kafka/producers/produce_youtube_chat_from_s3.py \
  --s3-path s3://figuretibucket/data/raw/youtube/youtube_lake.csv \
  --bootstrap-server localhost:9092 \
  --topic raw_youtube_chat \
  --limit 105 \
  --delay-ms 20
```

## Validacion rapida

```bash
/home/hadoop/bigdata-kafka/scripts/status_kafka.sh
/home/hadoop/bigdata-kafka/scripts/list_topics.sh
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic raw_youtube_chat
aws s3 ls s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/
aws s3 ls s3://figuretibucket/output/batch/from_kafka/
```

## Dashboard

En local:

```powershell
cd C:\Users\itsma\Documents\BigData\Proyectofinal\emr_kafka_setup\dashboard
powershell -ExecutionPolicy Bypass -File .\start_dashboard.ps1
```

Sincronizar usando `.env`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync_from_aws.ps1
```

Si se quiere forzar un host:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\sync_from_aws.ps1 -HostName ec2-REEMPLAZAR.compute-1.amazonaws.com
```
