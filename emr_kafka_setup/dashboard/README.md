# Radar Electoral Streaming Dashboard

Dashboard local para observar el flujo real:

```text
S3 Raw -> Python Producer -> Kafka -> Flink Streaming -> Kafka Results -> Dashboard
```

## Abrir localmente

Desde `emr_kafka_setup/dashboard`:

```bash
chmod +x ./start_dashboard.sh ./scripts/*.sh
./start_dashboard.sh
```

Luego abrir:

```text
http://127.0.0.1:8787
```

El arranque local solo sirve el frontend y `server.js`. No conecta a AWS, no descarga datos y no lee snapshots locales. La pagina inicia en cero.

## Iniciar streaming en AWS

En la UI pulsa `Conectar AWS`.

El navegador llama al servidor local:

```text
Browser -> Node local /api/aws/start -> bootstrap_emr_streaming.sh -> SSH -> EMR
```

Ese endpoint prepara Kafka, recrea los topics de la sesion, inicia los jobs Flink y lanza el producer desde S3. El total sale de `DATA_SIZE` en `.env`; por ejemplo `DATA_SIZE=10000` produce diez mil mensajes:

```text
--limit $DATA_SIZE --producer-delay-ms 10 --window-seconds 5
```

Despues de conectar, la UI consulta `/api/live-delta` cada 3 segundos. Los contadores, chats, alertas y graficos suben solo con los eventos recibidos durante la sesion actual.

El lector de deltas conserva cursor por topic/particion. Si Kafka produce mas rapido que el browser, el dashboard puede quedar con `lag`, pero sigue leyendo en orden y no salta al final del topic.

## Spark Batch cada 1,000 eventos

El dashboard consulta `/api/aws/status` y `/api/spark/status`. Cuando `raw_youtube_chat` alcanza un nuevo multiplo de `SPARK_BATCH_SIZE` (`1000` por defecto), llama `/api/spark/start` y agenda un batch Spark remoto.

Cada batch genera rutas S3 separadas por target:

```text
s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat/batch_0001000/
s3://figuretibucket/output/batch/from_kafka/batch_0001000/
```

El estado remoto vive en:

```text
/home/hadoop/bigdata-kafka/logs/spark_batch_status.json
```

Jobs ejecutados por batch:

- Job 1: Kafka `raw_youtube_chat` -> parquet S3.
- Job 2: reglas locales.
- Job 4: OffendES Spark ML.
- Job 5: scoring hibrido + agregados.

## Flujo visual

- `raw_youtube_chat` aparece como chat principal en la parte superior.
- `Eventos Flink` queda abajo como feed contenido de eventos Flink.
- `Comentarios filtrados` queda a su costado y muestra nombre, comentario y categoria/tag.
- El buscador solo filtra `Comentarios filtrados`.
- `Muestra validada`, `Eventos Flink`, `Alertas` y `Spark Batch` empiezan en `0`.
- `Spark Batch` permanece en `0` hasta que exista una metrica incremental real.
- `Spark Batch enrichment` muestra batches de 1,000 eventos, jobs internos y outputs S3.
- `Flink windows`, `alerts_polarization` y reglas agregan datos de forma acumulada.
- Si AWS no responde, la pagina queda vacia y muestra el error; no existe fallback local con JSONL llenos.

## Datos locales

`data/runtime/` queda reservado para cache de sesion si se necesita mas adelante. Los JSON/JSONL precargados no forman parte del dashboard streaming puro.

## Scripts utiles

Arranque remoto directo, si quieres probarlo sin la UI:

```bash
./scripts/bootstrap_emr_streaming.sh --producer-delay-ms 10 --window-seconds 5
```

Detener Kafka, producer y jobs Flink en EMR:

```bash
./scripts/stop_emr_streaming.sh
```

Lectura puntual de deltas remotos:

```bash
./scripts/live_delta_from_aws.sh --max-raw 20 --max-flink 20 --max-alerts 10
```
