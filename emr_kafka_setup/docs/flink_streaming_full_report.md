# Flink Streaming Full Report

Fecha de validacion: 2026-06-17 UTC

## Arquitectura validada

Kafka `raw_youtube_chat` -> Flink Streaming -> Kafka `nlp_stream_results` / Kafka `alerts_polarization` -> Dashboard futuro.

Este bloque complementa la arquitectura del proyecto:

S3 Raw -> Python Producer -> Kafka -> Spark Batch / Flink Streaming -> S3 Curated / Kafka Results -> Dashboard.

## Entorno EMR

- Nodo master: `ip-172-31-14-56`
- Kafka: `/home/hadoop/kafka`, version 3.6.2, modo KRaft single-node
- Proyecto: `/home/hadoop/bigdata-kafka`
- Flink: `1.17.1-amzn-1`
- Java: `OpenJDK 1.8.0_492`
- Python: `3.7.16`
- PyFlink: no disponible en el entorno, por eso se implemento con Java DataStream API.
- Bootstrap local: `localhost:9092`
- Bootstrap interno del cluster: `ip-172-31-14-56.ec2.internal:9092`

## Artefactos implementados

- Codigo Java Flink: `/home/hadoop/bigdata-kafka/flink/jobs/FlinkKafkaStreamingJobs.java`
- JAR compilado: `/home/hadoop/bigdata-kafka/flink/jobs/flink-streaming-jobs.jar`
- Script build: `/home/hadoop/bigdata-kafka/flink/scripts/build_flink_jobs.sh`
- Script Job 1: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job1_normalize_stream.sh`
- Script Job 2: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job2_window_metrics.sh`
- Script Job 3: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job3_political_signals.sh`
- Script Job 4: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job4_actor_polarization.sh`
- Script Job 5: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job5_risk_alerts.sh`

La implementacion usa Flink DataStream con `KafkaConsumer` y `KafkaProducer` dentro de `SourceFunction` y `SinkFunction`, empaquetando dependencias de Kafka en un fat JAR. Esto evita instalar dependencias globales y mantiene aislada la prueba academica.

## Topics usados

- Entrada: `raw_youtube_chat`
- Resultados streaming: `nlp_stream_results`
- Alertas: `alerts_polarization`

Offsets finales verificados:

```text
raw_youtube_chat:0:34
raw_youtube_chat:1:31
raw_youtube_chat:2:40
nlp_stream_results:0:94
nlp_stream_results:1:39
nlp_stream_results:2:80
alerts_polarization:0:0
alerts_polarization:1:0
alerts_polarization:2:3
```

Conteos aproximados por topic:

- `raw_youtube_chat`: 105 mensajes
- `nlp_stream_results`: 213 mensajes
- `alerts_polarization`: 3 mensajes

## Job 1 - Normalizacion streaming

- Entrada: `raw_youtube_chat`
- Salida: `nlp_stream_results`
- Event type: `normalized_comment`
- Script: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job1_normalize_stream.sh`
- Capacidad Flink demostrada: procesamiento evento por evento con baja latencia.

Que hace:

- Parsea JSON desde Kafka.
- Usa `message_clean`, si no existe usa `message_raw`, si no existe usa string vacio.
- Normaliza a minusculas, reduce espacios multiples y calcula longitud.
- Conserva `event_id`, particion y offset.

Comando ejecutado:

```bash
MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job1_normalize_stream.sh > /home/hadoop/bigdata-kafka/logs/flink_job1_normalize_stream.log 2>&1
```

Evidencia:

- JobID: `5d9d7d917c716eb2308ddec2a934e399`
- Runtime: `2381 ms`
- Muestra consumida desde `nlp_stream_results`:

```json
{"job_name":"flink_job1_normalize_stream","event_type":"normalized_comment","source_topic":"raw_youtube_chat","source_partition":2,"source_offset":0,"payload":{"stream_text":"como keiko esta ganando si en el extranjeto los peruanos halla dicen que no han podido votar en argentina y espana y otros paises","message_length":129,"is_empty_message":false}}
```

Por que corresponde a Flink y no Spark:

Flink procesa cada evento apenas llega al stream, sin esperar a cerrar un batch ni escribir primero en S3.

## Job 2 - Metricas por ventanas

- Entrada: `raw_youtube_chat`
- Salida: `nlp_stream_results`
- Event type: `window_metrics`
- Script: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job2_window_metrics.sh`
- Capacidad Flink demostrada: windowing streaming por tiempo de procesamiento.

Que hace:

- Agrupa comentarios en ventanas de procesamiento.
- Calcula `comment_count`, autores unicos aproximados, longitud promedio, vacios y spam simple.

Comando ejecutado:

```bash
MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job2_window_metrics.sh > /home/hadoop/bigdata-kafka/logs/flink_job2_window_metrics.log 2>&1
```

Evidencia:

- JobID: `fc9d0864061ff3eb5cd86afd442fe944`
- Runtime: `3918 ms`
- Los resultados se publicaron en `nlp_stream_results` junto con los demas eventos streaming.

Por que corresponde a Flink y no Spark:

La metrica se calcula como ventana temporal continua sobre Kafka; es una operacion natural de streaming.

## Job 3 - Deteccion de senales politicas

- Entrada: `raw_youtube_chat`
- Salida: `nlp_stream_results`
- Event type: `political_signals`
- Script: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job3_political_signals.sh`
- Capacidad Flink demostrada: clasificacion por evento en streaming.

Que hace:

- Detecta reglas locales: terruqueo, fraude, instituciones electorales, menciones politicas, polarizacion, lenguaje discriminatorio, insultos e indicadores de spam.
- Genera `local_risk_score_stream` y `local_rule_tags`.

Comando ejecutado:

```bash
MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job3_political_signals.sh > /home/hadoop/bigdata-kafka/logs/flink_job3_political_signals.log 2>&1
```

Evidencia:

- JobID: `e944fcfe1b84f5e1f39eb809075bc510`
- Runtime: `2238 ms`

Por que corresponde a Flink y no Spark:

Las reglas ligeras pueden activarse en milisegundos para alimentar resultados operativos o dashboards.

## Job 4 - Polarizacion por actor politico

- Entrada: `raw_youtube_chat`
- Salida: `nlp_stream_results`
- Event type: `actor_polarization_window`
- Script: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job4_actor_polarization.sh`
- Capacidad Flink demostrada: agregacion temporal por clave.

Que hace:

- Detecta actores como Keiko/Fujimori/FP, Castillo/Peru Libre, JP, RLA/Porky, Antauro y ONPE/JNE.
- Agrupa por actor y ventana.
- Calcula menciones, insultos, fraude, terruqueo, discriminacion y `polarization_score`.

Comando ejecutado:

```bash
MAX_MESSAGES=105 DELAY_MS=20 WINDOW_SECONDS=2 /home/hadoop/bigdata-kafka/flink/scripts/flink_job4_actor_polarization.sh > /home/hadoop/bigdata-kafka/logs/flink_job4_actor_polarization.log 2>&1
```

Evidencia:

- JobID: `173bc7174462903904e3f57687a81aea`
- Runtime: `3800 ms`

Por que corresponde a Flink y no Spark:

La agregacion por actor y ventana permite observar cambios de conversacion en tiempo casi real.

## Job 5 - Alertas de riesgo

- Entrada: `raw_youtube_chat`
- Salida: `alerts_polarization`
- Event type: `risk_alert`
- Script: `/home/hadoop/bigdata-kafka/flink/scripts/flink_job5_risk_alerts.sh`
- Capacidad Flink demostrada: deteccion de eventos criticos y ruteo a topic especializado.

Que hace:

- Emite alerta cuando detecta combinaciones como terruqueo + insulto, fraude + institucion electoral, discriminacion, homofobia o riesgo local alto.
- Genera `alert_id`, `alert_type`, `severity`, `reason`, `actor`, `message_text`, `local_rule_tags` y `local_risk_score_stream`.

Comando ejecutado:

```bash
MAX_MESSAGES=105 DELAY_MS=5 /home/hadoop/bigdata-kafka/flink/scripts/flink_job5_risk_alerts.sh > /home/hadoop/bigdata-kafka/logs/flink_job5_risk_alerts.log 2>&1
```

Evidencia:

- JobID: `8515f28a0afe4b766c09ab9ab98f9117`
- Runtime: `2205 ms`
- Alertas detectadas: 3

Muestra consumida desde `alerts_polarization`:

```json
{"job_name":"flink_job5_risk_alerts","event_type":"risk_alert","source_topic":"raw_youtube_chat","source_partition":2,"source_offset":12,"payload":{"alert_type":"terruqueo_plus_insult","severity":"medium","reason":"terruqueo|electoral_institution|political_mention|general_insult","actor":"keiko_fujimori_fp","local_risk_score_stream":4}}
```

Por que corresponde a Flink y no Spark:

Las alertas deben separarse y publicarse en un topic operativo apenas se detectan, sin esperar procesamiento batch.

## Comandos de validacion usados

```bash
/home/hadoop/bigdata-kafka/scripts/status_kafka.sh
/home/hadoop/bigdata-kafka/scripts/list_topics.sh
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic raw_youtube_chat
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic nlp_stream_results
/home/hadoop/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic alerts_polarization
timeout 20s /home/hadoop/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nlp_stream_results --from-beginning --max-messages 12
timeout 20s /home/hadoop/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts_polarization --from-beginning --max-messages 3
```

## Evidencias locales

- Log Job 1: `/home/hadoop/bigdata-kafka/logs/flink_job1_normalize_stream.log`
- Log Job 2: `/home/hadoop/bigdata-kafka/logs/flink_job2_window_metrics.log`
- Log Job 3: `/home/hadoop/bigdata-kafka/logs/flink_job3_political_signals.log`
- Log Job 4: `/home/hadoop/bigdata-kafka/logs/flink_job4_actor_polarization.log`
- Log Job 5: `/home/hadoop/bigdata-kafka/logs/flink_job5_risk_alerts.log`
- Muestra NLP: `/home/hadoop/bigdata-kafka/logs/flink_nlp_stream_results_sample.jsonl`
- Muestra alertas: `/home/hadoop/bigdata-kafka/logs/flink_alerts_sample.jsonl`

## Copias esperadas en S3

- `s3://figuretibucket/codes/kafka/flink/FlinkKafkaStreamingJobs.java`
- `s3://figuretibucket/codes/kafka/flink/flink-streaming-jobs.jar`
- `s3://figuretibucket/codes/kafka/flink/scripts/`
- `s3://figuretibucket/docs/kafka/flink_streaming_full_report.md`
- `s3://figuretibucket/docs/kafka/current_architecture_mermaid.md`

## Limitaciones

- Kafka es self-managed en el master EMR por restriccion de AWS Academy; no es una arquitectura productiva equivalente a Amazon MSK.
- La validacion uso una muestra pequena de 105 mensajes, no el dataset completo de 160,464 comentarios.
- Flink ejecuta reglas rapidas de baja latencia; el NLP pesado y el modelo OffendES se mantienen en Spark Batch.
- El dashboard queda como siguiente fase.
- OffendES y las reglas locales son senales analiticas, no verdad final sobre intencion politica o toxicidad.

## Proximo paso

Conectar un dashboard o consumidor operativo a `nlp_stream_results` y `alerts_polarization`, y luego ejecutar una prueba mayor controlada con 1000 mensajes antes de considerar una corrida completa.
