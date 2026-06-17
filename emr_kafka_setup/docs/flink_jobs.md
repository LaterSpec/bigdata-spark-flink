Continuemos con el bloque Flink Streaming y dejemos implementada la mejor arquitectura posible dentro de las restricciones del entorno EMR/AWS Academy.

Contexto actual:
- Arquitectura nueva del proyecto:
  S3 Raw -> Python Producer -> Kafka -> Spark Batch / Flink Streaming -> S3 Curated / Kafka Results -> Dashboard
- Kafka está instalado self-managed en el master EMR por restricción de AWS Academy, ya que no se puede usar Amazon MSK.
- Kafka 3.6.2:
  /home/hadoop/kafka
- Proyecto Kafka:
  /home/hadoop/bigdata-kafka
- Bootstrap local:
  localhost:9092
- Bootstrap interno del cluster:
  ip-172-31-14-56.ec2.internal:9092
- Para procesos distribuidos, usar:
  ip-172-31-14-56.ec2.internal:9092
  no localhost.
- Topics ya creados:
  raw_youtube_chat
  nlp_stream_results
  alerts_polarization
  nlp_batch_results
- Producer Python:
  /home/hadoop/bigdata-kafka/producers/produce_youtube_chat_from_s3.py
- Dataset raw:
  s3://figuretibucket/data/raw/youtube/youtube_lake.csv
- Spark Batch ya quedó adaptado a Kafka con 5 jobs:
  Job 1 Kafka Raw Ingest
  Job 2 Reglas locales desde Kafka Parquet
  Job 3 OffendES Spark ML reutilizado desde S3
  Job 4 Inferencia OffendES sobre datos Kafka
  Job 5 Scoring híbrido + agregados
- Reporte Spark:
  /home/hadoop/bigdata-kafka/docs/spark_batch_from_kafka_full_report.md
  s3://figuretibucket/docs/kafka/spark_batch_from_kafka_full_report.md

Objetivo:
Implementar y validar el bloque completo de Flink Streaming consumiendo desde Kafka topic raw_youtube_chat.

La arquitectura esperada para Flink es:
Kafka raw_youtube_chat -> Flink Streaming Jobs -> Kafka nlp_stream_results / Kafka alerts_polarization -> Dashboard futuro

Se deben dejar 5 jobs Flink diferenciados, cada uno demostrando una capacidad distinta de streaming:

1. Flink Job 1 - Normalización streaming
2. Flink Job 2 - Métricas por ventanas
3. Flink Job 3 - Detección de señales políticas
4. Flink Job 4 - Polarización por actor político
5. Flink Job 5 - Alertas de riesgo

Importante:
- Primero inspecciona si Flink ya está instalado/disponible en EMR.
- Detecta versión de Flink, Java y Python.
- Si PyFlink está disponible, úsalo.
- Si PyFlink no está disponible o se complica con conectores Kafka, implementa con Flink Java/Scala si el entorno lo soporta.
- Si Flink no está instalado en el cluster actual, documenta el bloqueo y deja scripts/código listos, pero primero intenta verificar si EMR ya lo trae.
- No rompas Kafka ni Spark.
- No borres topics ni outputs existentes.
- No ejecutes la carga completa de 160,464 comentarios todavía.
- Usa primero los 105 mensajes ya existentes en Kafka.
- Si se necesita más data para validar ventanas, puedes ejecutar el producer con máximo 1000 mensajes y delay pequeño, pero no full.

Rutas base:
- Código local Flink:
  /home/hadoop/bigdata-kafka/flink/jobs/
- Docs locales:
  /home/hadoop/bigdata-kafka/docs/
- Logs locales:
  /home/hadoop/bigdata-kafka/logs/
- Código S3:
  s3://figuretibucket/codes/kafka/flink/
- Docs S3:
  s3://figuretibucket/docs/kafka/
- Outputs opcionales S3 para evidencia:
  s3://figuretibucket/output/streaming/flink/

Diseño recomendado:
Preferentemente crear 5 scripts separados para que el informe pueda decir claramente que hay 5 jobs:
- flink_job1_normalize_stream.py
- flink_job2_window_metrics.py
- flink_job3_political_signals.py
- flink_job4_actor_polarization.py
- flink_job5_risk_alerts.py

Si por limitaciones del entorno conviene más hacer un solo pipeline, crea:
- flink_streaming_pipeline_5_jobs.py
pero documenta internamente las 5 secciones/jobs y sus salidas.

Todos los jobs deben leer desde:
- Kafka topic: raw_youtube_chat
- Bootstrap: ip-172-31-14-56.ec2.internal:9092

Salidas:
- Jobs 1, 2, 3 y 4 deben escribir resultados JSON al topic:
  nlp_stream_results
- Job 5 debe escribir alertas JSON al topic:
  alerts_polarization

Formato base de mensajes de salida:
Cada mensaje producido por Flink debe incluir:
- job_name
- event_type
- processing_ts
- source_topic
- source_partition si está disponible
- source_offset si está disponible
- payload con los campos procesados

Job 1 - Normalización streaming:
Entrada:
- raw_youtube_chat

Debe hacer:
- Parsear JSON.
- Elegir texto principal:
  message_clean si existe;
  si no, message_raw;
  si no, string vacío.
- Normalizar:
  minúsculas;
  espacios múltiples;
  texto nulo;
  longitud del mensaje.
- Generar:
  stream_text
  message_length
  is_empty_message
  processing_ts
  event_id si existe
  kafka metadata si está disponible

Salida:
- nlp_stream_results
- event_type: normalized_comment

Debe demostrar:
- procesamiento evento por evento de baja latencia.

Job 2 - Métricas por ventanas:
Entrada:
- raw_youtube_chat

Debe hacer:
- Calcular ventanas de tiempo de procesamiento, por ejemplo 30 segundos o 1 minuto.
- Métricas:
  comment_count
  unique_authors si es viable
  avg_message_length
  empty_count
  spam_like_count simple
- Generar:
  window_start
  window_end
  metrics

Salida:
- nlp_stream_results
- event_type: window_metrics

Debe demostrar:
- windowing streaming.

Job 3 - Detección de señales políticas:
Entrada:
- raw_youtube_chat

Debe hacer reglas rápidas por evento para:
- has_terruqueo
- has_fraude
- has_electoral_institution
- has_political_mention
- has_polarization_signal
- has_discriminatory_language
- has_ethnic_racial_slur
- has_homophobic_slur
- has_general_insult
- is_spam_noise
- local_risk_score_stream
- local_rule_tags

Usar términos locales similares a Spark:
- terruco, terruqueo, senderista, rojo, comunista, movadef, mrta
- fraude, robo, actas falsas, actas impugnadas, irregularidades
- ONPE, JNE, actas, mesa, personeros, votos, conteo
- Keiko, Fujimori, FP, JP, Juntos por el Perú, Castillo, Perú Libre, Antauro, Porky
- cholo, serrano, paisano, indio, llama
- kbro, kbros, cabro, rosquete
- mierda, csm, ctm, burro, ignorante, rata, lacra

Salida:
- nlp_stream_results
- event_type: political_signals

Debe demostrar:
- clasificación por evento en streaming.

Job 4 - Polarización por actor político:
Entrada:
- raw_youtube_chat

Debe hacer:
- Detectar actor político mencionado:
  Keiko/Fujimori/FP
  Castillo/Peru Libre
  JP/Juntos por el Peru
  Lopez Aliaga/Porky/RLA
  Antauro
  ONPE/JNE como instituciones
- Agrupar por ventanas de 30 segundos o 1 minuto.
- Calcular:
  mention_count
  insult_count
  fraud_count
  terruqueo_count
  discriminatory_count
  polarization_score

Salida:
- nlp_stream_results
- event_type: actor_polarization_window

Debe demostrar:
- agregación temporal por clave en streaming.

Job 5 - Alertas de riesgo:
Entrada:
- raw_youtube_chat
Idealmente reutilizar la misma lógica de reglas locales streaming.

Debe generar alertas cuando:
- terruqueo + insulto;
- fraude + institución electoral;
- discriminación étnica/racial;
- homofobia;
- local_risk_score_stream alto;
- pico de menciones polarizadas si se puede detectar por ventana.

Campos:
- alert_id
- alert_type
- severity
- reason
- event_id si existe
- actor si existe
- message_text
- local_rule_tags
- local_risk_score_stream
- created_at

Salida:
- alerts_polarization
- event_type: risk_alert

Debe demostrar:
- detección de eventos críticos en streaming y separación de topic de alertas.

Validación mínima:
1. Confirmar Kafka corriendo:
   /home/hadoop/bigdata-kafka/scripts/status_kafka.sh
2. Confirmar topics:
   /home/hadoop/bigdata-kafka/scripts/list_topics.sh
3. Verificar que raw_youtube_chat tiene mensajes.
4. Ejecutar cada job Flink con duración controlada o modo bounded si es posible.
5. Consumir muestras desde:
   nlp_stream_results
   alerts_polarization
6. Guardar evidencia de:
   - mensajes producidos por cada job;
   - counts aproximados si se puede;
   - logs de ejecución;
   - comandos usados.

Si Flink queda corriendo como streaming infinito:
- Ejecutarlo con timeout controlado o detenerlo manualmente luego de validar.
- Documentar cómo iniciarlo y cómo detenerlo.
- No dejar procesos duplicados consumiendo indefinidamente sin documentar.

Documentación final obligatoria:
Crear:
  /home/hadoop/bigdata-kafka/docs/flink_streaming_full_report.md

Copiar a:
  s3://figuretibucket/docs/kafka/flink_streaming_full_report.md

El reporte debe incluir:
- Arquitectura:
  Kafka raw_youtube_chat -> Flink Streaming -> nlp_stream_results / alerts_polarization
- Los 5 jobs Flink.
- Para cada job:
  - nombre;
  - entrada;
  - salida;
  - script;
  - qué hace;
  - capacidad Flink que demuestra;
  - comando de ejecución;
  - evidencia de prueba;
  - por qué corresponde a Flink y no Spark.
- Topics usados.
- Limitaciones:
  - Kafka self-managed en EMR master por AWS Academy;
  - muestra pequeña de 105 o 1000 mensajes;
  - modelo NLP pesado se mantiene en Spark, Flink usa reglas rápidas de baja latencia;
  - dashboard queda como siguiente fase si aún no existe.
- Próximo paso:
  conectar dashboard o ejecutar prueba con 1000/full.

También crear o actualizar:
  /home/hadoop/bigdata-kafka/docs/current_architecture_mermaid.md

Con este Mermaid actualizado:

flowchart LR
    A["YouTube Live Chat JSON/CSV raw<br/>160,464 comentarios"] --> B["S3 Data Lake Raw<br/>fuente durable"]
    B --> C["Python Producer<br/>simula streaming desde S3"]
    C --> D["Kafka self-managed en EMR master<br/>topic: raw_youtube_chat"]

    D --> E["Spark Batch en EMR<br/>5 jobs batch"]
    D --> F["Flink Streaming en EMR<br/>5 jobs streaming"]

    E --> G["S3 Curated<br/>predicciones, reglas, híbrido, agregados"]
    F --> H["Kafka topic<br/>nlp_stream_results"]
    F --> I["Kafka topic<br/>alerts_polarization"]

    G --> J["Dashboard futuro"]
    H --> J
    I --> J

    K["Dataset externo OffendES<br/>labels de ofensividad"] --> L["Spark ML Training<br/>modelo base NLP"]
    L --> E

    M["Reglas locales peruanas<br/>terruqueo, fraude, ONPE, JNE,<br/>actores políticos, insultos locales"] --> E
    M --> F

Copiar el Mermaid a:
  s3://figuretibucket/docs/kafka/current_architecture_mermaid.md

Criterio de éxito:
- Quedan implementados o, si el entorno bloquea, claramente preparados y documentados los 5 jobs Flink.
- Al menos una prueba real consume desde raw_youtube_chat.
- Se producen mensajes en nlp_stream_results.
- Si hay condiciones de alerta en la muestra, se producen mensajes en alerts_polarization; si no, documentar que no hubo alertas por contenido de muestra.
- Todo queda documentado y copiado a S3.
- No se rompe Kafka ni Spark.