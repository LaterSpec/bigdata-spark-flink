# Spark Batch Records - Proyecto Big Data

## 1. Objetivo de la fase Spark Batch

Esta fase implementa y documenta el procesamiento historico del proyecto **Deteccion y analisis del discurso discriminatorio y polarizacion politica en redes sociales usando Big Data**.

El objetivo fue dejar una etapa Spark Batch real y reproducible sobre AWS/EMR/S3, capaz de:

- procesar el historico completo de YouTube Live Chat electoral peruano;
- aplicar reglas locales multilabel para senales politicas y contextuales;
- entrenar modelos NLP distribuidos con `pyspark.ml` usando OffendES;
- aplicar inferencia sobre el lake completo;
- combinar modelo y reglas en una salida hibrida con scoring de riesgo;
- generar reportes y agregados historicos en S3.

## 2. Relacion con el documento del proyecto

Spark Batch cubre el procesamiento historico del corpus completo. Es la capa adecuada para analizar los 160,464 comentarios deduplicados, generar agregados por minuto, entrenar modelos NLP y producir salidas consolidadas para dashboard o informe.

NLP cubre ofensividad, odio y vulgaridad general en espanol mediante OffendES. Este dataset externo permite entrenar modelos supervisados porque el dataset peruano de YouTube no tiene etiquetas manuales.

Las reglas locales complementan al modelo porque OffendES no entiende directamente contexto politico peruano. Las reglas cubren terruqueo, fraude electoral, instituciones electorales, menciones politicas, polarizacion, discriminacion contextual peruana e insultos.

## 3. Infraestructura usada

- Bucket S3: `s3://figuretibucket/`
- EMR: 3 nodos, 1 Primary/Master y 2 Core/Workers.
- Spark ejecutado en EMR.
- Data Lake raw: `s3://figuretibucket/data/raw/youtube/youtube_lake.csv`
- Dataset externo OffendES en S3.

## 4. Estructura S3 usada

```text
s3://figuretibucket/
  codes/
    spark/
      spark_batch_rules_youtube.py
      spark_train_offendes.py
      spark_apply_offendes_youtube.py
      spark_hybrid_diagnostics.py
    local_baseline/
    flink/
    producer/

  data/
    raw/
      youtube/
        youtube_lake.csv
    processed/
      youtube_classified/

  dataset/
    offendES/
      train.parquet
      validation.parquet
      test.parquet

  output/
    batch/
      predictions/
      aggregates_by_minute/
      reports/
      models/
    streaming/
      classified/
      alerts/
      metrics/

  logs/
    emr/
```

## 5. Dataset principal YouTube

Entrada principal:

```text
s3://figuretibucket/data/raw/youtube/youtube_lake.csv
```

Total procesado:

```text
160,464 comentarios deduplicados
```

Columnas usadas principalmente:

- `source_file`
- `video_id`
- `timestamp_text`
- `timestamp_usec`
- `video_offset_msec`
- `author`
- `message`

`video_offset_msec` permitio generar agregados historicos por minuto.

## 6. Dataset externo OffendES

Entradas:

```text
s3://figuretibucket/dataset/offendES/train.parquet
s3://figuretibucket/dataset/offendES/validation.parquet
s3://figuretibucket/dataset/offendES/test.parquet
```

Labels:

- `0 = ofensivo_directo`
- `1 = odio_agresion_grupal`
- `2 = neutral_no_ofensivo`
- `3 = vulgaridad_contextual`

Mapping binario usado:

- `labels 0 y 1 = ofensivo`
- `labels 2 y 3 = no_ofensivo`

## 7. Enfoque hibrido

La solucion final de la fase Spark Batch no depende solo del modelo. Se usa un enfoque hibrido:

- Modelo Spark ML: detecta ofensividad, odio y vulgaridad general en espanol desde OffendES.
- Reglas multilabel: detectan senales locales peruanas como terruqueo, fraude, instituciones electorales, menciones politicas, polarizacion e insultos.
- `local_risk_score`: puntaje local basado en reglas.
- `hybrid_risk_level`: nivel `bajo`, `medio` o `alto` combinando confianza del modelo y reglas locales.
- `hybrid_risk_reason`: explicacion textual de por que se asigno el nivel de riesgo.

Conclusion metodologica: el resultado defendible para riesgo politico/local es el hibrido, no el `pred_binary_sparkml` crudo.

## 8. Jobs Spark Batch documentados

### Job Spark 1 - Ingesta batch desde Data Lake S3

- Estado: completado.
- Entrada: `s3://figuretibucket/data/raw/youtube/youtube_lake.csv`
- Salida: DataFrame Spark historico de YouTube.
- Que hace: lee el lake raw desde S3, preservando columnas de origen y mensajes originales.
- Capacidad tecnica demostrada: lectura distribuida desde S3 en EMR.
- Por que Spark Batch y no Flink: se procesa historico completo ya almacenado, no eventos en tiempo real.

### Job Spark 2 - Reglas locales multilabel

- Estado: completado.
- Entrada: `s3://figuretibucket/data/raw/youtube/youtube_lake.csv`
- Salida: `s3://figuretibucket/output/batch/predictions/run_rules_v1_full/`
- Que hace: aplica reglas locales para terruqueo, fraude, instituciones electorales, politica, polarizacion, discriminacion, insultos y spam/ruido.
- Capacidad tecnica demostrada: transformaciones Spark SQL, regex, columnas booleanas multilabel y scoring local.
- Por que Spark Batch y no Flink: se analiza todo el corpus historico en una corrida reproducible; Flink se usara luego para streaming.

### Job Spark 3 - Agregados historicos por minuto

- Estado: completado.
- Entrada: `s3://figuretibucket/output/batch/predictions/run_rules_v1_full/`
- Salida: `s3://figuretibucket/output/batch/aggregates_by_minute/run_rules_v1_full/`
- Que hace: genera agregados por minuto usando `video_offset_msec`.
- Capacidad tecnica demostrada: agregaciones historicas por ventana temporal derivada.
- Por que Spark Batch y no Flink: se calculan metricas historicas sobre datos ya acumulados; Flink servira para ventanas en vivo.

### Job Spark 4 - Entrenamiento NLP con OffendES usando pyspark.ml

- Estado: completado.
- Entrada: OffendES `train.parquet`, `validation.parquet`, `test.parquet`.
- Salidas:
  - `s3://figuretibucket/output/batch/models/offendes_binary_sparkml/`
  - `s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/`
  - `s3://figuretibucket/output/batch/reports/spark_ml_training/summary.md`
- Que hace: entrena modelos binario y multiclase con `RegexTokenizer`, `StopWordsRemover`, `HashingTF`, `IDF` y `LogisticRegression`.
- Capacidad tecnica demostrada: entrenamiento distribuido de NLP con Spark ML.
- Por que Spark Batch y no Flink: entrenamiento ML supervisado se realiza offline sobre datasets etiquetados, no como procesamiento evento a evento.

### Job Spark 5 - Inferencia hibrida sobre YouTube con Spark ML + reglas + risk score

- Estado: completado.
- Entradas:
  - `s3://figuretibucket/data/raw/youtube/youtube_lake.csv`
  - modelos Spark ML OffendES
  - `s3://figuretibucket/output/batch/predictions/run_rules_v1_full/`
- Salidas:
  - `s3://figuretibucket/output/batch/predictions/run_sparkml_offendes_full/`
  - `s3://figuretibucket/output/batch/predictions/run_hybrid_sparkml_rules_full/`
  - `s3://figuretibucket/output/batch/predictions/run_hybrid_scored_full_v2/`
  - `s3://figuretibucket/output/batch/reports/hybrid_diagnostics_v2/summary.md`
- Que hace: aplica modelos Spark ML a YouTube, combina predicciones con reglas locales y genera columnas de riesgo hibrido.
- Capacidad tecnica demostrada: inferencia distribuida, union logica modelo-reglas, diagnostico y explicabilidad basica.
- Por que Spark Batch y no Flink: se clasifica el historico completo y se generan salidas consolidadas; Flink se usara luego para inferencia/alertas streaming.

## 9. Resultados Spark Rules full

Corrida:

```text
s3://figuretibucket/output/batch/predictions/run_rules_v1_full/
```

Reporte:

```text
s3://figuretibucket/output/batch/reports/run_rules_v1_full/summary.md
```

Resultados sobre 160,464 comentarios:

| flag | count | porcentaje |
|---|---:|---:|
| `has_terruqueo` | 6,498 | 4.05% |
| `has_fraude` | 8,070 | 5.03% |
| `has_electoral_institution` | 7,160 | 4.46% |
| `has_political_mention` | 50,322 | 31.36% |
| `has_polarization_signal` | 9,146 | 5.70% |
| `has_discriminatory_language` | 2,232 | 1.39% |
| `has_ethnic_racial_slur` | 2,064 | 1.29% |
| `has_homophobic_slur` | 168 | 0.10% |
| `has_general_insult` | 5,146 | 3.21% |
| `is_spam_noise` | 5,250 | 3.27% |

`local_risk_score`:

- promedio: `0.5698`
- minimo: `0`
- maximo: `8`

Validacion de estabilidad:

- Comparado con la corrida de 70k, todas las diferencias estuvieron debajo de `0.43` puntos porcentuales.
- Esto permite defender que las reglas se mantienen estables al pasar de muestra grande a full.

## 10. Resultados Spark ML

Entrenamiento:

```text
s3://figuretibucket/output/batch/reports/spark_ml_training/summary.md
```

Modelos:

```text
s3://figuretibucket/output/batch/models/offendes_binary_sparkml/
s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/
```

Metricas test OffendES:

| modelo | accuracy | macro F1 |
|---|---:|---:|
| Binario | 0.838968 | 0.749030 |
| Multiclase | 0.771939 | 0.548677 |

Lectura:

- El modelo binario es util como detector general de ofensividad.
- El modelo multiclase aporta interpretabilidad analitica, pero su macro F1 es menor por desbalance, especialmente en `odio_agresion_grupal`.

## 11. Diagnostico de sobre-deteccion Spark ML

Aplicacion sobre YouTube:

```text
s3://figuretibucket/output/batch/predictions/run_sparkml_offendes_full/
```

Resultado crudo:

- `pred_binary_sparkml`: `53.25% ofensivo`, `46.75% no_ofensivo`.

Diagnostico:

- Este `53.25%` no se toma como metrica final.
- Se considera inflado por cambio de dominio: OffendES proviene de comentarios generales en espanol, mientras que YouTube es chat politico peruano con ruido, spam, ironia y contexto electoral.
- `60.31%` de predicciones binarias caen en confianza `0.50-0.70`, zona gris.
- Muchos ofensivos ML no tienen reglas locales activas.

Conclusion del diagnostico:

- `pred_binary_sparkml` crudo sirve como senal general de ofensividad, no como conclusion final de odio o riesgo politico.
- Para analisis defendible se usa la salida hibrida.

## 12. Resultados del hibrido

Entradas:

```text
s3://figuretibucket/output/batch/predictions/run_hybrid_sparkml_rules_full/
```

Diagnostico valido:

```text
s3://figuretibucket/output/batch/reports/hybrid_diagnostics_v2/summary.md
```

Salida puntuada:

```text
s3://figuretibucket/output/batch/predictions/run_hybrid_scored_full_v2/
```

Resultados:

- `ml_offensive_high_confidence`: `13.61%`
- `hybrid_risk_level`:
  - bajo: `52.37%`
  - medio: `42.44%`
  - alto: `5.19%`

Interpretacion:

- `hybrid_risk_level=alto` es la senal mas defendible de riesgo.
- `hybrid_risk_level=medio` incluye casos con ML ofensivo de baja/media confianza o reglas locales moderadas.
- `hybrid_risk_level=bajo` cubre comentarios sin senal fuerte de modelo ni reglas.

## 13. Limitaciones

- `is_spam_noise` en Spark Rules v1 es mas conservador que el baseline local por limitaciones tecnicas de regex/Unicode en Spark/Java Regex.
- OffendES no entiende directamente terruqueo, fraude electoral ni politica peruana.
- Terminos etnico-raciales como `cholo`, `serrano`, `indio` o `paisano` requieren revision contextual.
- Polarizacion no equivale automaticamente a odio.
- Vulgaridad contextual no debe tratarse automaticamente como odio.
- El modelo Spark ML crudo puede sobredetectar ofensividad por cambio de dominio.

## 14. Conclusion de la fase

La fase Spark Batch queda completada. Se implemento procesamiento historico real sobre S3/EMR/Spark, reglas locales multilabel, agregados por minuto, entrenamiento NLP distribuido con OffendES, inferencia sobre 160,464 comentarios y scoring hibrido explicable.

Queda pendiente:

- Kafka.
- Flink streaming.
- Dashboard.
- Informe final.

## 15. Checklist final

- [x] Data Lake S3
- [x] EMR 3 nodos
- [x] Spark Rules full
- [x] Spark ML training
- [x] Spark ML inference
- [x] Hybrid scoring
- [x] 5 jobs Spark Batch documentados
- [ ] Kafka
- [ ] Flink
- [ ] Dashboard
- [ ] Informe final
