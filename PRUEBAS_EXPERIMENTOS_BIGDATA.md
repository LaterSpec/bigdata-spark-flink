# Pruebas y experimentos Big Data

> **Evidencia histórica:** estas mediciones se conservan tal como fueron obtenidas. La topología operativa vigente está documentada en `architecture.md`.

Este documento registra la fase experimental de Spark ML. Las validaciones posteriores de Kafka, Flink, batches paralelos y dashboard están en `docs/DISTRIBUTED_RUNTIME_VALIDATION.md`; no se mezclan aquí para preservar los resultados originales.

## 1. Setup AWS/S3/EMR

### Infraestructura validada

- Bucket S3: `s3://figuretibucket/`
- Cluster EMR: 3 nodos.
- Master EMR: `ec2-100-56-102-9.compute-1.amazonaws.com`
- Usuario SSH validado: `hadoop`
- Spark en EMR: `3.4.1-amzn-2`
- Acceso S3 desde EMR validado con `aws s3 ls s3://figuretibucket/`.

### Estructura usada en S3

```text
s3://figuretibucket/
  codes/
    local_baseline/
    spark/
    flink/
    producer/
  data/
    raw/youtube/youtube_lake.csv
    processed/youtube_classified/
  dataset/
    offendES/train.parquet
    offendES/validation.parquet
    offendES/test.parquet
  output/
    batch/
      predictions/
      aggregates_by_minute/
      reports/
    streaming/
      classified/
      alerts/
      metrics/
  logs/
    emr/
```

## 2. Baseline local previo

Antes de Spark, se implemento un baseline local en `local_baseline/`:

- Modelo binario OffendES: `labels 0/1 = ofensivo`, `labels 2/3 = no_ofensivo`.
- Modelo multiclase OffendES:
  - `0 = ofensivo_directo`
  - `1 = odio_agresion_grupal`
  - `2 = neutral_no_ofensivo`
  - `3 = vulgaridad_contextual`
- Reglas locales peruanas multilabel:
  - terruqueo
  - fraude
  - instituciones electorales
  - menciones politicas
  - polarizacion
  - lenguaje discriminatorio
  - insultos generales
  - spam/ruido

## 3. Spark Rules v1

### Objetivo

Ejecutar reglas locales multilabel con Spark puro en EMR, sin `joblib`, sin `sklearn` y sin modelo ML todavia.

Script:

```text
s3://figuretibucket/codes/spark/spark_batch_rules_youtube.py
```

Entrada:

```text
s3://figuretibucket/data/raw/youtube/youtube_lake.csv
```

### Corridas realizadas

| corrida | limite | total procesado |
|---|---:|---:|
| `run_rules_v1_5000` | 5,000 | 5,000 |
| `run_rules_v1_70000` | 70,000 | 70,000 |
| `run_rules_v1_full` | sin limite | 160,464 |

### Outputs full

```text
s3://figuretibucket/output/batch/predictions/run_rules_v1_full/
s3://figuretibucket/output/batch/aggregates_by_minute/run_rules_v1_full/
s3://figuretibucket/output/batch/reports/run_rules_v1_full/summary.md
s3://figuretibucket/output/batch/reports/run_rules_v1_full/comparison_vs_70000.md
```

### Resultados full sobre 160,464 comentarios

| flag | count | porcentaje |
|---|---:|---:|
| `has_terruqueo` | 6,498 | 4.0495% |
| `has_fraude` | 8,070 | 5.0292% |
| `has_electoral_institution` | 7,160 | 4.4621% |
| `has_political_mention` | 50,322 | 31.3603% |
| `has_polarization_signal` | 9,146 | 5.6997% |
| `has_discriminatory_language` | 2,232 | 1.3910% |
| `has_ethnic_racial_slur` | 2,064 | 1.2863% |
| `has_homophobic_slur` | 168 | 0.1047% |
| `has_general_insult` | 5,146 | 3.2069% |
| `is_spam_noise` | 5,250 | 3.2718% |

Risk score full:

```text
avg = 0.5698
min = 0
max = 8
```

### Comparacion full vs 70k

| flag | full % | 70k % | delta pp |
|---|---:|---:|---:|
| `has_terruqueo` | 4.0495 | 4.3557 | -0.3062 |
| `has_fraude` | 5.0292 | 5.0100 | +0.0192 |
| `has_electoral_institution` | 4.4621 | 4.0343 | +0.4278 |
| `has_political_mention` | 31.3603 | 31.7257 | -0.3654 |
| `has_polarization_signal` | 5.6997 | 5.7471 | -0.0474 |
| `has_discriminatory_language` | 1.3910 | 1.0414 | +0.3496 |
| `has_ethnic_racial_slur` | 1.2863 | 0.9329 | +0.3534 |
| `has_homophobic_slur` | 0.1047 | 0.1086 | -0.0039 |
| `has_general_insult` | 3.2069 | 3.3129 | -0.1060 |
| `is_spam_noise` | 3.2718 | 3.1443 | +0.1275 |

Lectura: las reglas se mantienen estables al escalar de 70k a 160,464 comentarios. Todas las variaciones estan por debajo de 0.43 puntos porcentuales.

### Limitacion aceptada

`is_spam_noise` en Spark Rules v1 es mas conservador que el baseline local. Se acepto esta diferencia como limitacion tecnica por regex/Unicode en Spark/Java Regex. No se modificara por ahora para no bloquear el avance.

## 4. Proxima prueba: Spark ML OffendES

## 4. Spark ML OffendES

### Objetivo

- Entrenar modelos NLP distribuidos con `pyspark.ml`.
- Guardar modelo binario y multiclase en S3.
- Aplicar los modelos sobre `youtube_lake.csv`.
- Combinar predicciones ML con `run_rules_v1_full`.

### Scripts

```text
s3://figuretibucket/codes/spark/spark_train_offendes.py
s3://figuretibucket/codes/spark/spark_apply_offendes_youtube.py
```

### Modelos generados

```text
s3://figuretibucket/output/batch/models/offendes_binary_sparkml/
s3://figuretibucket/output/batch/models/offendes_multiclass_sparkml/
```

### Reporte de entrenamiento

```text
s3://figuretibucket/output/batch/reports/spark_ml_training/summary.md
s3://figuretibucket/output/batch/reports/spark_ml_training/metrics_summary.json
s3://figuretibucket/output/batch/reports/spark_ml_training/binary_confusion_matrix/
s3://figuretibucket/output/batch/reports/spark_ml_training/multiclass_confusion_matrix/
```

### Metricas en test OffendES

Modelo binario:

| metric | value |
|---|---:|
| accuracy | 0.838968 |
| weighted_precision | 0.848269 |
| weighted_recall | 0.838968 |
| weighted_f1 | 0.842932 |
| macro_precision | 0.738108 |
| macro_recall | 0.762796 |
| macro_f1 | 0.749030 |

Modelo multiclase:

| metric | value |
|---|---:|
| accuracy | 0.771939 |
| weighted_precision | 0.775909 |
| weighted_recall | 0.771939 |
| weighted_f1 | 0.773450 |
| macro_precision | 0.547203 |
| macro_recall | 0.553491 |
| macro_f1 | 0.548677 |

Lectura: el binario es mas estable para senal general de ofensividad. El multiclase aporta una lectura analitica mas rica, pero la clase severa `odio_agresion_grupal` sigue siendo dificil por desbalance.

### Inferencia Spark ML sobre YouTube full

Salida ML:

```text
s3://figuretibucket/output/batch/predictions/run_sparkml_offendes_full/
```

Salida hibrida ML + reglas:

```text
s3://figuretibucket/output/batch/predictions/run_hybrid_sparkml_rules_full/
```

Total procesado:

```text
160,464 comentarios
```

Distribucion binaria Spark ML:

| pred_binary_sparkml_label | count | porcentaje |
|---|---:|---:|
| ofensivo | 85,426 | 53.2369% |
| no_ofensivo | 75,038 | 46.7631% |

Distribucion multiclase Spark ML:

| pred_multiclass_sparkml_label | count | porcentaje |
|---|---:|---:|
| ofensivo_directo | 73,656 | 45.9019% |
| neutral_no_ofensivo | 54,874 | 34.1971% |
| vulgaridad_contextual | 30,332 | 18.9027% |
| odio_agresion_grupal | 1,602 | 0.9984% |

Confianzas promedio:

| columna | avg | min | max |
|---|---:|---:|---:|
| confidence_binary_sparkml | 0.708065 | 0.500012 | 1.000000 |
| confidence_multiclass_sparkml | 0.575489 | 0.261257 | 1.000000 |

### Nota interpretativa

El modelo OffendES fue entrenado con datos externos en espanol, por lo que detecta ofensividad/odio/vulgaridad general, pero no entiende por si solo terruqueo ni polarizacion peruana. Por eso la salida recomendada para el proyecto es la hibrida: predicciones Spark ML + reglas locales multilabel.
