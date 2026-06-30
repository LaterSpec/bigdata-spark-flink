# Plan Spark Batch - Baseline Local a EMR

> **Registro histórico:** el runtime vigente usa batches disjuntos y concurrentes desde Kafka sobre `EMR_WORKERS`. Consulta `architecture.md`; los objetivos y resultados originales de esta fase se conservan.

Este documento define el plan que llevo el baseline local NLP + reglas peruanas a un flujo Spark Batch en EMR. Se conserva como record historico de una fase ya completada. No implementa ni ejecuta Kafka/Flink.

Para la arquitectura distribuida vigente, ver `architecture.md`.

## Objetivo del job Spark Batch

Leer comentarios desde S3, aplicar clasificacion y reglas locales, y escribir salidas batch listas para analisis y dashboard.

Entrada principal:

```text
s3://figuretibucket/data/raw/youtube/youtube_lake.csv
```

Entradas auxiliares:

```text
s3://figuretibucket/codes/local_baseline/artifacts/
s3://figuretibucket/codes/local_baseline/peruvian_rules.py
s3://figuretibucket/codes/local_baseline/utils.py
```

Salidas propuestas:

```text
s3://figuretibucket/output/batch/predictions/
s3://figuretibucket/output/batch/aggregates_by_minute/
s3://figuretibucket/output/batch/reports/
s3://figuretibucket/data/processed/youtube_classified/
```

## Enfoque recomendado

Para una primera version Spark Batch defendible, conviene separar el trabajo en dos capas:

1. Capa distribuida Spark:
   - leer `youtube_lake.csv`
   - normalizar columnas
   - aplicar reglas locales con UDFs o expresiones Spark
   - calcular agregados por minuto si existe `video_offset_msec`
   - escribir CSV/Parquet particionado en S3

2. Capa modelo ML:
   - opcion inicial simple: ejecutar inferencia con artefactos `joblib` usando Pandas UDF por particiones
   - opcion mas robusta para Spark: reentrenar o portar el baseline a `pyspark.ml` con `RegexTokenizer`, `HashingTF`/`IDF` y `LogisticRegression`

Recomendacion para el proyecto: empezar con reglas locales en Spark puro y luego portar el modelo a `pyspark.ml`. Los modelos `scikit-learn/joblib` sirven para baseline local y prueba de concepto, pero no son lo mas natural para ejecucion distribuida grande.

## Script Spark propuesto

Crear en una siguiente fase:

```text
codes/spark/spark_batch_classify_youtube.py
```

Responsabilidades:

```text
1. Leer CSV desde s3://figuretibucket/data/raw/youtube/youtube_lake.csv
2. Detectar columna textual message
3. Crear message_raw y message_clean
4. Crear text_norm para reglas
5. Aplicar flags locales:
   has_terruqueo
   has_fraude
   has_electoral_institution
   has_political_mention
   has_polarization_signal
   has_discriminatory_language
   has_ethnic_racial_slur
   has_homophobic_slur
   has_general_insult
   is_spam_noise
   local_risk_score
   local_rule_tags
6. Agregar predicciones del modelo si se usa estrategia joblib o pyspark.ml
7. Escribir predicciones en output/batch/predictions/
8. Si existe video_offset_msec, escribir agregados por minuto en output/batch/aggregates_by_minute/
9. Escribir resumen batch en output/batch/reports/
```

## Esquema de salida recomendado

Predicciones:

```text
message_raw
message_clean
pred_binary
pred_binary_label
confidence_binary
pred_multiclass
pred_multiclass_label
confidence_multiclass
has_terruqueo
has_fraude
has_electoral_institution
has_political_mention
has_polarization_signal
has_discriminatory_language
has_ethnic_racial_slur
has_homophobic_slur
has_general_insult
is_spam_noise
local_risk_score
local_rule_tags
video_offset_msec
minute
video_id
source_file
```

Agregados por minuto:

```text
source_file
video_id
minute
comentarios
ofensivos
terruqueo
fraude
polarizacion
spam
avg_local_risk_score
```

## Comando futuro para ejecutar en EMR

Cuando el script exista en S3:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://figuretibucket/codes/spark/spark_batch_classify_youtube.py \
  --input s3://figuretibucket/data/raw/youtube/youtube_lake.csv \
  --output-predictions s3://figuretibucket/output/batch/predictions/ \
  --output-aggregates s3://figuretibucket/output/batch/aggregates_by_minute/ \
  --output-reports s3://figuretibucket/output/batch/reports/
```

## Dependencias

Para reglas Spark puras:

```text
Solo PySpark incluido en EMR.
```

Para inferencia con `scikit-learn/joblib`:

```bash
pip3 install --user scikit-learn joblib pandas numpy
```

Advertencia: instalar paquetes en EMR master no siempre instala automaticamente en workers. Para produccion, usar bootstrap actions, `--py-files`, entorno empaquetado, o portar el modelo a `pyspark.ml`.

## Riesgos tecnicos

- `scikit-learn/joblib` dentro de Spark puede funcionar para muestras y batch mediano, pero requiere cuidar dependencias en workers.
- El modelo local fue entrenado en OffendES; detecta ofensividad/odio general en espanol, no entiende por si solo terruqueo ni polarizacion peruana.
- Las reglas locales son lexicas y pueden tener falsos positivos; deben conservarse como flags multilabel, no como sentencia unica.
- Terminos como `cholo`, `serrano`, `paisano` o `indio` requieren revision contextual.
- Polarizacion no equivale automaticamente a odio.
- Vulgaridad sola no debe tratarse como odio.

## Validaciones antes de ejecutar Spark

```bash
aws s3 ls s3://figuretibucket/data/raw/youtube/youtube_lake.csv
aws s3 ls s3://figuretibucket/codes/local_baseline/artifacts/
aws s3 ls s3://figuretibucket/output/batch/
spark-submit --version
```

## Plan de implementacion siguiente

1. Crear `codes/spark/spark_batch_classify_youtube.py` localmente.
2. Reutilizar diccionarios de `local_baseline/peruvian_rules.py` o convertirlos a regex Spark.
3. Probar con limite pequeno de filas desde S3.
4. Escribir outputs temporales bajo un prefijo versionado, por ejemplo `output/batch/predictions/run_YYYYMMDD_HHMM/`.
5. Validar conteos y porcentajes contra el baseline local de 70,000 comentarios.
6. Subir script a `s3://figuretibucket/codes/spark/`.
7. Ejecutar `spark-submit` en EMR.
8. Revisar outputs y costos; apagar cluster al terminar.
