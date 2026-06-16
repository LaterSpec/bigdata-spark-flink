# Big Data Spark/Flink Project

Proyecto final Big Data para deteccion y analisis de discurso discriminatorio, ofensividad, terruqueo y polarizacion politica en comentarios de YouTube Live Chat electoral peruano.

## Estado Actual

- Spark Batch completado en AWS EMR sobre `160,464` comentarios.
- Data Lake y outputs principales almacenados en `s3://figuretibucket/`.
- Reglas locales multilabel implementadas.
- Spark ML con OffendES entrenado usando `pyspark.ml`.
- Salida hibrida Spark ML + reglas documentada.

## Documentacion Clave

- `docs/SPARK_BATCH_RECORDS.md`: record formal de la fase Spark Batch.
- `PRUEBAS_EXPERIMENTOS_BIGDATA.md`: bitacora acumulativa de pruebas y resultados.
- `README_AWS_SETUP.md`: setup AWS/S3/EMR.
- `README_SPARK_BATCH_PLAN.md`: plan de conversion a Spark Batch.
- `plan_pipeline_bigdata_discurso_politico.md`: plan general del proyecto.

## Nota Sobre Datos

Los archivos raw, outputs pesados, modelos generados y credenciales no se versionan en Git. La fuente de verdad para datos y resultados grandes es S3.
