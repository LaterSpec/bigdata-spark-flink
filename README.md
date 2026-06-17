# Big Data Spark/Flink Project

Proyecto final Big Data para deteccion y analisis de discurso discriminatorio, ofensividad, terruqueo y polarizacion politica en comentarios de YouTube Live Chat electoral peruano.

## Estado Actual

- Spark Batch completado en AWS EMR sobre `160,464` comentarios.
- Data Lake y outputs principales almacenados en `s3://figuretibucket/`.
- Reglas locales multilabel implementadas.
- Spark ML con OffendES entrenado usando `pyspark.ml`.
- Salida hibrida Spark ML + reglas documentada.
- Nueva arquitectura objetivo documentada para evolucionar a una capa streaming en AWS con Kafka como siguiente paso.

## Documentacion Clave

- `architecture.md`: arquitectura objetivo del proyecto y roadmap de evolucion en AWS.
- `docs/SPARK_BATCH_RECORDS.md`: record formal de la fase Spark Batch.
- `PRUEBAS_EXPERIMENTOS_BIGDATA.md`: bitacora acumulativa de pruebas y resultados.
- `README_AWS_SETUP.md`: setup AWS/S3/EMR.
- `README_SPARK_BATCH_PLAN.md`: record del plan que llevo a la fase Spark Batch ya completada.
- `plan_pipeline_bigdata_discurso_politico.md`: plan general del proyecto.

## Nota Sobre Datos

Los archivos raw, outputs pesados, modelos generados y credenciales no se versionan en Git. La fuente de verdad para datos y resultados grandes es S3.

## Siguiente direccion del proyecto

La fase batch ya esta resuelta. La siguiente expansion arquitectonica sera habilitar streaming sobre AWS sin mover la fuente de verdad fuera de S3:

- Kafka sera la capa central de eventos.
- Flink procesara streaming y alertas de baja latencia.
- Spark seguira resolviendo historico, entrenamiento e inferencia masiva.

En la siguiente etapa de trabajo debemos empezar por definir y luego configurar Kafka en AWS, pero esa implementacion aun no forma parte de este repo.
