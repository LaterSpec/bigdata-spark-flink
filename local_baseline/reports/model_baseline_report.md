# Model Baseline Report

## Resumen

Se entreno un baseline local con OffendES para cubrir la parte de NLP supervisado del proyecto antes de integrarlo a Spark batch y Flink streaming. El objetivo de este baseline es validar una primera capa de clasificacion de ofensividad y agresion general en espanol usando los splits locales de `dataset/train.parquet`, `dataset/validation.parquet` y `dataset/test.parquet`.

## Por que usamos OffendES

OffendES aporta textos en espanol ya etiquetados para ofensividad, agresion y lenguaje problemático. Eso cubre una necesidad central del proyecto: el chat electoral peruano de `youtube_lake.csv` es valioso como fuente real, pero no viene anotado para entrenamiento supervisado. OffendES permite construir un modelo base defendible para el documento final, alineado con la parte de tecnicas NLP y clasificacion inicial de lenguaje ofensivo/odio/agresion general.

## Que parte del objetivo del documento cubre

Este baseline cubre la parte de entrenamiento y evaluacion de un modelo NLP base para deteccion automatica de:

- ofensividad general
- agresion verbal
- odio o agresion severa
- vulgaridad contextual

Eso se conecta directamente con el bloque de Spark ML Training y con la futura inferencia batch sobre comentarios peruanos. Tambien deja listo un artefacto inicial que puede convertirse despues en una etapa de inferencia para Flink.

## Limites del baseline

OffendES no cubre por si solo el contexto politico peruano. No reconoce de manera especializada categorias como:

- terruqueo
- narrativas de fraude electoral
- polarizacion entre candidatos o partidos peruanos
- spam/ruido caracteristico del live chat

Por eso se agrego una capa de reglas locales peruanas. El modelo aporta ofensividad general en espanol y las reglas complementan el contexto electoral peruano.

## Distribucion de labels

- Train: [{"label": 0, "len": 2051}, {"label": 1, "len": 212}, {"label": 2, "len": 13212}, {"label": 3, "len": 1235}]
- Validation: [{"label": 0, "len": 22}, {"label": 1, "len": 4}, {"label": 2, "len": 64}, {"label": 3, "len": 10}]
- Test: [{"label": 0, "len": 2340}, {"label": 1, "len": 211}, {"label": 2, "len": 9651}, {"label": 3, "len": 1404}]

La clase `1 = odio_agresion_grupal` esta claramente desbalanceada. Por eso la metrica principal de comparacion es `F1 macro`, que penaliza mejor el mal rendimiento en clases minoritarias.

## Comparacion de modelos

### Modelo binario

- Definicion: `0/1 = ofensivo`, `2/3 = no_ofensivo`
- Accuracy: 0.8780
- Precision macro: 0.7997
- Recall macro: 0.8001
- F1 macro: 0.7999

### Modelo multiclase

- Clases: `ofensivo_directo`, `odio_agresion_grupal`, `neutral_no_ofensivo`, `vulgaridad_contextual`
- Accuracy: 0.8503
- Precision macro: 0.6779
- Recall macro: 0.6533
- F1 macro: 0.6637

## Recomendacion final

El modelo binario debe ser la capa principal inicial del pipeline para detectar ofensividad general, mientras que el multiclase debe usarse como señal analitica complementaria para distinguir insulto directo, odio/agresion severa, neutralidad y vulgaridad contextual.

En terminos de pipeline:

- usar el binario como detector robusto y simple de ofensividad general
- usar el multiclase como una vista mas rica para analisis y dashboard
- usar reglas locales peruanas para `terruqueo`, `fraude`, `polarizacion politica` y `spam_ruido`

## Siguiente paso hacia la arquitectura Big Data

- Spark batch: usar este baseline para clasificar historicamente el `youtube_lake.csv`
- Flink streaming: aplicar el modelo como capa de scoring y combinarlo con reglas locales en tiempo real
- Dashboard: mostrar porcentaje ofensivo, clases detectadas y flags politicos por ventana
