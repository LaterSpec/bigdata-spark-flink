# Spark Batch Rules v1 - Execution Report

## Corridas ejecutadas

Entrada:

```text
s3://figuretibucket/data/raw/youtube/youtube_lake.csv
```

Script:

```text
s3://figuretibucket/codes/spark/spark_batch_rules_youtube.py
```

Corridas:

```text
run_rules_v1_5000
run_rules_v1_70000
```

Salidas 5,000:

```text
s3://figuretibucket/output/batch/predictions/run_rules_v1_5000/
s3://figuretibucket/output/batch/aggregates_by_minute/run_rules_v1_5000/
s3://figuretibucket/output/batch/reports/run_rules_v1_5000/
```

Salidas 70,000:

```text
s3://figuretibucket/output/batch/predictions/run_rules_v1_70000/
s3://figuretibucket/output/batch/aggregates_by_minute/run_rules_v1_70000/
s3://figuretibucket/output/batch/reports/run_rules_v1_70000/
```

## Resultados Spark 5,000

| flag | count | percentage |
|---|---:|---:|
| has_terruqueo | 96 | 1.9200% |
| has_fraude | 38 | 0.7600% |
| has_electoral_institution | 255 | 5.1000% |
| has_political_mention | 831 | 16.6200% |
| has_polarization_signal | 87 | 1.7400% |
| has_discriminatory_language | 10 | 0.2000% |
| has_ethnic_racial_slur | 10 | 0.2000% |
| has_homophobic_slur | 0 | 0.0000% |
| has_general_insult | 54 | 1.0800% |
| is_spam_noise | 131 | 2.6200% |

Risk score:

```text
avg=0.2396
min=0
max=4
```

## Resultados Spark 70,000

| flag | count | percentage |
|---|---:|---:|
| has_terruqueo | 3049 | 4.3557% |
| has_fraude | 3507 | 5.0100% |
| has_electoral_institution | 2824 | 4.0343% |
| has_political_mention | 22208 | 31.7257% |
| has_polarization_signal | 4023 | 5.7471% |
| has_discriminatory_language | 729 | 1.0414% |
| has_ethnic_racial_slur | 653 | 0.9329% |
| has_homophobic_slur | 76 | 0.1086% |
| has_general_insult | 2319 | 3.3129% |
| is_spam_noise | 2201 | 3.1443% |

Risk score:

```text
avg=0.5718
min=0
max=8
```

## Comparacion contra baseline local 70,000

| flag | Spark 70k | Local baseline 70k | delta pp |
|---|---:|---:|---:|
| has_terruqueo | 4.3557% | 3.9371% | +0.4186 |
| has_fraude | 5.0100% | 4.9657% | +0.0443 |
| has_electoral_institution | 4.0343% | 4.4114% | -0.3771 |
| has_political_mention | 31.7257% | 31.6129% | +0.1128 |
| has_polarization_signal | 5.7471% | 5.7243% | +0.0228 |
| has_discriminatory_language | 1.0414% | 1.4700% | -0.4286 |
| has_ethnic_racial_slur | 0.9329% | 1.3743% | -0.4414 |
| has_homophobic_slur | 0.1086% | 0.0957% | +0.0129 |
| has_general_insult | 3.3129% | 3.2700% | +0.0429 |
| is_spam_noise | 3.1443% | 14.3400% | -11.1957 |

## Lectura rapida

Las senales principales del proyecto se mantienen estables entre Spark y el baseline local: fraude, menciones politicas, polarizacion e insulto general tienen diferencias pequenas. Terruqueo sube moderadamente en Spark, probablemente por diferencias de normalizacion.

La mayor diferencia esta en `is_spam_noise`. En Spark se removieron backreferences de regex para evitar errores de Java Regex con emojis/caracteres Unicode durante ejecucion distribuida. Eso hace que el detector Spark sea mas conservador con spam que el modulo Python local. Es una decision segura para esta primera fase batch; luego se puede mejorar con reglas Spark sin backreferences o una UDF controlada.

## Nota tecnica

Esta fase no usa `joblib`, `sklearn` ni el modelo OffendES. Es Spark puro con reglas locales multilabel y agregados por minuto. La siguiente fase recomendada es portar el modelo a `pyspark.ml`.
