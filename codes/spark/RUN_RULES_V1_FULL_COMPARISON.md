# Spark Rules v1 Full vs 70k

## Corridas comparadas

| corrida | registros |
|---|---:|
| run_rules_v1_70000 | 70,000 |
| run_rules_v1_full | 160,464 |

## Comparacion de flags

| flag | full % | 70k % | delta pp |
|---|---:|---:|---:|
| has_terruqueo | 4.0495 | 4.3557 | -0.3062 |
| has_fraude | 5.0292 | 5.0100 | +0.0192 |
| has_electoral_institution | 4.4621 | 4.0343 | +0.4278 |
| has_political_mention | 31.3603 | 31.7257 | -0.3654 |
| has_polarization_signal | 5.6997 | 5.7471 | -0.0474 |
| has_discriminatory_language | 1.3910 | 1.0414 | +0.3496 |
| has_ethnic_racial_slur | 1.2863 | 0.9329 | +0.3534 |
| has_homophobic_slur | 0.1047 | 0.1086 | -0.0039 |
| has_general_insult | 3.2069 | 3.3129 | -0.1060 |
| is_spam_noise | 3.2718 | 3.1443 | +0.1275 |

## Risk score

| corrida | avg | min | max |
|---|---:|---:|---:|
| run_rules_v1_70000 | 0.5718 | 0 | 8 |
| run_rules_v1_full | 0.5698 | 0 | 8 |

## Lectura

Las reglas se mantienen estables al pasar de 70,000 comentarios a todo el lake. Todas las diferencias estan por debajo de 0.43 puntos porcentuales, lo cual es una variacion baja para una muestra ampliada.

Las senales mas consistentes son fraude, polarizacion, homofobia, insulto general y spam. Las variaciones mas visibles aparecen en instituciones electorales y lenguaje etnico-racial, pero siguen siendo pequenas y no sugieren que alguna regla se haya disparado de forma anomala.

La limitacion aceptada de `is_spam_noise` en Spark Rules v1 se mantiene: no se modifico el detector de spam y no se usaron regex con backreferences para evitar errores Unicode en EMR.
