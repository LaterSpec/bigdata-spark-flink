# Analisis refinado con 20 ejemplos por label

Se extrajeron **20 ejemplos por cada label** desde `dataset/train.parquet` para revisar el significado de cada numero con una muestra balanceada.

Archivo de apoyo:

- `dataset/label_stratified_20_each.json`

## Conclusion refinada

- `label 2` es el mas claro: corresponde a `neutral_no_ofensivo`.
- `label 0` parece `ofensivo_directo`: insulto, ataque personal, burla corporal o descalificacion fuerte hacia una persona.
- `label 3` parece `vulgaridad_contextual`: uso de groserias, tono brusco o expresiones vulgares que muchas veces no buscan odiar ni atacar directamente.
- `label 1` parece la clase mas severa o mas socialmente sensible: `odio_agresion_grupal`, con mensajes de hostilidad intensa, ataques a colectivos o deseo de dano/violencia mas marcado.

## Lectura por label

### Label 0

Patron dominante:

- insulto directo a una persona
- humillacion o body shaming
- lenguaje claramente despectivo

Ejemplos representativos:

- "Estupido aburrido"
- "soy una lechuga subnormal"
- "Windy tus videos son un asco"
- "parece que esta muerta"
- "hola so gorda"

Interpretacion recomendada:

- `ofensivo_directo`

### Label 1

Patron dominante:

- agresion mas fuerte o mas ideologica
- ataques a grupos o colectivos
- formulaciones mas cercanas a odio, desprecio social o violencia verbal intensa

Ejemplos representativos:

- "las feministas tambien deberian de castigar..."
- "los viejos ya estan tardando en morir"
- "muerte a los gordos traicioneros"
- "que les follen a los demas"
- "hay una gran diferencia entre ficcion y apologia al odio y la violencia"

Interpretacion recomendada:

- `odio_agresion_grupal`

Nota:

No todos los ejemplos son odio puro en sentido legal o academico estricto, pero si parece la clase mas cercana a agresion severa, colectivos o dano social.

### Label 2

Patron dominante:

- apoyo
- conversacion normal
- sugerencias
- critica comun sin carga fuerte
- humor casual

Ejemplos representativos:

- "Me encanta todo el contenido que haces"
- "Dalas te apoyamos"
- "me gustó mucho el vídeo"
- "No puedo creer q en España haya gente tan mala.. Denuncia"

Interpretacion recomendada:

- `neutral_no_ofensivo`

Nota:

Hay textos con palabras fuertes dentro de un contexto defensivo o coloquial, pero no parecen etiquetados como ofensivos.

### Label 3

Patron dominante:

- groseria casual
- vulgaridad coloquial
- insulto suave o muletilla
- tono agresivo pero no necesariamente odio directo

Ejemplos representativos:

- "Sube más putos videos"
- "Que video de mierda"
- "Esta mujer es la puta ama"
- "Wismichu sois el puto amo"
- "me has alegrado en puto día"

Interpretacion recomendada:

- `vulgaridad_contextual`

## Mapeo recomendado para ML

### Opcion binaria

- `2` -> `no_ofensivo`
- `0, 1, 3` -> `ofensivo_o_toxico`

### Opcion multiclase

- `0` -> `ofensivo_directo`
- `1` -> `odio_agresion_grupal`
- `2` -> `neutral_no_ofensivo`
- `3` -> `vulgaridad_contextual`

## Recomendacion para el proyecto

Para el plan del proyecto, este dataset sirve bien como base para entrenar un clasificador inicial de ofensividad/toxicidad en espanol. Luego, sobre el chat politico peruano, conviene agregar reglas separadas para:

- `terruqueo`
- `fraude`
- `polarizante`
- `spam_ruido`

Porque esas categorias de negocio no aparecen explicitamente en estos labels numericos.
