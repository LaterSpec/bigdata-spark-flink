# Auditoria inicial de labels del dataset externo

- Archivo analizado: `dataset/train.parquet`
- Filas totales en train: **16710**
- Muestra exportada: `dataset/train_first_200.json` con **200** filas

## Relacion con el plan

Segun `plan_pipeline_bigdata_discurso_politico.md`, este dataset externo debe servir para entrenar el modelo base de NLP antes de aplicarlo al chat politico peruano. Por eso lo urgente no es solo leer los numeros, sino fijar un mapeo defendible entre `label` y categorias de negocio para el modelo.

## Distribucion global de labels

- `label 0`: 2051 filas, promedio 101.98 caracteres, mediana 62.0
- `label 1`: 212 filas, promedio 178.06 caracteres, mediana 132.0
- `label 2`: 13212 filas, promedio 190.85 caracteres, mediana 145.0
- `label 3`: 1235 filas, promedio 90.22 caracteres, mediana 59.0

## Distribucion de las primeras 200 filas

- `label 2`: 200 filas

## Hallazgo clave de la muestra inicial

Las primeras 200 filas del `train` caen completamente en `label = 2`. Al leer esos textos, predominan comentarios de apoyo, conversacion casual, sugerencias, humor y critica no claramente discriminatoria. Eso vuelve muy probable que `2` sea la clase base segura: `neutral` o `no_ofensivo`.

## Mapeo provisional recomendado

- `label 0` -> `ofensivo_directo: insulto o ataque claro contra una persona`
- `label 1` -> `odio_agresion_grupal: agresion intensa, deshumanizacion o ataque a colectivos/temas sensibles`
- `label 2` -> `neutral_no_ofensivo: comentario normal, apoyo, conversacion o critica no marcada como odio`
- `label 3` -> `vulgaridad_contextual: profanidad o tono brusco, pero muchas veces sin intencion clara de odio`

## Como lo usaria para el modelo

- Modelo binario inicial recomendado: `2 = no_ofensivo`, `0/1/3 = ofensivo_o_toxico`.
- Modelo multicategoria recomendado para analisis: `2 = neutral`, `3 = vulgar`, `0 = ofensivo_directo`, `1 = odio_o_agresion_grupal`.
- Para el caso peruano del plan, conviene agregar reglas externas para `terruqueo`, `fraude`, `polarizante` y `spam_ruido`, porque esas clases no aparecen de forma explicita aqui.

## Riesgo importante

Este mapeo es inferido por lectura manual del texto, no por una leyenda oficial del dataset. Se ve consistente, pero antes de entrenar la version final conviene validar al menos 30 a 50 ejemplos por label.

## Ejemplos por label

### Label 0

- `31983` [youtube]: Es la Veneno ☠️ barata mala que va a salir no recuerdo ni su nombre jaja
- `8003` [youtube]: Estupido aburrido
- `8093` [youtube]: Y le dices que mejore su contenido, pero el tuyo es un puto asco. Porque no te fijas más en lo tuyo que en lo demás , lo único q haces es tirar odio
- `59061` [instagram]: Un poco vulgar extravagante y baja clase
- `51699` [instagram]: Eres una pringada y también tienes una gran falta de empatia y mucha bajeza moral para criticar a una persona y un artista tan maravilloso como @pabloalboran. La lástima es que te conozcamos por ser su hater. Das pena.
- `50719` [instagram]: Esty paso de "Soy una pringada" a "soy una lechuga subnormal"
- `45650` [youtube]: Son la misma basura ambos
- `45058` [youtube]: Windy no esta buena lo que pasa es que está gorda Change my mind
- `11100` [youtube]: MALA PERSONA ERES MALA EE (celo tu no tu eres mi YouTube fav)
- `31201` [youtube]: hola so gorda 😄😄😄
- `10994` [youtube]: No habia visto tanta mierda junta
- `20360` [youtube]: Mierda,no aprendes verdad?

### Label 1

- `56710` [instagram]: si la mayoría de los policías fueran buenos, no habría represión contra el pueblo
- `20762` [youtube]: No puedo con esto, se me parte el corazón. Son unos putos desgraciados asquerosos. Han tratado a ese niño como un juguete, como una mascota que les regalas a los niños por navidad. Son unos desgraciados.  No puedo imaginarme el sufrimiento de ese pobre niñito, ese sentimiento de abandono y de incomprensión. Un niño de esa edad no entiende porque sus padres, que son unos putos desgraciados, lo abandonan.  No puedo,  no lo entiendo y estoy llorando.
- `51695` [instagram]: ENFERMOS Y PSICOPATAS es lo que son, no pueden dejarte tranquilo? Deberían haberte dado el perro como debían y dejar de meter mierda como siguen haciendo a día de hoy. Chicos enserio sois grandes, y pronto se hará JUSTICIA 😔
- `13578` [youtube]: Por lo menos podrían dejarle un bonito comentario a Windy? Tantos comentarios malos supongo le la pondrán triste... y más o menos para que ven sus vídeos si no les interesa! Nada más vienen a criticar puras estupideces por que no tienen más nada que hacer! Vayan a hacer algo productivo por sus miserables vidas! Que les hace falta mamaguevooos!!!
- `48915` [youtube]: Putos delinquentes
- `4327` [youtube]: Una falta de respeto a las que fueron verdaderas víctimas de violación o femicidio, las feministas también deberían de castigar a este tipo de personas, son una ESTÚPIDAS!!!
- `23288` [youtube]: Nadie se da cuenta que los picineros son un par de  pendejos pelotudos .
- `4385` [youtube]: Cuenta la leyenda que si eres relevante en internet las femimonguers empiezan comiéndote la pOllA y terminan comiéndote la olla para después huir y dejarte lidiando con las consecuencias y los herpes XDDD.
- `129` [twitter]: Pues no sé que decirte. Supongo q gente como esta no tendrán la suerte de saber qué es ser madre/padre. Con ser imbécil les vale.
- `30614` [youtube]: Que gente tan asquerosa y cobarde, está bien que hayas defendido a tu hermanita de esos acosadores, grande Dalas🤗🤗
- `6018` [youtube]: Jajajajaja. Las nuevas generaciones son cada vez más estupidas.
- `42488` [youtube]: Obviamente no les voy a desear nada malo a sus hijos que son niños indefensos pero habría que ver qué sienten y piensan esos corazones y mentes podridas si uno de sus hijos se enferma o sufre un accidente

### Label 2

- `52564` [instagram]: En vez de la magia de mi melena, la magia de mi nariz xD
- `32984` [youtube]: A ver, los milenials y la gente normal necesitamos un programa tipo Sálvame pero contigo comentando los salseos. Me das minutos de vida
- `58447` [instagram]: Me encanta todo el contenido que haces se nota que te curras mucho los videos y que te gusta lo que haces. Sigue asi que eres un crack👏👏 y gracias por todo😘😘😘💙💙💙🔥🔥🔥🔥
- `10341` [youtube]: a Laura sige así que vales mucho más que 10 o 20 o 30 estúpidos que te critiquen😍
- `53087` [instagram]: Y si no mes gusta Dalas, que hacen aquí,lárguense a fastidiar a otro lado,a qui solo estamos gente, que ha apoyado a dalas durante todo este juicio, y no por que una reportera perdon por la palabra pero una reportera pendeja diga "pero si hay varias denuncias es por algo" Dios mío, si hay tanto hate contra el, claro que harán hasta lo imposible por hundirlo, eso está claro, pero de nosotros depende para darle ánimos, somos sus fans no policías, no abojados, no jueces, NADA, como para andar criticando, así que mejor vallanse de aquí, aquí nadie los llamó.
- `57276` [instagram]: Ahora tienes más carita de bebe. Estas genial en ambas, cada una te da un aire diferente. Pero es genial que te sientas tan bien con tu nueva nariz y que todo haya salido bien❤️
- `34700` [youtube]: jsjsjsjjsjs que pedo con las personas que le dan dislikes a éste vídeo, es una maravilla xd
- `13381` [youtube]: Haz un te ries pierdes >:c quitándote los pelos de las cejas completas y del pecho con cera  :u like para que wismichu lo vea y lo haga xd
- `3527` [twitter]: Esa cuenta es real?? No puedo creer q en España haya gente tan mala.. Denuncia, pero no a Twitter, a la policía!! No hay porqué aguantar a estas “personas”...
- `55578` [instagram]: Dalas te apoyamos y se q vas a salir adelante lucha por ti y por lizy sois unas personas maravillosas os quiero mucho sois mis youtuber favoritos y siempre estaré de vuestro lado😜😘😘😍
- `44834` [youtube]: Dalas hiciste que perdiéramos el interés por el vídeo del debate con Juan de Dios al tardar te tanto en ponerlo ya se perdió la frescura de la noticia es como ya a quien le importa ya pasó mucho tiempo! Hubiera estado perfecto si la subes toda de una cuando ya la tenías no que la partiste en mil partes etc mal eso dalas
- `41414` [youtube]: Hahahahahaha el puto poema hahahah que bueno

### Label 3

- `32932` [youtube]: Sube más putos videos más seguidos joder
- `9611` [youtube]: Que video de mierda omo
- `46588` [youtube]: Dalas contéstale al caballito y dejala callada y que no se meta contigo yo te voy a defender pero deja a esta tonta callada
- `5623` [youtube]: Te descubrí hace dos días en plenos exámenes, no he estudiado una puta mierda gracias a tí, cuando me deprimo pensado en el futuro de mierda que me espera por no haber estudiado estos exámenes me pongo tus vídeos para relajarme, saludos
- `51296` [instagram]: Esta mujer es la puta ama @lizy_p_makeup comparte tremenda fotito con nosotras i love baby😘
- `41737` [youtube]: No me creo lo k vi xddddd el puto naruto con esos perlas
- `47357` [youtube]: ¿Qué piensas del Kpop pringada?
- `37410` [youtube]: La muñeca está de MIERDA dice hahahahahahha
- `22498` [youtube]: Oye andaba buscando una canción, que se llama palomitas y mi pregunta es: que le pasó coño si es un temazo
- `5292` [youtube]: Veo mucha positividad que mierda
- `48282` [youtube]: verga :D ya me aburrio tantas pinches polemicas
- `18874` [youtube]: ¿No te parece mal que Makiman se ponga a dar por culo a un cocodrilo (que probablemente esté sedado) para hacer su puto vídeo de mierda? A mí estas cosas no me hacen gracia y me las tomo más en serio porque me pongo en el lugar del animal. Yo siendo él me parecería flipante haber acabado, sin yo quererlo, en la piscina radioactiva del jodido Makiman y siendo pescado por una portería de fútbol, en lugar de estar tan tranquilamente en el agua de cualquier río, o yo que sé. No sé, a mí esto me da mucho asco aunque parezca que haga gracia.
