# YouTube 500 Overview

Total procesado: **500** comentarios de `youtube_lake.csv`.

Agregados por minuto generados con `video_offset_msec`.

## Distribucion pred_binary

- `no_ofensivo`: 419 (83.8%)
- `ofensivo`: 81 (16.2%)

## Distribucion pred_multiclass

- `neutral_no_ofensivo`: 404 (80.8%)
- `ofensivo_directo`: 63 (12.6%)
- `odio_agresion_grupal`: 20 (4.0%)
- `vulgaridad_contextual`: 13 (2.6%)

## Conteo de flags locales

- `has_terruqueo`: 22 (4.4%)
- `has_fraude`: 23 (4.6%)
- `has_electoral_institution`: 23 (4.6%)
- `has_political_mention`: 156 (31.2%)
- `has_polarization_signal`: 31 (6.2%)
- `has_discriminatory_language`: 14 (2.8%)
- `has_ethnic_racial_slur`: 13 (2.6%)
- `has_homophobic_slur`: 1 (0.2%)
- `has_general_insult`: 25 (5.0%)
- `is_spam_noise`: 79 (15.8%)

## Local risk score

- promedio: 0.59
- minimo: 0
- maximo: 6

## Top 20 terminos activados por categoria

- `terruqueo`: `comunista`=7, `rojos`=4, `terruco`=3, `rojo`=3, `sendero`=3, `sendero luminoso`=3, `comunistas`=1, `terrucos`=1, `terruca`=1
- `fraude`: `fraude`=23, `robo`=1
- `electoral_institution`: `onpe`=8, `votos`=6, `conteo`=4, `jne`=2, `mesas`=1, `mesa`=1, `personeros`=1, `boca de urna`=1, `personero`=1, `actas`=1
- `political_mention`: `keiko`=78, `jp`=41, `sanchez`=28, `fujimori`=11, `roberto sanchez`=8, `castillo`=6, `lopez aliaga`=2, `fujimorismo`=2, `porky`=2, `antauro`=2, `boluarte`=1, `dina`=1, `acuna`=1, `peru libre`=1, `fuerza popular`=1, `ppk`=1, `fp`=1, `humala`=1
- `polarization`: `dictadura`=5, `rata`=5, `zurdos`=4, `ratas`=3, `corruptos`=2, `odio`=2, `mafia`=2, `corrupto`=2, `corrupta`=2, `terrorista`=1, `delincuentes`=1, `traidor`=1, `lacra`=1, `mafiosa`=1, `terroristas`=1, `izquierdista`=1
- `ethnic_racial_slur`: `paisana`=7, `cholos`=3, `cholo`=1, `serranos`=1, `provinciano`=1, `llama`=1
- `homophobic_slur`: `kbros`=1
- `general_insult`: `mierda`=10, `burro`=3, `vaga`=3, `ctm`=2, `ignorante`=2, `vago`=2, `miserable`=2, `csm`=1

## Ejemplos de terruqueo

- Rojos de mierdaaaa
- ECLIPSE. CHINA NO SON COMUNISTAS OE!! XDDD MIRA LEE: UN PAIS COMUNISTA ES UN PAIS SIN CLASES! BUSCA IMAGENES DE CHINOS POBRES
- JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCO
- Fraude se voltio la tortilla un robo comunista
- TODOS QUEREMOS QUE PERU SE CONVIERTA EN BOLIVIA CUBA Y VENEZUELA Y QUE TENGAMOS GOBIERNO COMUNISTA SOCIALISTA, VIVA LA IZQUIERDA, SANCHEZ NUESTRO PRESIDENTE
- no mas terrucos viva bladimiro montesinos el. craken
- MI CHINITA YA GANO ROJOS LLORONES
- SI VEEN UN ROJO CON IPHONE AHHHHHH TRABAJA PARA EL ESTADO ES BURROOO X Q EN EL SECTOR PRIVADO SUS NEURONAS NO RINDEN

## Ejemplos de discriminacion etnico-racial

- LA PAISANA JACINTA ES NUESTRA VICEPRESIDENTA JAJAJAJAJAJAJAJAJAJAJAJAJAJAJJAJAJAJAJAJAJAJA SUFRE PERUANO SUFREEEEEEEE JAJAJAJAJAJAJJAJAA ASOSTADOS LOS VEO PEEEE JAJAJAJJAJAJAJJA VIVA PERUZUELAA!! JAJA
- ... ¡¡ VAKANCIAS 2026 !!!:tangerine:................ ¡¡ JODER AL CHÓLO !!!.... :tangerine:........ ¡¡ NO DEJARLO GOBERNAR !!:tangerine:🇺🇸:skull::statue_of_liberty:
- LA PAISANA JACINTA ES NUESTRA VICEPRESIDENTA JAJAJAJAJAJAJAJAJAJAJAJAJAJAJJAJAJAJAJAJAJAJA SUFRE PERUANO SUFREEEEEEEE JAJAJAJAJAJAJJAJAA ASOSTADOS LOS VEO PEEEE JAJAJAJJAJAJAJJA VIVA PERUZUELAA!! JAJA
- :face-blue-smiling::face-blue-smiling:LOS CHOLOS QUE SE LLENAN DE HIJOS, NO TRABAJAN ,NO ESTUDIAN , NO SABEN NI HABLAR QUIEREN ARREGLAR A UN PAIS, CUANDO NI CON SU MISERABLE VIDA PUEDEN:face-blue-smiling::face-purple-crying::face-purple-crying:
- LA PAISANA JACINTA ES NUESTRA VICEPRESIDENTA JAJAJAJAJAJAJAJAJAJAJAJAJAJAJJAJAJAJAJAJAJAJA SUFRE PERUANO SUFREEEEEEEE JAJAJAJAJAJAJAJAJAJJAJAA ASOSTADOS LOS VEO PEEEE JAJAJAJAJAJAJAJAJJA
- :face-blue-smiling::face-blue-smiling:LOS CHOLOS QUE SE LLENAN DE HIJOS, NO TRABAJAN ,NO ESTUDIAN , NO SABEN NI HABALR QUIEREN ARREGLAR A UN PAIS, CUANDO NI CON SU MISERABLE VIDA PUEDEN:face-blue-smiling::face-purple-crying::face-purple-crying:
- KEYKO NOS PROMETIO QUE LA PAISANA JACINTA COMO MINISTRA DE EDUCACION!:rolling_on_the_floor_laughing::rolling_on_the_floor_laughing::rolling_on_the_floor_laughing:
- los fans de keyko dicen cholos serranos provincianos como si ellos fueran rubios de ojos azules es lo.peor renegar de sus orígenes:face-blue-smiling::face-blue-smiling:

## Ejemplos de fraude

- :face-green-smiling::face-green-smiling::face-green-smiling::face-green-smiling::face-green-smiling::face-green-smiling::face-green-smiling:FRAUDE ELECTORAL:face-green-smiling::face-green-smiling:NO AMPLIARON HORARIO DE VOTACION PARA TRABAJADORES:face-red-droopy-eyes::face-red-droopy-eyes::face-red-droopy-eyes::face-red-droopy-eyes:
- ES UNA FRAUDE
- Fraude se voltio la tortilla un robo comunista
- ASI DICEN ES FRAUDE DE AHI LE SUBIRAN VOTOS A JP COMO HICIERON EN LA PRIMERA VUELTA
- FRAUDE
- SE LOS DIJE YA COMENZARON CON SU FRAUDE ESTOS DELINCUENTES DE LA ONPE Y JNE JUNTO CON ENCUESTADORAS ESTAFADORAS Y MEDIOS COMUNICACIÓN CORRUPTOS MUCHACHOS EL FRAUDE YA EMPEZARON :woozy_face:🇵🇪
- estoy seguro q si se recontaran nuevamente sanchez ganaría x mucho lastima q el fraude esta dentro y fuera del país xq así eeuu lo quiere xq tiene q saquear al perú.
- FRAUDE JP

## Ejemplos de polarizacion

- JP es la voz, NO A LA DICTADURA de los fujirratas
- VAMOS ROBERTO SANCHEZ, CANTALO DE UNA VEZ PRENSA DE MIERDA COMPRADA!!🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪
- JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCO
- jp pa kbros
- HAY GENTE TERRORISTA EN EL PARTIDO DE SANCHEZZZ!!!:face-red-droopy-eyes::face-red-droopy-eyes::face-red-droopy-eyes:
- ASI DICEN ES FRAUDE DE AHI LE SUBIRAN VOTOS A JP COMO HICIERON EN LA PRIMERA VUELTA
- puro ignorante escribiendo , realmente no saben quien es sanchez
- Vamos jp vamos keiko pero vamos a la mierda

## Ejemplos de lenguaje homofobico

- jp pa kbros

## Ejemplos de insulto general

- ya csm harto dices denunciable denunciable, acercate a tu comisaria mas cercana y gritales FRAOOODEEEE
- calla ctm
- Rojos de mierdaaaa
- VAMOS ROBERTO SANCHEZ, CANTALO DE UNA VEZ PRENSA DE MIERDA COMPRADA!!🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪
- puro ignorante escribiendo , realmente no saben quien es sanchez
- Vamos jp vamos keiko pero vamos a la mierda
- Puro VAGO VIVIENDO EN EL CERRO LURIGANCHO VOTANDO POR ROBERTO SÁNCHEZ
- gente de mierda

## Top comentarios con mayor local_risk_score

- score=6 tags=`political_mention|polarization|homophobic_slur` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: jp pa kbros
- score=6 tags=`terruqueo|fraude|political_mention|polarization` binary=`ofensivo` multiclass=`neutral_no_ofensivo`: KEIKO, Mano DURA contra Los ZuRdos de MI3@da. FRAUDE Comunista!
- score=4 tags=`fraude|electoral_institution|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: ASI DICEN ES FRAUDE DE AHI LE SUBIRAN VOTOS A JP COMO HICIERON EN LA PRIMERA VUELTA
- score=4 tags=`terruqueo|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCOJP TERRUCO JP TERRUCO JP TERRUCO
- score=4 tags=`terruqueo|fraude` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: Fraude se voltio la tortilla un robo comunista
- score=4 tags=`terruqueo|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: TODOS QUEREMOS QUE PERU SE CONVIERTA EN BOLIVIA CUBA Y VENEZUELA Y QUE TENGAMOS GOBIERNO COMUNISTA SOCIALISTA, VIVA LA IZQUIERDA, SANCHEZ NUESTRO PRESIDENTE
- score=4 tags=`terruqueo|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: ALBERTO FUJIMORI TERRUCO
- score=4 tags=`terruqueo|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: DE NUEVO FUJIMORI DERROTA A SENDERO LUMINOSO
- score=4 tags=`fraude|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: LOPEZ ALIAGA YA LO PREDIJO: FRAUDE, FRAUDE Y EN LA MADRUGADA QUE VIENE CONFIRMARÁ QUE JP GANÓ. FRAUDE, FRAUDE!!!!!
- score=4 tags=`fraude|political_mention|polarization` binary=`ofensivo` multiclass=`ofensivo_directo`: SÁNCHEZ LLEGÓ A ESTA 2da VUELTA CON FRAUDE, VERGÜENZA NACIONAL
- score=4 tags=`terruqueo|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: VAMOS PUEBLO SOMOS KEIKO:orange_circle::orange_heart::orange_heart::orange_heart: POR UN PAIS PROSPERO Y SEGURO SOMO KEIKO PUEBLO DILE NO AL COMUNISMO DE JP SÁNCHEZ EL TERRUCO :grinning_face_with_big_eyes::thumbs_up:🇵🇪
- score=4 tags=`terruqueo|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: TODOS QUEREMOS QUE PERU SE CONVIERTA EN BOLIVIA CUBA Y VENEZUELA Y QUE TENGAMOS GOBIERNO COMUNISTA SOCIALISTA, VIVA LA IZQUIERDA, SANCHEZ NUESTRO PRESIDENTE
- score=4 tags=`fraude|electoral_institution|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: NO AL FRAUDE EN LA PAGINA DE LA ONPE ESTÁ GANANDO AMPLIAMENTE FUERZA POPULAR.... NO AL FRAUDE :warning: OJO .S.O.S. CUIDADO QUE PASEN COSAS RARAS EN LA MADRUGADA OJO, OJOO SEÑORES CUIDAO AHÍ OJO
- score=4 tags=`terruqueo|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: falta extranjero y allá es todo Keiko ,ya fueron rojos :rolling_on_the_floor_laughing::rolling_on_the_floor_laughing::rolling_on_the_floor_laughing::rolling_on_the_floor_laughing::rolling_on_the_floor_laughing::rolling_on_the_floor_laughing:
- score=4 tags=`fraude|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: FRAUDE JP
- score=4 tags=`fraude|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: estoy seguro q si se recontaran nuevamente sanchez ganaría x mucho lastima q el fraude esta dentro y fuera del país xq así eeuu lo quiere xq tiene q saquear al perú.
- score=2 tags=`terruqueo` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: ECLIPSE. CHINA NO SON COMUNISTAS OE!! XDDD MIRA LEE: UN PAIS COMUNISTA ES UN PAIS SIN CLASES! BUSCA IMAGENES DE CHINOS POBRES
- score=2 tags=`terruqueo` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: si soy terruca
- score=2 tags=`political_mention|polarization|general_insult` binary=`ofensivo` multiclass=`ofensivo_directo`: VAMOS ROBERTO SANCHEZ, CANTALO DE UNA VEZ PRENSA DE MIERDA COMPRADA!!🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪🇵🇪
- score=2 tags=`political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: HAY GENTE TERRORISTA EN EL PARTIDO DE SANCHEZZZ!!!:face-red-droopy-eyes::face-red-droopy-eyes::face-red-droopy-eyes:

## Comparacion breve contra muestra de 5000

- Comparacion contra `youtube_sample_predictions_with_rules_v2_5000.csv`:
- `has_terruqueo`: actual=4.4%, 5000=4.1%, diferencia=+0.3 pp (estable)
- `has_fraude`: actual=4.6%, 5000=4.7%, diferencia=-0.1 pp (estable)
- `has_electoral_institution`: actual=4.6%, 5000=4.5%, diferencia=+0.1 pp (estable)
- `has_political_mention`: actual=31.2%, 5000=30.6%, diferencia=+0.6 pp (estable)
- `has_polarization_signal`: actual=6.2%, 5000=5.5%, diferencia=+0.7 pp (estable)
- `has_discriminatory_language`: actual=2.8%, 5000=1.5%, diferencia=+1.3 pp (estable)
- `has_ethnic_racial_slur`: actual=2.6%, 5000=1.5%, diferencia=+1.1 pp (estable)
- `has_homophobic_slur`: actual=0.2%, 5000=0.1%, diferencia=+0.1 pp (estable)
- `has_general_insult`: actual=5.0%, 5000=4.2%, diferencia=+0.8 pp (estable)
- `is_spam_noise`: actual=15.8%, 5000=14.8%, diferencia=+1.0 pp (estable)
- No se observa una regla local disparada de forma excesiva fuera de lo esperable para esta muestra.

## Advertencia de contexto

Estas reglas son multilabel y no producen una etiqueta final unica. `onpe`, `jne` y `actas` activan instituciones electorales, pero no fraude por si solas. Terminos como `cholo`, `paisano`, `indio` o `serrano` activan una alerta etnico-racial porque pueden ser discriminatorios, aunque requieren revision contextual. La vulgaridad sola no debe interpretarse como odio, y polarizacion politica tampoco significa automaticamente discurso de odio.