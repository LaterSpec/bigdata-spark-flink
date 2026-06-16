# YouTube 70000 Overview

Total procesado: **70000** comentarios de `youtube_lake.csv`.

Agregados por minuto generados con `video_offset_msec`.

## Distribucion pred_binary

- `no_ofensivo`: 59363 (84.8%)
- `ofensivo`: 10637 (15.2%)

## Distribucion pred_multiclass

- `neutral_no_ofensivo`: 57711 (82.4%)
- `ofensivo_directo`: 8531 (12.2%)
- `odio_agresion_grupal`: 2153 (3.1%)
- `vulgaridad_contextual`: 1605 (2.3%)

## Conteo de flags locales

- `has_terruqueo`: 2756 (3.9%)
- `has_fraude`: 3476 (5.0%)
- `has_electoral_institution`: 3088 (4.4%)
- `has_political_mention`: 22129 (31.6%)
- `has_polarization_signal`: 4007 (5.7%)
- `has_discriminatory_language`: 1029 (1.5%)
- `has_ethnic_racial_slur`: 962 (1.4%)
- `has_homophobic_slur`: 67 (0.1%)
- `has_general_insult`: 2289 (3.3%)
- `is_spam_noise`: 10038 (14.3%)

## Local risk score

- promedio: 0.56
- minimo: 0
- maximo: 8

## Top 20 terminos activados por categoria

- `terruqueo`: `rojos`=620, `comunista`=569, `rojo`=484, `terruco`=330, `terrucos`=275, `comunistas`=267, `sendero`=201, `sendero luminoso`=164, `movadef`=84, `mrta`=55, `caviares`=42, `terruqueo`=31, `terruquear`=20, `roja`=19, `terruca`=18, `senderista`=16, `senderistas`=6, `terruquean`=3, `comunachos`=1
- `fraude`: `fraude`=3312, `robo`=140, `actas impugnadas`=18, `irregularidades`=6, `actas falsas`=3, `fraudulenta`=2, `actas duplicadas`=1
- `electoral_institution`: `onpe`=1227, `votos`=825, `conteo`=507, `boca de urna`=232, `mesa`=189, `actas`=169, `jne`=163, `personeros`=119, `mesas`=53, `personero`=49, `cedula`=43, `flash electoral`=23
- `political_mention`: `keiko`=11119, `jp`=6707, `sanchez`=3287, `fujimori`=1099, `castillo`=780, `roberto sanchez`=732, `fujimorismo`=493, `antauro`=475, `humala`=222, `fuerza popular`=182, `porky`=122, `ppk`=113, `fp`=96, `dina`=95, `pedro castillo`=81, `apra`=81, `acuna`=80, `lapiz`=63, `boluarte`=62, `lopez aliaga`=62
- `polarization`: `zurdos`=596, `basura`=408, `odio`=334, `mafia`=291, `rata`=275, `terroristas`=215, `corrupta`=191, `corrupto`=190, `dictadura`=183, `corruptos`=173, `terrorista`=162, `ratas`=140, `delincuentes`=117, `zurdo`=112, `delincuente`=85, `lacra`=74, `izquierdistas`=68, `traidor`=56, `mafiosa`=56, `mafioso`=45
- `ethnic_racial_slur`: `cholos`=274, `paisana`=264, `serranos`=225, `cholo`=94, `llama`=35, `indios`=26, `indigena`=17, `mote`=13, `provinciano`=9, `llamas`=9, `indio`=8, `india`=5, `chola`=2, `serrana`=2, `paisano`=2
- `homophobic_slur`: `kbro`=28, `kbros`=16, `loca`=10, `cabro`=5, `cabros`=3, `rosquetes`=3, `cabron`=1, `rosquete`=1
- `general_insult`: `mierda`=694, `burro`=382, `ignorante`=265, `vaga`=215, `miserable`=200, `csm`=128, `bruto`=86, `ctm`=77, `payaso`=53, `porqueria`=48, `imbecil`=44, `idiota`=39, `vago`=35, `asqueroso`=32, `pendejo`=27, `pendejos`=23, `estupido`=14, `pendeja`=7, `asquerosa`=4, `pendejas`=1

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
- JP PA KBROS
- TE HACES LA LOCA
- PURO KBRO VOTA JP
- los kbros votando por la llorona
- solo los kbros votaron por keiko
- El Cori es kbro pero cae bien
- Que cabros oye cabros ustedes por votar por jp

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

- score=8 tags=`terruqueo|political_mention|polarization|homophobic_slur` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: puro webon y kbro vota por un aliado del movadef y de antauro
- score=6 tags=`terruqueo|fraude|political_mention|polarization` binary=`ofensivo` multiclass=`neutral_no_ofensivo`: KEIKO, Mano DURA contra Los ZuRdos de MI3@da. FRAUDE Comunista!
- score=6 tags=`political_mention|polarization|homophobic_slur` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: Que cabros oye cabros ustedes por votar por jp
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`terruqueo|fraude|political_mention|polarization` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: si hace fraude JP los militares lo sacan, y se va a poner feo para los terrucos, como sucedió en Chile
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`political_mention|polarization|homophobic_slur` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: puro kbro vota por JP
- score=6 tags=`terruqueo|political_mention|polarization|ethnic_racial_slur` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: lo peor es que hay mucho indígena diciendo terruco y socialista y no se dan cuenta que Keiko es la más zurda y comunista xD
- score=6 tags=`political_mention|polarization|homophobic_slur` binary=`ofensivo` multiclass=`ofensivo_directo`: LA LOCA PEDRO SALINAS.... LA ODIA A LA PORKY
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`political_mention|polarization|homophobic_slur` binary=`no_ofensivo` multiclass=`odio_agresion_grupal`: LOS QUE APOYAN JP SON KBROS
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: TREMENDO FRAUDE EN TODAS LAS ELECCIONES FINALISTA KEIKO Y UN ROJO CSM
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`political_mention|polarization|homophobic_slur` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP PA KBROS
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES
- score=6 tags=`terruqueo|fraude|political_mention|polarization|general_insult` binary=`no_ofensivo` multiclass=`neutral_no_ofensivo`: JP TERRUCO MISERABLE ADMIRADOR DEL NARCO Y PEDOFILO EVO QUE SE ROBO EN 15 AÑOS DE SOCIALISMO SUS RESERVAS INTERNACIONALES

## Comparacion breve contra muestra de 5000

- Comparacion contra `youtube_sample_predictions_with_rules_v2_5000.csv`:
- `has_terruqueo`: actual=3.9%, 5000=4.1%, diferencia=-0.2 pp (estable)
- `has_fraude`: actual=5.0%, 5000=4.7%, diferencia=+0.2 pp (estable)
- `has_electoral_institution`: actual=4.4%, 5000=4.5%, diferencia=-0.1 pp (estable)
- `has_political_mention`: actual=31.6%, 5000=30.6%, diferencia=+1.0 pp (estable)
- `has_polarization_signal`: actual=5.7%, 5000=5.5%, diferencia=+0.2 pp (estable)
- `has_discriminatory_language`: actual=1.5%, 5000=1.5%, diferencia=-0.1 pp (estable)
- `has_ethnic_racial_slur`: actual=1.4%, 5000=1.5%, diferencia=-0.1 pp (estable)
- `has_homophobic_slur`: actual=0.1%, 5000=0.1%, diferencia=+0.0 pp (estable)
- `has_general_insult`: actual=3.3%, 5000=4.2%, diferencia=-0.9 pp (estable)
- `is_spam_noise`: actual=14.3%, 5000=14.8%, diferencia=-0.5 pp (estable)
- No se observa una regla local disparada de forma excesiva fuera de lo esperable para esta muestra.

## Advertencia de contexto

Estas reglas son multilabel y no producen una etiqueta final unica. `onpe`, `jne` y `actas` activan instituciones electorales, pero no fraude por si solas. Terminos como `cholo`, `paisano`, `indio` o `serrano` activan una alerta etnico-racial porque pueden ser discriminatorios, aunque requieren revision contextual. La vulgaridad sola no debe interpretarse como odio, y polarizacion politica tampoco significa automaticamente discurso de odio.