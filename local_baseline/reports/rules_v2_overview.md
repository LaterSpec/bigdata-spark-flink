# Rules V2 Overview

Muestra analizada: **5000** comentarios de `youtube_lake.csv`.

## Conteo de flags

- `has_terruqueo`: 206 (4.1%)
- `has_fraude`: 237 (4.7%)
- `has_electoral_institution`: 224 (4.5%)
- `has_political_mention`: 1531 (30.6%)
- `has_polarization_signal`: 276 (5.5%)
- `has_discriminatory_language`: 77 (1.5%)
- `has_ethnic_racial_slur`: 73 (1.5%)
- `has_homophobic_slur`: 4 (0.1%)
- `has_general_insult`: 209 (4.2%)
- `is_spam_noise`: 741 (14.8%)

## Top 20 terminos activados por categoria

- `terruqueo`: `comunista`=52, `rojos`=50, `rojo`=36, `terruco`=26, `terrucos`=19, `sendero`=18, `comunistas`=16, `sendero luminoso`=15, `movadef`=5, `caviares`=3, `terruca`=2, `mrta`=2, `terruquear`=2, `terruqueo`=1, `senderista`=1
- `fraude`: `fraude`=225, `robo`=11, `actas impugnadas`=1, `irregularidades`=1
- `electoral_institution`: `onpe`=92, `votos`=60, `conteo`=31, `boca de urna`=15, `actas`=14, `jne`=13, `mesa`=12, `personeros`=10, `personero`=3, `mesas`=2, `cedula`=2
- `political_mention`: `keiko`=766, `jp`=458, `sanchez`=227, `fujimori`=89, `castillo`=57, `roberto sanchez`=54, `antauro`=35, `fujimorismo`=33, `humala`=16, `dina`=11, `porky`=11, `fuerza popular`=11, `acuna`=8, `pedro castillo`=8, `ppk`=6, `fp`=6, `lopez aliaga`=4, `apra`=4, `lapiz`=4, `boluarte`=3
- `polarization`: `zurdos`=47, `basura`=31, `odio`=29, `corrupta`=22, `dictadura`=21, `rata`=21, `corruptos`=18, `terroristas`=16, `corrupto`=15, `mafia`=14, `terrorista`=13, `ratas`=13, `lacra`=11, `delincuente`=9, `delincuentes`=8, `zurdo`=6, `mafiosa`=5, `traidor`=4, `mafioso`=3, `criminal`=2
- `ethnic_racial_slur`: `paisana`=22, `cholos`=21, `cholo`=11, `serranos`=10, `llama`=4, `indigena`=2, `provinciano`=1, `chola`=1, `indios`=1, `serrana`=1
- `homophobic_slur`: `kbros`=2, `loca`=1, `kbro`=1
- `general_insult`: `mierda`=71, `burro`=32, `vaga`=27, `ignorante`=24, `miserable`=18, `csm`=8, `ctm`=8, `imbecil`=7, `vago`=6, `bruto`=6, `porqueria`=4, `pendejo`=3, `idiota`=3, `payaso`=2, `asqueroso`=2, `pendeja`=2, `pendejos`=1

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

## Advertencia de contexto

Estas reglas son multilabel y no producen una etiqueta final unica. `onpe`, `jne` y `actas` activan instituciones electorales, pero no fraude por si solas. Terminos como `cholo`, `paisano`, `indio` o `serrano` activan una alerta etnico-racial porque pueden ser discriminatorios, aunque requieren revision contextual. La vulgaridad sola no debe interpretarse como odio, y polarizacion politica tampoco significa automaticamente discurso de odio.