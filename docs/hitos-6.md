# Hito 6 — Integración de metadatos y meteorología

**Estado:** verificación técnica sintética integrada D27 superada; ratificación
académica y validación real pendientes. **Responsabilidad:** Irantzu valida significados y
uniones; Carlos implementa y verifica la integración. **Objetivos:** OE3,
OE4. **Requisito previo:** Silver eléctrica aceptada en H05.

## Finalidad

Enriquecer la electricidad seleccionada con metadatos fiables y, **solo si
la correspondencia es justificable**, observaciones meteorológicas. Las
mediciones sin información contextual válida deben conservarse o tratarse
según reglas explícitas y quedar contabilizadas.

## Actividades incrementales

`diagnose_synthetic_metadata(spark, config, silver_publication_id, workspace)`,
en `tfm_lakehouse.silver.metadata_diagnostics`, verifica la Silver explícita y
lee la versión de metadatos del manifiesto Bronze asociado. Comprueba versión,
metadatos del commit, esquema, recuento y linaje por fila; no selecciona la
última publicación Bronze ni une datos eléctricos. El contrato histórico Bronze
no registra un UUID físico de tabla para compararlo con uno anterior.

El resultado distingue columnas ausentes, valores nulos y vacíos, edificios
aprobados sin metadatos y claves duplicadas idénticas o conflictivas. Los
duplicados cuentan claves y se separan entre edificios aprobados y otros; las
claves nulas o vacías se cuentan aparte y no intervienen en los recuentos por
campo. La igualdad de duplicados considera todas las columnas, incluido el
linaje. La cardinalidad esperada es como máximo una fila de metadatos por
edificio; el diagnóstico no resuelve duplicados ni decide una unión.
Las huellas detectan cambios durante las acciones, pero no autentican el
almacenamiento ni garantizan protección frente a modificaciones externas
coordinadas. Una nueva versión Bronze concurrente también detiene el diagnóstico
de forma conservadora. Las reglas sintéticas de interpretación y tratamiento
de conflictos están aprobadas; su ratificación académica conjunta sigue pendiente.

La selección sintética de este diagnóstico y las regresiones de vínculo y
publicación Silver superaron 43 pruebas. La distinción entre cadena vacía y
`null` se comprobó con un DataFrame modificado en prueba; Bronze representa
como `null` los campos CSV sin valor según su contrato.

### Vista sintética de metadatos

El autor ha aprobado para sintéticos H06-D01: bloquear duplicados idénticos
de metadatos de los edificios seleccionados; D02: bloquear los conflictivos;
D03: diagnosticar
duplicados ajenos sin bloquear; D04: conservar mediciones sin metadatos y
expresar ausencias; D05: conservar originales e interpretar área solo si es
finita, positiva y exactamente representable; D06: vista derivada sin
persistencia vinculada a Bronze y Silver; D07: área `Decimal(38,18)` sin
redondeo, con estado para positivos no representables; y D08: consulta IQR
independiente, sin recalcular IQR al enriquecer. La ratificación académica
conjunta sigue pendiente.

`enrich_synthetic_metadata(spark, config, silver_publication_id, workspace)`
devuelve diagnóstico y DataFrame derivado. La superficie conserva el original y
clasifica `valid`, `null`, `empty`, `nonfinite`, `invalid`, `zero`, `negative`,
`positive_nonrepresentable`, `missing_row` o `missing_column`. La zona declarada
se valida con el catálogo disponible, sin interpretar instantes ni DST. Los
literales `NaN`, `Infinity`, `+Infinity` y `-Infinity` se indican como no
finitos; el resto sigue la gramática numérica exacta de H05. Los
campos de texto distinguen fila o columna ausente, nulo, vacío y presente.
El informe `building_quality` agrupa disponibilidad y estados por edificio.
Se verifica el multiconjunto completo eléctrico, la identidad Silver, el
manifiesto y la huella de metadatos. El DataFrame es diferido: sus archivos
fuente deben permanecer disponibles y sin cambios después de la devolución.
La regresión sintética de publicación y vínculo superó 110 pruebas; una
selección posterior de integridad, duplicados, colisiones y versiones históricas
superó siete. Se probó además la independencia ante una publicación IQR no
aceptada y otra aceptada, sin leer ni recalcular IQR durante el enriquecimiento.
La aceptación estadística IQR no es requisito de esta vista.

### Identidad contextual sintética

El autor ha aprobado D15: diferir la integración meteorológica; D16: una
publicación Delta contextual independiente; D17: vincularla a Silver, Bronze,
metadatos y método exactos; D18: versionar la interpretación y acreditar sus
recursos horarios; D19: reserva, commit y finalización con recuperación explícita
y sin reintentos automáticos; D20: selección por ID explícito; D21: mantener IQR
vinculado a la Silver eléctrica original; y D22: exigir fuentes retenidas.
Su ratificación académica conjunta está pendiente.

D23 aprueba la interpretación horaria controlada para la intención contextual:
el recurso resuelto con la precedencia de `zoneinfo` se lee una vez y sus mismos
bytes se usan para SHA-256 y `ZoneInfo.from_file`. Se rechazan evidencias
distintas entre trabajadores participantes, diferencias de esquema o del
multiconjunto completo frente a la vista H06. No se interpreta ni convierte
ningún instante eléctrico. `build_synthetic_contextual_intent` y
`verify_synthetic_contextual_intent`, en `tfm_lakehouse.silver.contextual_identity`,
generan o contrastan la intención sin escribir tablas, reservas ni marcadores.
La evidencia solo cubre trabajadores participantes y no acredita la retención
futura; Bronze no registra UUID físico histórico de metadatos.
La selección sintética de identidad contextual y regresiones de vínculo y
publicación Silver superó 45 pruebas. Una comprobación posterior de identidad
Silver distinta, recurso horario modificado y fuente temporalmente ausente
superó una prueba adicional. El cambio de recurso en replay se inyectó sobre
un archivo sintético; la resolución productiva se comprueba por separado.

### Publicación contextual sintética

`publish_synthetic_contextual(spark, config, silver_publication_id,
publication_id, workspace)`, en `tfm_lakehouse.silver.contextual_publication`,
implementa una publicación independiente bajo
`contextual-publications/<publication_id>/`: `intent.json`, `table/` y
`completed.json`. El bloqueo local cooperativo abarca la reserva y la
verificación. La intención versionada vincula la identidad eléctrica, el
manifiesto Bronze, la versión de metadatos, el método de enriquecimiento, la
evidencia horaria, el esquema y la huella del multiconjunto. No incorpora
observaciones meteorológicas ni recalcula IQR.
La validación del espacio puede inspeccionar estructuralmente el árbol IQR;
no consulta sus resultados para construir el contexto.

D24 aprueba la intención contextual versión 2, que añade la huella SHA-256
del árbol físico de metadatos a partir de rutas relativas y contenido. La
huella se contrasta durante el consumo y las revalidaciones, también en la
recuperación; acredita coherencia con la evidencia registrada, no autenticidad
externa ni protección frente a cambios externos coordinados y revertidos entre
comprobaciones. Las reservas y publicaciones anteriores sin esta evidencia se rechazan
sin modificarlas ni migrarlas. El marcador de finalización conserva su propia
versión de contrato 1, distinta de la versión 2 de la intención. El lector
histórico exige la misma evidencia física. La ratificación
académica conjunta sigue pendiente. Las comparaciones de intención, marcador y
esquema respetan también los tipos JSON, sin equiparar booleanos y números.

Las filas y la evidencia horaria se capturan en una misma acción y se contrastan
con la intención; las filas se conservan en memoria del proceso conductor para
evitar una nueva evaluación de las fuentes al escribir. Por ello la API se
limita a fixtures sintéticos pequeños: no impone un límite propio de filas o
bytes y la configuración de Spark no garantiza memoria suficiente en Python.
Un fallo de recogida ocurre antes de crear una nueva reserva, aunque el directorio
y el archivo de bloqueo pueden haberse creado. No se ha probado el agotamiento de
memoria del proceso. Se verifican esquema y multiconjunto de
la tabla, identidad del commit y fuentes retenidas. Delta normaliza a nullable
los campos superiores; el esquema original queda en la intención.

La reinvocación explícita continúa una reserva válida sin tabla, reconcilia un
commit verificado sin marcador o devuelve la publicación idéntica sin otro
commit. Los conflictos y evidencias incompletas bloquean la operación; no se
eliminan ni se sobrescriben. Commit y marcador no forman una transacción atómica.
En aquella fase faltaba la validación sintética integral y no existía lector
histórico independiente. No hay selección automática de publicaciones.
Una selección de publicación y recuperación superó cinco pruebas, tres de ellas
con Spark y Delta; otra selección de controles unitarios de identidad e IQR
superó 45. Estas cifras no cubren todos los fallos y mutaciones exigidos para
aceptar la publicación contextual.
Una repetición que añadía la regresión de intención determinista terminó con
cuatro pruebas superadas y dos resultados inconclusos durante la preparación
Bronze, tras interrupciones manuales de Spark; la regresión añadida sí pasó.
La repetición limpia de los dos casos interrumpidos superó ambas pruebas.
Una prueba con dos publicadores cooperativos verificó la exclusión y un único
commit. Otra detectó una limitación bloqueante de recuperación: tras reservar,
una modificación del registro Delta de metadatos con suma de comprobación
actualizada no impidió publicar, cuando la versión y el contenido contextual
seguían siendo iguales. La intención versión 1 no conservaba la huella física del
árbol de metadatos entre ejecuciones. No se ha demostrado alteración de filas;
la corrección aprobada en D24 incorpora esa huella en la intención.

La verificación D24 superó 27 casos sintéticos distintos con Spark/Delta y 47
pruebas unitarias. Cubrió versiones anteriores, las dos mutaciones que habían
reproducido el defecto, cambios durante la publicación, recuperación, esquema,
multiconjuntos, fuentes, zonas horarias, concurrencia e independencia de los
resultados IQR. Dos casos no llegaron al punto contextual por un fallo del
proceso auxiliar de la JVM y pasaron al repetirlos en una sesión limpia. Otro
falló al preparar dos Silver sobre el mismo run Bronze; se corrigió el fixture
para respetar H05 y la repetición pasó. La revisión independiente respaldó
iniciar la lectura histórica explícita.
La regresión sintética general se ejecutó en la verificación integrada H06.
Seleccionó 1.011 casos y terminó con 908 resultados de llamada superados,
67 fallos y 37 errores; hubo además 26 exclusiones por alcance y seguridad.
Una llamada superada tuvo también un error de finalización, por lo que los
recuentos de pytest no son categorías de casos disjuntas. Las primeras trazas
registran falta de memoria de ejecución de Spark en agregaciones y después un
contexto detenido; su causa raíz y el alcance exacto de la cascada siguen en
investigación. Los dos primeros casos fallidos pasaron en una sesión nueva; una
secuencia focal instrumentada de tres casos también pasó sin identificar la
causa. En otra secuencia se observaron estados Delta persistidos; una limpieza
aislada de su caché redujo su número, sin reproducir ni explicar el fallo.
Esa ejecución no permitió la aceptación técnica integrada.

### Corrección de liberación de recursos D25

El autor aprobó D25, opción A, para corregir la liberación en IQR y en el
diagnóstico de metadatos. Se intenta liberar cada DataFrame registrado aunque
falle otra liberación. La excepción de la operación conserva su identidad,
causa y traza; los errores de liberación se conservan como excepciones y se
añaden como notas o, si no es posible, se registran. Si la operación termina
bien, se propaga el primer error de liberación. Si fallan simultáneamente todos
los mecanismos de diagnóstico, no se puede garantizar la observabilidad de
los errores secundarios.

La selección D25 superó 13 pruebas unitarias y tres casos Spark/Delta:
liberación fallida tras un diagnóstico correcto, diagnóstico histórico y
recuperación IQR desde una reserva. Para evitar inspeccionar las rutas protegidas
reales, el lanzador temporal usa raíces protegidas sintéticas; mantiene las
comprobaciones de rutas y emplea Ivy local. `TFM_REAL_CONFIG` permanece ausente.
La revisión independiente no identificó defectos bloqueantes. Esta corrección
no explica ni resuelve el agotamiento de memoria de Spark; en ese momento la
aceptación integrada de H06 seguía pendiente.

Comprobaciones, con SHA-256 del lanzador temporal
`7d3b81a36baeef30a8e836abb1a25bb56228140226d399c52e85137a06475040`:

```bash
env -u TFM_REAL_CONFIG PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m pytest -q -p no:cacheprovider tests/silver/test_cleanup_d25.py --tb=long
env -u TFM_REAL_CONFIG PYTHONDONTWRITEBYTECODE=1 timeout 480s .venv/bin/python -B /tmp/h06_d25_runner.py -q -p no:cacheprovider tests/silver/test_cleanup_d25_integration.py tests/silver/test_silver_iqr_publication.py::test_metadata_diagnostics_bind_historical_version_and_preserve_evidence tests/silver/test_silver_iqr_publication.py::test_orchestrated_recovery_of_reservation_without_delta --tb=long --junitxml=/tmp/h06-d25-spark-escalated.xml
```

### Verificación integrada D26 y D27

En D26 se compararon tres iteraciones sintéticas en sesiones limpias con otras
tres en una sesión compartida. Se observaron estados Delta retenidos entre
iteraciones, sin reproducir el agotamiento de memoria ni establecer su causa.

D27 aprueba dos puertas independientes: resistencia en sesión prolongada y
regresión sintética permitida en segmentos aislados. La resistencia completó
20 iteraciones y 140 operaciones en una sola sesión, en 1.374,69 segundos,
sin OOM, fallos ni paradas preventivas. Registró 31.940 trabajos y 58.480
identificadores de etapas asociados; estos últimos no acreditan etapas
ejecutadas. El muestreo puede no captar picos breves.

La regresión recogió 1.051 nodos: 1.023 permitidos superaron preparación,
llamada y cierre en 89 procesos independientes, sin fallos, errores ni saltos.
Se excluyeron individualmente 28: uno de datos reales, 25 que establecen
`TFM_REAL_CONFIG` y dos que inspeccionan rutas protegidas del proyecto. El
lanzador utilizó raíces protegidas sintéticas e Ivy local; las comprobaciones
de las rutas reales no quedaron cubiertas. La partición coincidió exactamente
con el inventario permitido y las huellas de fuentes permanecieron estables.
La revisión independiente respaldó ambas puertas dentro de esos límites.

La segmentación es una política de verificación, no una corrección del OOM
histórico, cuya causa sigue sin determinarse. Tampoco se ha establecido la causa
del incidente histórico de recencia IQR. La ratificación académica conjunta y
la validación con BDG2 real siguen pendientes.

### Lectura contextual histórica explícita

`read_synthetic_contextual(spark, config, publication_id, workspace)`, en
`tfm_lakehouse.silver.contextual_reader`, devuelve el marcador verificado y un
DataFrame Delta de la publicación indicada, fijado a la versión 0. El ejemplo
presupone `spark` local, una `config` sintética con sus rutas dentro de un
`workspace` absoluto aislado bajo `/tmp`, Bronze con los 16 edificios
aprobados y Silver eléctrica ya publicados:

```python
from tfm_lakehouse.silver.contextual_reader import read_synthetic_contextual
from tfm_lakehouse.silver.contextual_publication import publish_synthetic_contextual

publish_synthetic_contextual(spark, config, "silver_01", "contexto_01", workspace)
evidence, contextual = read_synthetic_contextual(spark, config, "contexto_01", workspace)
```

El ID es obligatorio. La lectura comparte el bloqueo local ya existente y
rechaza evidencia ausente, incompatible o modificada; no crea reservas ni
marcadores y no reconcilia commits. Contrasta la intención v2, los recursos
horarios, las fuentes retenidas, el commit, el marcador, el esquema completo
y los multiconjuntos contextual y eléctrico. No selecciona una publicación
automáticamente ni consulta resultados IQR. La validación del espacio puede
inspeccionar estructuralmente el árbol IQR.

El DataFrame devuelto es diferido: tras la devolución, la tabla y las fuentes
retenidas deben seguir disponibles e inalteradas. El bloqueo ya no protege las
acciones posteriores ni frente a modificaciones externas no cooperativas.
La huella D24 abarca todo el árbol físico de metadatos: una versión Bronze
posterior que lo cambie también impedirá leer la publicación anterior, aunque
su versión lógica siga retenida.

La selección de lectura superó diez pruebas con Spark/Delta, incluida la
coexistencia con IQR aceptado y no aceptado, y las comprobaciones unitarias y
regresiones unitarias seleccionadas superaron 48 pruebas. Las comprobaciones de
solo lectura abarcan los árboles de publicación y fuentes; Spark puede usar
archivos temporales internos durante sus acciones.

### Diagnóstico sintético de meteorología

El autor ha aprobado para sintéticos H06-D09: diagnosticar sin enriquecer ni
asociar analíticamente; D10: utilizar solo la versión weather del manifiesto
Bronze asociado a Silver e indicar su ausencia; D11: tratar `site_id` como
correspondencia espacial nominal, sin probar representatividad física; D12:
conservar etiquetas nominales y diagnosticar compatibilidad y ambigüedad sin
afirmar equivalencia temporal física; D13: conservar y diagnosticar duplicados
idénticos y conflictivos sin resolverlos; y D14: diagnosticar ausencias,
variables, cobertura, unidades y códigos sin imputar ni reinterpretar. La
ratificación académica conjunta sigue pendiente.

`diagnose_synthetic_weather(spark, config, silver_publication_id, workspace)`
devuelve disponibilidad, mapeos de edificios, versión y huella de origen,
duplicados globales y perfiles por emplazamiento, variable y mes. Las claves
meteorológicas se comparan como etiquetas originales; la ausencia de filas se
cuenta frente a las etiquetas eléctricas nominales distintas recibidas por sitio
y mes, no frente a horas físicas ni una rejilla completa. Por variable se
distinguen etiquetas horarias con filas de aquellas con valores numéricos
finitos. Las columnas ausentes se declaran por separado.
Los duplicados de metadatos y la disponibilidad de `site_id` se informan por
separado, también cuando una misma publicación presenta ambas incidencias.

La sensibilidad a transición compara hipótesis de offset de una etiqueta; no
distingue horas ambiguas de inexistentes, no selecciona `fold` ni asigna
instantes. La coincidencia nominal no demuestra alineación física ni
representatividad espacial. El contrato Bronze no registra UUID físico histórico;
los códigos especiales se cuentan sin transformar sus valores originales.
La selección sintética del diagnóstico, independencia de IQR y regresiones de
caracterización meteorológica superó 73 pruebas. Aquella selección no
materializó conjuntamente weather disponible e IQR aceptado. Una prueba
posterior sí los combinó con 16.667 horas sintéticas y verificó que el
diagnóstico no consulta ni recalcula IQR.
Otra sonda sintética confirmó el diagnóstico sin bloqueo de duplicados
conflictivos ajenos y la fidelidad de las unidades al catálogo documental.
Tras reforzar el diagnóstico simultáneo de duplicados y columna ausente, la
selección afectada y las regresiones meteorológicas superaron 72 pruebas.

### Asociación meteorológica nominal sintética

El autor aprobó D28–D31 para ampliar H06 con asociación meteorológica y una
publicación independiente, revisando expresamente el aplazamiento D15. D32
revisa D28: permite elegir por sitio la etiqueta nominal más cercana hasta
1.800 segundos, incluido el límite; ante empate se elige la anterior. Primero
se selecciona la clave y después se rechaza si está duplicada, sin buscar otra.
El desplazamiento es meteorología menos electricidad. No se convierten zonas,
no se resuelve DST y la proximidad nominal no acredita simultaneidad física.

D33 exige la fuente y claves meteorológicas para publicar; D34 conserva texto,
representación decimal exacta y posibles códigos especiales; D35 distingue
duplicados y cobertura sin inflar observaciones compartidas; D36 exige captura
distribuida durable para la futura publicación. La ratificación académica
conjunta sigue pendiente. Se conserva la aceptación sintética D01–D27 y D09–D14
sigue vigente para el diagnóstico independiente.

`associate_synthetic_weather` construye una vista sintética diferida vinculada
a los IDs explícitos de contexto y Silver y a la versión Bronze del manifiesto.
Conserva el multiconjunto contextual y eléctrico y rechaza asociaciones cuya
clave elegida esté duplicada. La evidencia incluye la huella física weather y
la versión de política. Las fuentes deben seguir disponibles e inalteradas
durante las acciones posteriores: la vista no es la captura durable de D36.
La publicación meteorológica, su recuperación, lector histórico y cobertura
completa corresponden a actividades posteriores y no están implementados aquí.

### Calidad y cobertura meteorológicas sintéticas

`diagnose_synthetic_weather_quality(spark, config, silver_id, contextual_id,
workspace)` devuelve evidencia y tres DataFrames diferidos: `coverage`,
`variables` y `sensitivity`. Por ejemplo, con la sesión y configuración
sintéticas y las publicaciones explícitas ya disponibles:

```python
from tfm_lakehouse.silver.weather_quality import diagnose_synthetic_weather_quality

evidence, reports = diagnose_synthetic_weather_quality(
    spark, config, "silver_01", "contexto_01", workspace
)
reports["coverage"].where("scope = 'site'").show()
```

Los resultados agrupan directamente por edificio o emplazamiento y mes de la
etiqueta eléctrica; las fechas inválidas se agrupan en `unassigned`. La
precedencia de estados es sitio ausente, sitio inválido y fecha inválida. Las
filas restantes son elegibles y se reparten entre asociación exacta, anterior,
posterior, clave duplicada rechazada y ausencia. Las filas originales repetidas
se cuentan; las etiquetas eléctricas y claves meteorológicas distintas se
deduplican dentro de cada grupo, sin sumar edificios ni meses entre sí.

La cobertura de filas es `associated_rows / eligible_rows`; se publica también
`associated_rows / electrical_rows`. La cobertura nominal de etiquetas es
`associated_electrical_keys / eligible_electrical_keys`. Los denominadores cero
producen null. Por variable, `available_ratio` y `interpretable_ratio` dividen
por `associated_rows`. Los valores numéricamente interpretables son los de
estado `finite`; los posibles códigos especiales se cuentan aparte, junto al
subconjunto finito sin esa marca y su proporción sobre `associated_rows`.
Ninguno de estos recuentos demuestra validez física ni elegibilidad analítica.
Cada variable incluye además las claves eléctricas elegibles y asociadas y las
claves meteorológicas asociadas, disponibles, finitas, con posible código y
finitas sin esa marca. No se ha implementado la cobertura opcional frente a un
calendario nominal completo.

Se preserva el texto y se reutiliza el intérprete decimal exacto: admite signo,
dígitos ASCII, punto decimal opcional y exponente decimal opcional; rechaza
espacios y no redondea. Los estados distinguen observación no asociada, columna
ausente, null, vacío, `NaN` exacto, sintaxis inválida, infinito literal,
desbordamiento, subdesbordamiento, escala excedida y finito. `-1` en
precipitación y `9` en nubosidad se señalan como posibles códigos sin atribuir
un significado analítico. Cada estado de una observación asociada es exclusivo.

La sensibilidad recalcula la selección para 0, 900, 1.800 y 3.600 segundos,
manteniendo el desempate hacia la etiqueta anterior y el rechazo de duplicados.
Los incrementos comparan cada tolerancia con la precedente; 3.600 segundos son
solo diagnóstico. Mayor cobertura no implica mejor coincidencia física. Los
desplazamientos con signo y las distancias absolutas se resumen con mínimo,
máximo y media para asociaciones únicas. La media se calcula por fila
eléctrica, incluida su multiplicidad y el uso compartido de observaciones. Los
DataFrames no recogen el conjunto completo en el conductor; siguen siendo
diferidos y requieren fuentes retenidas e inalteradas durante su evaluación.

Las pruebas sintéticas enfocadas de calidad superaron 16 casos; la regresión de
asociación, tres, y la selección unitaria numérica, temporal y meteorológica,
141. Una prueba Spark/Delta verificó el vínculo explícito y la independencia
IQR con fuentes inventadas. La resistencia prolongada y la regresión segmentada
integrada de D27 no se han repetido para esta ampliación; sus resultados
anteriores permanecen vigentes dentro del alcance en que se obtuvieron.

En el primer intento de prueba de la asociación, el arranque estándar consultó
metadatos de componentes de rutas protegidas. Se interrumpió la prueba sin leer
datos reales; los lanzadores posteriores sustituyen esas raíces por rutas
sintéticas y usan las dependencias locales existentes.

### Identidad meteorológica sintética preliminar

`build_synthetic_weather_identity(spark, config, silver_id, contextual_id,
publication_id, workspace)` devuelve una intención preliminar y una vista
diferida. Vincula el ID explícito, Silver, contexto v2, manifiesto y versión
weather Bronze, URI y SHA-256 declarados, huella física, esquema y versiones de
las políticas de asociación, interpretación y calidad. La regla operativa es
de 1.800 segundos inclusivos con desempate anterior y rechazo de la clave
duplicada elegida sin alternativa. Las tolerancias diagnósticas no la cambian.

`verify_synthetic_weather_identity(..., expected, candidate)` reconstruye la
intención, compara el JSON distinguiendo tipos y comprueba el esquema y el
multiconjunto completo con `exceptAll` en ambos sentidos. Verifica también las
proyecciones contextual y eléctrica mediante la asociación existente y
recomprueba las fuentes. El resultado acredita esa comparación durante la
acción, sin identificar como durable la vista diferida. La captura D36 y la
identidad definitiva del contenido están pendientes.

La huella física meteorológica conserva la definición D24: SHA-256 del mapa
canónico de rutas relativas y SHA-256 de cada archivo, incluido el registro
Delta. Los archivos se leen por bloques; el mapa de rutas permanece en memoria.
La huella acredita coherencia con la evidencia, no autenticidad externa, y no
detecta necesariamente modificaciones revertidas entre comprobaciones. Las
dependencias contextuales existentes mantienen sus propias limitaciones de
memoria; esta actividad no acredita escalabilidad de extremo a extremo.
La selección sintética de identidad, asociación y calidad superó 23 casos;
una prueba Delta adicional verificó la versión meteorológica histórica, la
reconciliación y el rechazo de una alteración del commit. La regresión Delta
de vínculo de calidad y la de vínculo de asociación superaron una prueba cada
una. No se repitieron la regresión general ni la resistencia D27 en esta
actividad.

1. Validar claves de metadatos, relación edificio-emplazamiento, superficie
   y zona horaria frente a las fuentes y contratos aprobados.
2. Implementar una unión de metadatos con cardinalidad controlada,
   detectando duplicidades y evitando multiplicar mediciones eléctricas.
3. Caracterizar muestreo meteorológico, unidades, códigos especiales,
   emplazamientos y representación horaria original.
4. Definir y probar la alineación meteorológica, incluidos periodos sin
   correspondencia y cambios de horario estacional.
5. Publicar la integración aprobada y sus métricas de cobertura, versiones
   de origen y casos sin enriquecimiento.

## Entregables previstos

- Reglas de unión auditables y enriquecimiento con metadatos implementado.
- Integración meteorológica opcional y documentada, o una decisión justificada
  de no incorporarla cuando las fuentes no permitan una unión válida.
- Pruebas de IDs ausentes, áreas nulas, claves repetidas, emplazamientos
  incompatibles y ausencia o irregularidad de datos meteorológicos.

## Criterios de aceptación

- [ ] Claves y cardinalidades se justifican y se prueban también con casos
      negativos.
- [ ] No se pierden ni multiplican observaciones eléctricas de forma
      involuntaria.
- [ ] Unidades y semántica horaria de meteorología verificadas; lo
      desconocido no se convierte silenciosamente a UTC ni a otra unidad.
- [ ] La cobertura de correspondencias válidas y no válidas se registra
      por fuente, emplazamiento y periodo.
- [ ] Gold puede distinguir información contextual presente y ausente.

## Issues de GitHub propuestos

- `[H06] Validar y asociar metadatos sin multiplicar registros`
- `[H06] Revisar unidades y alineación temporal de meteorología`
- `[H06] Integrar meteorología opcional con métricas de correspondencia`
- `[H06] Verificar trazabilidad y fallos de integración Silver`

## Límites y condición de avance

No forzar la unión meteorológica para completar una representación de la
arquitectura ni atribuir causalidad a una correspondencia por emplazamiento.
H07 podrá avanzar sin meteorología si ambos autores aprueban esa limitación.
