# Hito 6 — Integración de metadatos y meteorología

**Estado:** resistencia y regresión segmentada superadas en su alcance
original. Las ampliaciones meteorológicas tienen verificación sintética enfocada.
Las fuentes locales y un snapshot Bronze real se han verificado. La validación
real de Silver y la ratificación académica siguen pendientes. **Responsabilidad:** Irantzu valida significados y
uniones; Carlos implementa y verifica la integración. **Objetivos:** OE3,
OE4. **Requisito previo:** Silver eléctrica aceptada en H05.

## Finalidad

Enriquecer la electricidad seleccionada con metadatos fiables y, **solo si
la correspondencia es justificable**, observaciones meteorológicas. Las
mediciones sin información contextual válida deben conservarse o tratarse
según reglas explícitas y quedar contabilizadas.

## Actividades incrementales

### Validación progresiva con los datos locales de BDG2

La auditoría en solo lectura verificó los tres CSV contra el manifiesto
local e identificó una publicación Bronze con electricidad y metadatos
en versión 1 y meteorología en versión 0. La lectura secuencial de los
Parquet activos con Apache Parquet comparó exactamente las filas,
incluidos los nulos y las cuatro columnas de linaje. La igualdad en
orden físico comprobó también la multiplicidad. El contraste se
realizó sin Spark y se limita a las versiones y huellas verificadas.

El supervisor incorpora una carga para Silver eléctrica vinculada a la
instantánea auditada y a una autorización de ejecución única. Verifica la
autorización en el proceso lanzador y en el hijo, comprueba las 13 huellas
antes de iniciar la JVM y reserva las salidas de forma exclusiva. Spark
lee los Parquet exactos de electricidad y metadatos. Los archivos de
meteorología quedan vinculados por sus huellas.

La publicación usa la transformación `h05.3-v1` y la cohorte de
[H04](hitos-4.md). Una referencia independiente generada desde el CSV
permite comparar todas las filas y sus multiplicidades mediante
diferencias distribuidas en ambos sentidos. La comprobación incluye los
19 campos, los nulos, los valores decimales, las etiquetas nominales y el
linaje. El resultado debe tener un único commit Delta y las huellas de
entrada deben mantenerse estables. Linux restringe las escrituras del
proceso y sus descendientes a los destinos reservados y al espacio de
trabajo.

La integración con archivos inventados concilió 32 posiciones, incluidas
filas repetidas, publicó la versión Delta 0 y confirmó el cierre de los
procesos supervisados. La publicación con BDG2 sigue pendiente. Se
esperan 280.704 posiciones eléctricas, incluidas las 352 ausencias
observadas en la auditoría.

La auditoría registró dos claves meteorológicas duplicadas por
emplazamiento en Bull, Fox, Peacock y Robin. La interpretación temporal de
esas claves queda pendiente. También siguen pendientes la comparación
independiente con el archivo de distribución y el contraste de las tres
discrepancias de superficie con el libro de trabajo de la memoria.
Las superficies del CSV y de la memoria son, respectivamente, 5.842,9 y
5.824,9 m² para `Robin_lodging_Dorthy`, 5.885,2 y 5.885,3 m² para
`Fox_lodging_Wallace`, y 4.188,6 y 4.188,8 m² para
`Bull_lodging_Danielle`. Las diferencias no cambian la cohorte.

La validación continúa por Silver eléctrica, IQR, contexto y
meteorología, seguida de comprobaciones integradas proporcionadas.
Las pruebas sintéticas preventivas de esas capas y los escenarios
opcionales de estrés quedan aplazados. Los controles integrados
pendientes se reevaluarán con evidencia de ejecución.

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

La vista bloquea duplicados idénticos o conflictivos de metadatos de los
edificios seleccionados y diagnostica sin bloquear los duplicados ajenos.
Conserva las mediciones sin metadatos y expresa las ausencias. La superficie
original se conserva y solo se interpreta cuando es finita, positiva y
exactamente representable como `Decimal(38,18)`, sin redondeo. La vista
derivada no persistente se vincula a Bronze y Silver. La consulta IQR es
independiente y el enriquecimiento no recalcula IQR. La ratificación académica
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

La publicación Delta contextual es independiente y se vincula a las versiones
exactas de Silver, Bronze, metadatos y método. La interpretación temporal está
versionada y sus recursos horarios se acreditan. La reserva, el commit y la
finalización disponen de recuperación explícita sin reintentos automáticos.
La lectura exige un ID explícito y fuentes retenidas. IQR permanece vinculado
a la Silver eléctrica original. La ratificación académica conjunta sigue
pendiente.

La interpretación horaria controlada utiliza un recurso de zona: el recurso resuelto con la precedencia de `zoneinfo` se lee una vez y sus mismos
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

La intención contextual versión 2 incluye la huella SHA-256
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

### Conservación de excepciones y liberación de recursos

En IQR y en el diagnóstico de metadatos se intenta liberar cada DataFrame registrado aunque
falle otra liberación. La excepción de la operación conserva su identidad,
causa y traza; los errores de liberación se conservan como excepciones y se
añaden como notas o, si no es posible, se registran. Si la operación termina
bien, se propaga el primer error de liberación. Si fallan simultáneamente todos
los mecanismos de diagnóstico, no se puede garantizar la observabilidad de
los errores secundarios.

La selección de liberación de recursos superó 13 pruebas unitarias y tres casos Spark/Delta:
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

### Verificación integrada de resistencia y regresión

Se compararon tres iteraciones sintéticas en sesiones limpias con otras
tres en una sesión compartida. Se observaron estados Delta retenidos entre
iteraciones, sin reproducir el agotamiento de memoria ni establecer su causa.

La verificación comprende dos pruebas independientes: resistencia en sesión prolongada y
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

### Supervisión cooperativa de procesos

`launch_supervised_workload` ejecuta sondas registradas en una campaña
sintética autorizada y aplica los límites de recursos. El conductor
valida el recibo del hijo y confirma la primera muestra de procesos, memoria y
disco antes de enviar el permiso. Observa la ejecución y contrasta el resultado
y las muestras finales antes de aceptar el éxito. Mantiene los intervalos de
muestreo de 1 s para procesos y memoria y de 5 s para disco. Los relojes
monotónicos aplican el límite de 30 min a la operación y el de 8 h a la campaña.

Una parada queda registrada desde la primera observación que la exige. El
conductor vuelve a comprobar la identidad y la sesión de cada miembro y envía
`SIGTERM` mediante su `pidfd`. Mantiene la observación durante la gracia.
Puede enviar `SIGKILL` a un miembro que siga verificado tras 30 s desde la
confirmación del envío de `SIGTERM`. El conductor recoge al hijo directo y
comprueba por separado la salida de los descendientes observados. Conserva la
clasificación y los miembros pendientes al vencer el plazo. Cada ejecución
tiene un solo intento y cada miembro recibe como máximo una solicitud por fase.

El resultado distingue el éxito supervisado, la parada D44, los fallos de
monitoreo y de carga, la terminación incompleta y la evidencia sin confirmar.
La comprobación de identidad, el envío de señales y la publicación tienen
trabajos acotados con plazas independientes. Una operación en curso puede
seguir bloqueada en el kernel tras vencer su plazo. La comprobación inicial
del espacio D39 y su reserva se realizan antes de arrancar el hijo. Si el
resultado en disco queda incompleto, la respuesta conserva el estado y los
motivos conocidos. `failure.json` prevalece si también se llegó a ver un
`completed.json` cuya escritura no pudo confirmarse.

El 27/11/2026 se superaron 176 pruebas enfocadas en 27,85 s. Las 36 nuevas
comprobaron el rechazo inicial en cada umbral de parada y los fallos de
medición y escritura por inyección. Se envió `SIGTERM` a una sonda registrada
y a un descendiente sintético controlado por su padre. Otro hijo sintético
ignoró `SIGTERM` y recibió `SIGKILL` con un reloj inyectado. El plazo de
producción es de 30 s. La revisión independiente fue de solo lectura.

El seguimiento cubre los miembros observables de una carga cooperativa.
Un descendiente puede salir de la sesión y perder su parentesco visible
antes de la primera muestra. El `pidfd` fija la instancia del proceso entre
la comprobación y la señal. La pertenencia se comprueba justo antes del envío.
La integración futura requiere registrar y verificar la JVM, el daemon y los
workers reales de Spark bajo D46, incluidos sus cambios de grupo y el cierre
de procesos. La sonda de integración siguiente realiza esa comprobación para
un único perfil sintético.

### Integración de Spark con el supervisor de recursos

El lanzador registra `synthetic_spark_probe_v1` para `launch_supervised_workload`.
La sonda usa Spark local con dos particiones, ocho valores inventados y una
operación de shuffle. La campaña D39 debe estar autorizada en ambos procesos
y contener los tres JAR de Delta y ANTLR ya disponibles bajo `ivy/jars`.
El permiso vincula sus huellas, el directorio temporal de campaña y el destino
de evidencia. El JVM y los workers usan esa campaña. El arranque del gateway
queda restringido al `spark-submit` de la instalación PySpark fijada.

```python
from tfm_lakehouse.resource_supervisor import SparkProbeParameters, launch_supervised_workload
from tfm_lakehouse.scalability_workspace import authorize_campaign

campaign = authorize_campaign(campaign_id, owner_id)
result = launch_supervised_workload(
    campaign,
    campaign.root / "temp" / "pytest-spark-probe-01",
    "spark-probe-01",
    handler="synthetic_spark_probe_v1",
    parameters=SparkProbeParameters(),
)
```

El directorio base y el identificador de ejecución deben ser nuevos. El
supervisor coteja la JVM de Py4J con el proceso abierto por el conductor y al
daemon y los workers con sus muestras y registros de salida. El daemon
mantiene su propio grupo dentro del SID de la carga. El conductor recoge su
gateway y el supervisor recoge al conductor y confirma la salida de los
descendientes observados. El daemon conserva su propia limpieza de grupo.
El watchdog envía señales individuales mediante `pidfd`. Los límites D44 se
evalúan desde antes de conceder el permiso hasta la comprobación final.

El resultado conserva duraciones monotónicas de inicialización, operación y
cierre. Las medidas de heap proceden de un hilo de diagnóstico con espera
acotada. El RSS y la CPU se obtienen de procfs y los máximos corresponden a
las muestras observadas. El registro de eventos distingue los IDs de etapa
asociados a cada trabajo, los pares de etapa e intento con inicio de tarea y
los pares con `TaskEnd`. Los bytes de shuffle y derrame suman los intentos
de tarea que los notificaron. Un contador ausente conserva el valor nulo y
el registro documenta la cobertura de cada suma.

El supervisor mantiene los plazos y las garantías de atribución de D46
aunque se bloquee la consulta de heap. La consulta bloqueada se comprobó
mediante un proveedor inyectado. La observación sigue limitada a miembros
que se hayan podido identificar y a la disponibilidad del kernel. La sonda
cubre el arranque y la terminación controlada de este perfil local.

La verificación del 27/11/2026 superó 186 casos enfocados en 41,55 s, incluidas
las dos sondas Spark. En el cierre normal se identificaron el conductor, la
JVM, el daemon y dos workers. Hubo muestras D44 durante inicialización,
operación y cierre. El heap tenía un límite efectivo de 1 GiB y un máximo
observado de 71.261.440 B. El registro muestra un trabajo, dos pares de etapa
e intento con tareas terminadas, cuatro `TaskEnd`, 308 B de shuffle leído y
escrito y cero de derrame a memoria y disco en los cuatro informes.

En la segunda sonda se inyectó el límite RSS mientras los workers estaban
activos. El watchdog envió cinco `SIGTERM` reales a procesos verificados. Se
recogieron el conductor y su JVM, se confirmó la salida de los descendientes
y el proceso de contraste ajeno siguió activo. Los cuatro contadores de
tarea quedaron nulos al cerrarse el registro sin ningún `TaskEnd`.

### Calibración sintética Bronze

`synthetic_bronze_calibration_v1` ejecuta una calibración mediante
`launch_supervised_workload`. La preparación exclusiva crea tres CSV
inventados y un manifiesto con sus esquemas, asignaciones de sitio y SHA-256.
La electricidad contiene ocho filas anchas y 16 edificios, equivalentes a
128 posiciones. Los metadatos contienen 16 registros y la meteorología 32.
El lector de snapshots contrasta los commits y las versiones publicadas.
La calibración compara después todos los valores, duplicados y columnas de
linaje con la fixture, y vuelve a comprobar las huellas de las fuentes.

La ejecución autorizada del 27/11/2026 terminó con supervisión correcta,
sin avisos ni paradas. Publicó y reconcilió 8, 16 y 32 filas respectivamente
en tres tablas Delta, cada una en la versión 0. La preparación tardó 0,073 s,
el preflight 0,111 s y la ejecución supervisada completa 18,229 s. La ingesta
Bronze, incluidas sus validaciones y comprobaciones internas, tardó 10,331 s.
La lectura del snapshot y la reconciliación posterior tardaron 1,405 s.
La escritura de electricidad ocupó 4,828 s dentro de la ingesta. El arranque
Spark tardó 2,189 s y el cierre 0,509 s. La sonda Python previa tardó 3,018 s
y sus tareas forman parte de los contadores Spark de la ejecución.

Los tres CSV sumaron 3.798 B. Las tablas Delta ocuparon 135.168 B asignados en
18 ficheros, incluida su estructura de transacciones. La razón observada fue
35,59 sobre los bytes de entrada. El máximo muestreado de RSS conjunto fue
1.328.627.712 B. Estos datos corresponden a una sola calibración y no permiten
extrapolar tiempos ni ocupación a tamaños mayores. Antes de comparar anchuras,
los escenarios independientes previstos usan 128, 512 y 1.600 columnas y
conservan las mismas comprobaciones de fuente, linaje y supervisión.

La verificación de anchura posterior ejecutó ocho filas eléctricas con 1.600
edificios sintéticos, incluidos los 16 de la cohorte. Los metadatos pasaron a
1.600 registros y la meteorología conservó 32. El supervisor registró éxito,
sin avisos ni procesos pendientes, y la reconciliación comprobó las tres tablas
en versión 0. La ejecución supervisada tardó 21,086 s y la ingesta 12,218 s.
La reconciliación tardó 1,957 s y las tablas ocuparon 970.752 B asignados.

El resumen automático de eventos fue parcial por los límites de lectura de
línea y fichero. El registro Spark completo quedó conservado y una relectura
independiente confirmó 55 trabajos, 50 intentos de etapa con tareas terminadas,
73 `TaskEnd`, 24.867 B de shuffle en cada sentido y cero derrame reportado.
La comparación de anchura también cambia los valores eléctricos desde la
segunda hora y el número de metadatos. En esa ejecución, la verificación
usaba `collect()` y construía el multiconjunto en el conductor.

La verificación actual genera y valida cada registro CSV por separado y
calcula la huella SHA-256 de forma incremental. Spark genera los valores
esperados a partir de ordinales, al margen del lector CSV de Bronze. Tras
comprobar nombres, orden y tipos, agrupa por un array con todos los valores
de la fila y su linaje. La suma de las apariciones con signo debe ser cero
para cada fila. La comparación conserva las correlaciones, los nulos y los
duplicados, y devuelve al conductor un indicador de igualdad. El lector de
snapshots sigue comprobando las versiones Delta y sus commits.

El lector de eventos admite hasta 64 MiB por fichero, 2 MiB por línea y
200.000 eventos. Retiene identificadores para contar trabajos, etapas y tareas
sin duplicarlos. Las sumas de métricas sin observaciones se registran como
ausentes. Un fichero en curso, truncado o inválido no aporta evidencia
completa de medición. La verificación terminal sigue sujeta al plazo D46.

### Escritura distribuida Bronze

El primer intento combinado usó 17.544 filas eléctricas y 1.600 columnas de
edificios, equivalentes a 28.070.400 posiciones sintéticas. Validó los tres
CSV. La lectura eléctrica con `multiLine=True` produjo una partición, que
se mantuvo al materializar el DataFrame y al escribirlo. La única tarea
eléctrica agotó el heap Java de 1 GiB al ampliar un diccionario Parquet.
Spark registró `TASK_WRITE_FAILED` en la etapa 14.0. El intento terminó
antes del primer commit Delta y de la reconciliación distribuida.

La escritura redistribuye filas completas justo antes de persistir.
Bronze calcula el número de particiones con el mayor de dos cocientes
redondeados al alza, los bytes de CSV sobre 8 MiB y el número de campos
de todas las filas sobre 524.288, incluidos los cuatro campos de linaje.
El número de filas limita el número solicitado. A partir de dos particiones
se aplica `repartition(n)` sin columnas, con un intercambio circular. Una
única llamada de escritura produce el commit Delta de cada tabla. Se
conservan la lectura CSV, los valores y sus correlaciones, el esquema
ordenado y la publicación por versiones.

Los dos criterios estiman el volumen por tarea. El CSV incluye cabecera y
los registros pueden tener tamaños distintos. Las tareas de `local[2]`
comparten el mismo heap de 1 GiB. Reducir las filas por tarea puede reducir
el tamaño de cada diccionario, aunque dos diccionarios pueden crecer a la
vez. El shuffle añade trabajo de serialización y puede usar disco temporal.
Se mantienen las opciones de diccionario y de memoria y los límites
del supervisor.

Pasaron 44 pruebas enfocadas y tres pruebas Spark de contrato. Estas últimas
comprobaron la escritura repartida con CSV multilínea, nulos, duplicados y
linaje. Comprobaron también que un fallo en la segunda tabla conserva el
primer commit y deja un informe de ingesta fallida. La prueba supervisada
de 512 filas eléctricas y 1.600 edificios
terminó con `supervised_success` en 24,391 s. El plan adaptativo conservó el
intercambio explícito. Dos tareas eléctricas se solaparon durante 1,244 s y
escribieron 256 filas cada una. La fase de escritura eléctrica, incluido el
shuffle, tardó 6,267 s. El máximo muestreado fue de 718.878.568 B de heap
y 1.854.550.016 B de RSS conjunto. La reconciliación distribuida verificó
512 filas eléctricas, 1.600 de metadatos y 2.048 meteorológicas, con un solo
commit versión 0 en cada tabla. El supervisor no registró avisos ni paradas y
confirmó el cierre de los procesos.

El combinado con escritura distribuida reutilizó las mismas fuentes y terminó con
`supervised_success` en 89,449 s. Spark ejecutó 54, 1 y 2 tareas de
escritura para electricidad, metadatos y meteorología. Las eléctricas
escribieron entre 324 y 325 filas por tarea y alcanzaron dos tareas en
ejecución simultánea. La reconciliación distribuida verificó las tres
tablas y un único commit en versión 0 por tabla. D44 no registró avisos
ni paradas y D46 confirmó la recogida completa de los procesos.

El máximo muestreado de heap fue de 1.038.949.376 B de 1 GiB, antes de
las tareas de escritura eléctrica. Spark registró 327.805.071 B de
derrame a disco. La verificación de escalabilidad Bronze queda cerrada
en el alcance sintético ensayado.

### Calibración eléctrica Silver sintética

Se verificó en lectura el snapshot Bronze
`75df603267584eb59595760b14d4d795`: electricidad y metadatos Delta en
versión 0, con ocho y dieciséis filas, respectivamente. Las huellas de
los CSV, el informe H02 y los metadatos de commit son coherentes con
la evidencia Bronze conservada.

La publicación eléctrica `h05.3-v1` exige los nombres de la cohorte H04.
El snapshot contiene dieciséis identificadores inventados
`SYNTHETIC_building_00` a `SYNTHETIC_building_15`. La comprobación
de esquema rechaza esa combinación. La campaña sintética admite destinos Silver y de
evidencia separados de Bronze dentro de la misma campaña autorizada.

`d45-electrical-synthetic-v1` es el perfil de las calibraciones sintéticas de escalabilidad. El permiso del supervisor vincula la campaña, el run Bronze,
los UUID y versiones Delta, las huellas de los archivos y la cohorte.
El publicador reutiliza la transformación H05 y persiste esa identidad
en la intención, los metadatos del commit y la confirmación de
publicación. Su lector verifica esos vínculos y la igualdad distribuida
del multiconjunto completo frente a un resultado esperado independiente.
El perfil tiene su propio directorio de publicación. La API ordinaria
sigue exigiendo la cohorte H04 para `h05.3-v1`.

Se corrigió la liberación de la publicación eléctrica independiente.
Se intenta liberar el DataFrame incluso si falla `persist`, y se
intenta cerrar el descriptor aunque falle el desbloqueo. La excepción
primaria conserva su identidad, causa y traza. Cuatro casos de fallos
inyectados quedaron cubiertos en una selección de 16 pruebas unitarias.
No se inició Spark ni se publicó Silver para esta comprobación.

La calibración terminó con `supervised_success`
en 17,243 s. Sobre las ocho filas eléctricas Bronze y los dieciséis
edificios sintéticos, publicó y reconcilió exactamente 128 filas
eléctricas Silver en un solo commit Delta versión 0. El snapshot
Bronze y sus huellas se verificaron antes y después de publicar.
Pasaron 26 pruebas unitarias y tres pruebas Spark enfocadas. La revisión
independiente cotejó la publicación, el origen y la recogida de procesos.

La comprobación Bronze tardó 6,743 s, la materialización Silver 1,079 s,
la escritura Delta 0,976 s y la comprobación y publicación final 1,291 s.
El máximo muestreado fue de 409.377.240 B de heap de 1 GiB y
1.318.158.336 B de RSS conjunto. El registro Spark completo recogió
39 trabajos, 39 intentos de etapa ejecutados, 59 tareas terminadas,
33.440 B de shuffle en cada sentido y cero derrame reportado. Delta
ocupó 53.248 B asignados en seis ficheros. D44 no registró avisos ni
paradas, y D46 confirmó el cierre de los procesos.

La calibración de 128 posiciones no establece el rendimiento de otras
escalas. Cada escenario vincula su snapshot Bronze al registro del perfil.

La ejecución eléctrica Silver de 10.000 posiciones usó 625 filas Bronze
y los 16 edificios sintéticos. Terminó con `supervised_success` en
19,731 s y reconcilió exactamente 10.000 filas en un único commit Delta
versión 0. La verificación Bronze tardó 7,053 s, la materialización
Silver 1,369 s, la escritura 1,268 s y la reconciliación y publicación
1,765 s. El heap máximo muestreado fue de 292.679.808 B y el RSS conjunto
de 1.306.411.008 B. Spark registró 1.711.196 B de shuffle en cada sentido,
sin derrame reportado. El supervisor no registró paradas ni avisos y confirmó
la recogida completa de los procesos.

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

### Captura meteorológica distribuida sintética

D37 aprueba una captura identificada por ID explícito, UUID físico Delta,
versión 0, esquema y huella física D24, con reconciliación completa del
enriquecimiento y de las proyecciones contextual y eléctrica. Esta identidad
corresponde a una representación física concreta, no acredita autenticidad
externa ni una identidad lógica independiente de la representación. D38
aprueba la reserva exclusiva, propietario explícito y bloqueo cooperativo;
una captura existente no se sobrescribe ni recupera automáticamente.

```python
from tfm_lakehouse.silver.weather_capture import (
    create_synthetic_weather_capture, verify_synthetic_weather_capture,
)

capture = create_synthetic_weather_capture(
    spark, config, "silver_01", "contexto_01", "weather_01",
    "captura_01", "autor", workspace,
)
verified = verify_synthetic_weather_capture(spark, config, "captura_01", "autor", workspace)
```

La reserva, intención y marcador viven en el espacio sintético aislado. La
tabla se escribe de forma distribuida y el marcador de finalización es la
última escritura, tras verificar fuentes, commit, esquema y multiconjuntos.
Una captura incompleta, ajena o modificada se rechaza; no hay reintentos,
reparación, caducidad ni API de borrado. El autor decide su retirada manual tras
revisar identidad y dependencias. El propietario y el bloqueo coordinan usos
locales; no autentican a usuarios ni protegen contra escritores externos.

Los archivos de la tabla se huellan por bloques y el log Delta se lee por
líneas de hasta 8 MiB; una línea mayor bloquea la verificación. El mapa de
rutas, el catálogo y las dependencias contextuales conservan costes en el
conductor. La publicación contextual existente recoge sus filas y su lector
ordena y recorre grupos en el conductor. La captura tampoco evita el coste de
shuffle, almacenamiento temporal o posibles sesgos de partición. No se ha
acreditado escalabilidad de extremo a extremo ni validado con BDG2 real.

La integración sintética de captura superó dos pruebas Spark/Delta en
341,48 segundos, con 32 filas eléctricas, particiones alternativas y
verificación tras vaciar la caché. La selección unitaria de captura y
regresiones de identidad, asociación, calidad y D25 superó 42 casos; una
selección adicional de identidad, asociación y calidad superó 23 casos con
Spark sintético. Los fallos de escritura y de cambio de fuente durante la
materialización se inyectaron con un escritor simulado. No se ejecutaron la
regresión general, la resistencia D27 ni la campaña de escala para este cambio.

### Publicación meteorológica sintética y recuperación

`publish_synthetic_weather(spark, config, capture_id, owner_id, publication_id,
workspace)` publica una captura explícita verificada en una tabla Delta
independiente. El ID debe coincidir con la intención meteorológica de la
captura. La intención vincula el marcador y la huella de la captura; el
marcador de publicación registra UUID, commit, versión 0, esquema y huella.
Se concilian en ambos sentidos el multiconjunto completo y las proyecciones
contextual y eléctrica.

```python
from tfm_lakehouse.silver.weather_publication import publish_synthetic_weather

evidence = publish_synthetic_weather(
    spark, config, "captura_01", "autor", "weather_01", workspace
)
```

La reinvocación explícita puede continuar una reserva compatible sin tabla,
verificar y finalizar una tabla comprometida sin marcador o devolver una
publicación completa verificada. Una tabla parcial, intención o marcador
incompatibles, fuentes ausentes o evidencia desconocida bloquean la operación
y se conservan. Nunca se recrea una captura ni se ejecuta otro commit para
recuperar uno existente. No había selección automática ni lector histórico
meteorológico en esa fase.

La captura y sus fuentes deben permanecer disponibles e íntegras para
publicar, recuperar o verificar. D38 permite al autor retirarla tras revisar
dependencias; hacerlo puede impedir verificaciones posteriores y no establece
retención permanente. Los bloqueos son cooperativos y las verificaciones no son
una transacción entre tablas ni resisten necesariamente modificaciones externas
coordinadas o revertidas. Antes del marcador final no existe un UUID de
publicación anclado fuera de la tabla: una sustitución con la misma intención
y contenido puede resultar indistinguible. Tras finalizar, el marcador vincula
UUID, commit y huella. La escritura del marcador sincroniza el archivo, sin
garantizar resistencia completa ante pérdida de alimentación del sistema.

### Lectura histórica meteorológica sintética

`read_synthetic_weather(spark, config, publication_id, workspace)` exige un ID
explícito y devuelve la evidencia verificada y un DataFrame Delta fijado a la
versión 0. No reserva, recupera ni selecciona la publicación más reciente.

```python
from tfm_lakehouse.silver.weather_reader import read_synthetic_weather

evidence, frame = read_synthetic_weather(spark, config, "weather_01", workspace)
```

La lectura abre los bloqueos existentes en modo compartido, primero el de
publicación y después el de captura, sin crearlos. Comprueba intención, marcador,
commit, UUID, huellas y esquemas; verifica la captura y las fuentes retenidas y
reconcilia los multiconjuntos enriquecido, contextual y eléctrico. Rechaza
evidencia ausente, incompleta, alterada o incompatible, sin repararla.

El DataFrame devuelto conserva el esquema físico Delta, incluida su nulabilidad;
el esquema lógico original se verifica y figura en la evidencia. El DataFrame es
diferido y los bloqueos se liberan antes de las acciones del consumidor: tabla,
captura y fuentes deben permanecer disponibles e inalteradas. Los bloqueos no
excluyen escritores externos no cooperativos; las huellas acreditan coherencia,
no autenticidad externa. La limitación de anclaje de UUID anterior al marcador
final de publicación no cambia. Las relecturas, el shuffle, la enumeración de
archivos y las dependencias contextuales mantienen costes no evaluados a escala.

La integración sintética del lector superó una prueba Spark/Delta en 652,42
segundos con 32 filas; comprobó ausencia de escrituras de evidencia y rechazo
de marcador corrupto, tabla publicada o fuente weather alteradas y captura
ausente. La selección del ID antiguo frente a otro más reciente se comprobó
con dobles, y la contención de cada bloqueo se probó con procesos independientes
sin un publicador Spark concurrente. La regresión unitaria seleccionada de
lectores, publicación, captura, identidad y D25 superó 46 casos. No se repitieron
la regresión general ni la resistencia D27 para esta ampliación.

La integración sintética de publicación y recuperación superó una prueba
Spark/Delta en 713,53 segundos, con 32 filas eléctricas. La prueba interrumpió
la finalización tras un commit real sintético, rechazó alteraciones físicas de
weather y captura, recuperó sin otro commit y rechazó la sustitución física de
la tabla final. Las pruebas unitarias de publicación y regresiones relevantes
superaron 51 casos; otra selección sintética de identidad, asociación y calidad
superó 23. Las reservas parciales y la contención cooperativa se probaron con
dobles o en un mismo proceso, no como concurrencia multiproceso. No se ejecutaron
la regresión general, la resistencia D27 ni la campaña de escala para este cambio.

Una campaña posterior puede escalonar 128, 10.000, 100.000, 280.704 y
1.000.000 de filas eléctricas inventadas. Las 280.704 representan únicamente
16 edificios por 17.544 etiquetas nominales; no acreditan cobertura real. La
campaña debe incluir también el ancho y volumen de los tres orígenes Bronze,
Silver eléctrica, IQR y contexto, no solo la tabla de salida. Para cada escala
se proponen límites acordados antes de ejecutar, medición de tiempo, memoria
del conductor y trabajadores, disco temporal, shuffle y archivos, y parada ante
presión de recursos, falta de espacio, errores o incumplimiento de límites. La
validación real requiere autorización y rutas revisadas por separado.

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
