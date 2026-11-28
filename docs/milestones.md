# Plan de hitos técnicos y seguimiento en GitHub

## Finalidad y situación de partida

Este documento es la **referencia principal del estado de los hitos H01–H12** y
relaciona los objetivos específicos OE1–OE7 con entregables verificables. Las
guías específicas mantienen los contratos y procedimientos; el README resume el
estado y enlaza aquí. Los expedientes privados identifican cada ejecución y no
se publican. Este registro no sustituye la aceptación académica ni el calendario
de entregas acordado por el equipo.

**Antecedente de planificación:** la situación inicial se describía en el [`README.md`](../README.md) y en la
[`guía de los hitos 1 y 2`](hitos-1-2.md): entorno local Spark/Delta verificado
y capa Bronze implementada con pruebas de software sobre datos sintéticos. Los
hitos posteriores se propusieron como planificados. Esa situación se conserva
como antecedente, no como descripción actual de H03. El contraste con la
caracterización preliminar de la memoria continúa pendiente.

## Catálogo de hitos

Los títulos de la segunda columna son los propuestos para los *milestones* de
GitHub. Su creación o cierre en GitHub no acredita por sí sola la finalización
técnica de un hito.

| ID | Título del milestone en GitHub | Resultado esperado | Objetivos del TFM | Dependencia |
| --- | --- | --- | --- | --- |
| H01 | `H01 - Entorno local Spark y Delta` | Entorno reproducible y comprobación efectiva de escritura y lectura Delta | OE2, OE3 | Ninguna |
| H02 | `H02 - Ingesta Bronze y trazabilidad` | Conservación de fuentes, snapshots Delta e inspección de versiones publicadas | OE2, OE3, OE6 | H01 |
| H03 | `H03 - Caracterización de las fuentes BDG2` | Procedencia, estructura, cobertura e incidencias verificadas sobre fuentes locales | OE1 | H02 para comparar con Bronze |
| H04 | `H04 - Selección del caso y contratos analíticos` | Selección justificada y contratos de Silver, calidad e integración aprobados | OE1, OE2, OE4 | H03 |
| H05 | `H05 - Silver de electricidad y calidad` | Electricidad en formato largo, trazable y con indicadores de calidad | OE3, OE4 | H04 |
| H06 | `H06 - Integración de metadatos y meteorología` | Enriquecimiento contextual validado y asociación meteorológica justificada, si procede | OE3, OE4 | H05 |
| H07 | `H07 - Indicadores Gold y comparativas` | Agregaciones por periodo, cobertura e indicadores normalizados válidos | OE3, OE5 | H06 o decisión sin meteorología |
| H08 | `H08 - Consultas y visualización analítica` | Consultas y visualizaciones locales para tareas de análisis definidas | OE5, OE7 | H07 |
| H09 | `H09 - Integridad y recuperación integral` | Consistencia entre capas, reejecución y recuperación ante fallos | OE3, OE6 | H05–H08 |
| H10 | `H10 - Evaluación técnica controlada` | Medidas reproducibles de rendimiento y comportamiento ante distintos volúmenes | OE6 | H09 |
| H11 | `H11 - Validación funcional y analítica` | Indicadores contrastados y evaluación de tareas representativas | OE4, OE5, OE7 | H08–H10 |
| H12 | `H12 - Reproducibilidad y cierre del TFM` | Documentación, evidencias, limitaciones y revisión final coherentes | OE1–OE7 | H11 |

## Estado actual y nivel de evidencia

Esta tabla resume el árbol local. Las verificaciones citadas son evidencia
histórica salvo que se indique una ejecución posterior; el corte documental
anterior correspondía al 09/11/2026.

| Hito | Implementación y evidencia disponible | Pendiente |
| --- | --- | --- |
| H01 | Entorno local y escritura/lectura Delta sintética | Revisar cierre en seguimiento; aceptación residual AUD-09 |
| H02 | Bronze con versiones y recuperación. Igualdad exacta verificada entre los CSV locales y el snapshot real de electricidad, metadatos y meteorología | Mantener trazabilidad y controles de integridad en el consumo |
| H03 | Caracterización eléctrica y meteorológica; ejecuciones reales del 30/10/2026. Evolución eléctrica v2 probada solo con sintéticos | Cierre académico global y contraste con la memoria |
| H04 | Selección reproducida desde los CSV locales con los mismos 16 edificios y el mismo orden | Contraste con el libro de trabajo, procedencia externa y cierre formal |
| H05 | Transformación, publicación Silver e IQR y orquestación verificadas con sintéticos; contraste sintético con pandas | Validación real separadamente autorizada y cierre formal |
| H06 | Resistencia y regresión segmentada superadas en su alcance original. Verificación sintética de meteorología, escalabilidad Bronze y Silver eléctrica hasta 10.000 posiciones. Auditoría real de fuentes y Bronze completada | Validación progresiva de Silver, IQR, contexto y meteorología. La verificación integrada de las ampliaciones sigue pendiente. Las pruebas sintéticas preventivas restantes se aplazan |
| H07 | Planificado; sin Gold implementado | Fórmulas, cobertura y comparabilidad |
| H08 | Planificado; sin explotación implementada | Tareas y visualizaciones analíticas |
| H09 | Planificado; sin recuperación integral entre capas | Diseño y pruebas integrales |
| H10 | Planificado; sin benchmark validado | Protocolo y mediciones reproducibles |
| H11 | Planificado; sin validación funcional y analítica global | Contraste y evaluación de tareas |
| H12 | Planificado; sin cierre del TFM | Reproducibilidad y revisión académica final |

La implementación y la prueba sintética no conceden aceptación académica ni
autorización para BDG2 real. Los contratos y evidencias se mantienen en las guías
enlazadas en el catálogo. Las definiciones de `github-milestones.json` son
plantillas de planificación, no otro registro del estado actual.

### Protocolo progresivo sintético D45

La secuencia aprobada parte de una calibración de ocho filas eléctricas anchas
con 16 edificios y 128 posiciones equivalentes, y una
ejecución de 10.000 y de 100.000 posiciones. Si cada escenario requerido termina
correctamente, siguen tres ejecuciones de 280.704 posiciones nominales; el escenario
de un millón es opcional. La anchura Bronze sintética se varía por separado
entre 16, 128, 512 y 1.600 columnas, sin ejecutar automáticamente todas las
combinaciones. Las 280.704 posiciones son 16 edificios por 17.544 etiquetas
nominales, no un recuento real comprobado.

Las medidas de componentes, la verificación integral de escala y las dos
puertas D27 —regresión funcional segmentada y resistencia independiente— son
actividades distintas. La aprobación del protocolo no autoriza por sí sola una
campaña ni acredita escalabilidad. El supervisor cuenta con verificación
operativa sintética para las cargas registradas.

La primera implementación D44 evaluaba umbrales de forma aislada. Se probó
con 20 casos de medidas inventadas, incluidos límites y datos inválidos.

Se han añadido primitivas parciales para comprobar la autorización D39 en el
proceso actual, rechazar un `basetemp` ajeno o ya existente y reservar una
carpeta de evidencia sin sobrescritura. También se puede leer la identidad de
un proceso desde procfs. El envío de señales está bloqueado hasta disponer de
un lanzador que pruebe la pertenencia a la carga. La reserva aún no garantiza
durabilidad ante un fallo eléctrico; ninguna de estas piezas supervisa una
ejecución ni sustituye el preflight completo. Las 24 pruebas enfocadas usan
medidas y rutas sintéticas y no acreditan las garantías operacionales D44.

La verificación inicial del lanzador parcial superó 28 casos sintéticos,
incluidas reclamaciones concurrentes sobre una campaña D39. El hijo
reautorizaba D39 y contrastaba la configuración por un canal heredado.
Quedaban pendientes la comprobación de procedencia del lanzador, el preflight
integral y el control de recursos y terminación.

**Lanzador restringido D44 (27/11/2026).** En una campaña D39 autorizada,
`launch_workload(campaign, basetemp, run_id, handler="synthetic_probe_v1",
parameters=ProbeParameters("fixture.txt"))` ejecuta una sonda registrada. Si se
indica un fichero, debe ser un `.txt` sintético de hasta 64 KiB ya presente en
`raw/`. Ambos procesos verifican su huella y el padre vuelve a comprobarla
antes de registrar el resultado. `launch_probe` conserva
el uso sin fichero. Los destinos y las raíces excluidas se determinan en el
programa. El reclamo exclusivo se conserva fuera del `basetemp`, que debe
seguir sin existir durante el protocolo.

Antes de ejecutar la sonda, el hijo reautoriza la campaña y contrasta la
configuración, el reclamo y el permiso recibido. La comprobación vincula el
canal heredado, el recibo que escribe el hijo, PID, tiempo de inicio, grupo,
sesión y ejecutable. Si el sistema admite `pidfd`, se usa durante ese
contraste. El modelo es cooperativo entre procesos del mismo usuario.
La evidencia registra el intento antes de arrancar y distingue el resultado
completo de los fallos. El marcador sincronizado tras recoger al hijo fija
la finalización. Un error posterior al cerrar el directorio se registra como
aviso. Tras un fallo se cierra el canal y se espera al hijo con
plazo. Si sigue activo al vencer el plazo, se registra que aún no se ha recogido
y se continúa la espera en segundo plano mientras viva el padre. El lanzador
conserva los expedientes y carece de una vía de señalización.

Las 52 pruebas enfocadas de esta versión se superaron en 3,39 s. Incluyen
sondas en una campaña D39 y fallos inyectados de identidad, autorización e
inicialización.

**Muestreo de recursos D44 (27/11/2026).** `launch_workload` y `launch_probe`
devuelven el resultado de la sonda y el resumen `resources`. Cada ejecución
conserva `resource-samples.jsonl`, `resource-storage.jsonl` y
`resource-summary.json` en su carpeta de evidencia. Las cadencias durante la
ejecución son de 1 s para procesos y memoria y 5 s para almacenamiento.
Las observaciones iniciales y finales cubren también sondas más cortas. Cada
pasada de almacenamiento conserva su propia clasificación.
La duración de la operación va del arranque al resultado recibido. La de
la campaña se cuenta desde el primer uso del muestreador y se conserva entre
ejecuciones en el mismo dominio de reloj monotónico.

La suma de RSS y los contadores de CPU se refieren a los procesos verificados
en la sesión. Los deltas de CPU se calculan solo entre medidas de una misma
instancia. Se conserva la identidad de cada descendiente observado y, cuando
está disponible, su `pidfd`. Un cambio de grupo dentro de la sesión queda
registrado. La pérdida de identidad o la salida de la sesión exige parada y
excluye esos recursos del cálculo. Un descendiente que abandone la sesión y
pierda su parentesco observable entre dos muestras puede quedar fuera del
registro.

La memoria y el intercambio del sistema son medidas compartidas.
Cada cgroup visible se compara con su propio límite, incluidos sus ancestros.
La ocupación de la campaña suma los bloques asignados y cuenta los ficheros
regulares. La de `/tmp` corresponde al sistema de archivos que lo contiene.
La disponibilidad se obtiene con `statvfs`, sin enumerar `/tmp`.

El conductor consulta el estado del muestreador sin E/S mientras espera el
resultado. La inicialización y el cierre de los recolectores tienen un
segundo de espera cada uno. Las operaciones de evidencia tras el arranque
tienen un segundo por operación. Cada uno de los tres tipos de trabajo
admite dos hilos por proceso conductor, sin cola de espera. Un hilo que
permanece bloqueado conserva su plaza y sus descriptores hasta salir.

El evaluador conserva los avisos, las paradas y los máximos observados.
`resources.supervision_status` distingue el estado del muestreo del resultado
de la sonda. `deadline_misses` y `unfinished_io` detallan las operaciones
vencidas o pendientes. Los JSONL conservan las líneas completas anteriores a
un fallo. Si la llamada devuelve un error de confirmación, los bytes visibles
de una escritura iniciada pueden corresponder a una persistencia pendiente.
Si coexisten los dos marcadores de lanzamiento, prevalece `failure.json`
sobre `completed.json`. `launch_workload` y `launch_probe` conservan el modo
de observación. El punto de entrada que aplica las paradas D44 es
`launch_supervised_workload`, con la opción A de D46. La sonda Spark local
registrada usa el mismo supervisor y los JAR ya disponibles en D39.
Su alcance y verificación figuran en la [guía de H06](hitos-6.md#integración-de-spark-con-el-supervisor-de-recursos).

### Espacio sintético aislado D39 — 26/11/2026

`reserve_campaign(id, owner)` en `tfm_lakehouse.scalability_workspace` reserva
una sola vez un directorio bajo `data/lakehouse/synthetic/scalability/`, tras
comprobar exclusión de Git y sistema de archivos. `authorize_campaign(id, owner)`
habilita en el proceso una reserva completa existente. Los identificadores son
declaraciones de propiedad, no credenciales frente a otros procesos. La reserva
conserva los restos de una inicialización interrumpida y no los adopta ni elimina.

Cada directorio contiene `raw`, `lakehouse`, espacios independientes de Silver,
IQR, contexto y meteorología, `spark-local`, `spark-conf`, `temp`, `ivy` y
`evidence`. Las configuraciones sintéticas deben usar `raw` y `lakehouse` de la
misma campaña. Las pruebas pequeñas bajo `/tmp` siguen admitidas. Antes de usar
una campaña se vuelven a comprobar su propiedad, sus rutas y la exclusión Git;
la comprobación del montaje admite `ext4`, `xfs` y `btrfs` y rechaza otros tipos.

Para una futura sesión, `spark_session(campaign=campaign)` necesita una JVM nueva
y los tres JAR Delta/ANTLR ya disponibles en `ivy/jars` de esa campaña; no los
descarga. Sitúa los temporales de Python, JVM y Spark en el espacio reservado y
comprueba los valores efectivos antes de entregar la sesión. Los recursos deben
permanecer estables: las comprobaciones no ofrecen aislamiento frente a procesos
que sustituyan rutas o montajes durante la ejecución, ni equivalen a un límite de
recursos. La sesión no autoriza por sí sola el uso de fuentes reales.

Se reservó y verificó un espacio de prueba en el destino autorizado, montado en
`ext4`. La ejecución enfocada del 26/11/2026 superó 28 casos de reserva,
aislamiento y rechazo de sesiones o rutas ajenas, y la prueba
`test_local_spark_uses_campaign_temporary_storage` superó un arranque Spark
local mínimo con JAR ya presentes y datos inventados. No se ejecutó una campaña
de escala. D40,
D41, los dos controles de integración D27 de las ampliaciones y la validación
con BDG2 real permanecen pendientes.

La decisión D43 autoriza una recuperación excepcional de publicaciones v2
históricas incompletas únicamente con autorización para la publicación concreta
y evidencia independiente verificable de su existencia anterior a D42, vinculada
a la intención exacta. Sin esa evidencia, la recuperación se rechaza. Esta
condición matiza la recuperación histórica de estados incompletos; la lectura
verificada de publicaciones terminadas no requiere conservar la captura.

Durante la verificación anterior se estableció brevemente `TFM_REAL_CONFIG` en
una prueba negativa y se usó el directorio temporal predeterminado de pytest; la
primera acción se sustituyó por un espía. Las ejecuciones posteriores deben
comprobar que la variable esté ausente y que el directorio base, bajo un padre
autorizado, sea nuevo y no exista ni como enlace, antes de iniciar pytest.
Una revisión posterior ejecutó `git ls-tree`, consulta de lectura no incluida
entre las autorizadas; se interrumpió esa línea de revisión sin escribir en Git.
En otra repetición se comprobó por error una ruta sintética distinta de la que
utilizaba el ejecutor. La ejecución afectada terminó con un error de sockets
antes de iniciar Spark y no constituye evidencia de verificación.
También se interrumpió preventivamente una regresión meteorológica al detectar
que su fase final eliminaba archivos sintéticos de la prueba. La interrupción
ocurrió antes de esa fase; el error de Py4J producido por ella no es resultado
de la regresión. Se preparó una comprobación alternativa sin esos borrados.
La primera ejecución de esa alternativa falló al importar la asociación
meteorológica en un trabajador Python; se corrigió el ciclo de importación y se
añadió una comprobación en un intérprete nuevo. La repetición de la integración
sin borrados superó la captura, publicación y lectura meteorológicas sintéticas
en 611,26 segundos; ese tiempo no constituye una medición de escala.
En una comprobación D39 posterior se imprimió la raíz de campaña en el preflight,
pero se validó después de iniciar Spark. Se interrumpió esa ejecución durante la
ingesta sintética y se conservó la reserva parcial; el error asociado a la
interrupción no es un resultado funcional. La repetición reservó y validó una
campaña nueva antes de iniciar Spark y superó la publicación y lectura
contextuales, además de la huella tras repartición y `unpersist()`, en 148,53
segundos. Usó el perfil Spark sintético habitual, no el perfil temporal D39.
Una prueba multiproceso posterior volvió a ejecutar por error el lanzador pytest
en un hijo y reutilizó el mismo directorio base. Esa ejecución falló y pytest
pudo limpiar ese directorio sintético; el hijo se sustituyó por un proceso que
no invoca pytest.

La alternativa D43 seleccionada limita la evidencia histórica a expedientes de
reservas observadas en el árbol Git fijado anterior a D42. El expediente con el
formato previsto no está presente en ese árbol: las pruebas con datos inventados
y un lector sustituido no acreditan una recuperación histórica real operativa.

El borrador de D40 materializa filas contextuales y evidencia de zona en una
captura Delta independiente, con identidad física y huella de árbol. La
publicación v2 conserva su intención y marcador; un vínculo externo comprueba
la captura durante la recuperación, y el lector de una publicación terminada
no depende de conservarla. El orden de bloqueo es publicación y después captura.
Los bloqueos contextuales de lectura y escritura intentan liberar y cerrar
por separado sin sustituir una excepción primaria por un fallo de limpieza.

La interfaz excepcional `recover_legacy_synthetic_contextual` exige el ID y
hash exacto autorizados, una justificación y una captura identificada. Solo
admite un expediente de reserva observada en
`docs/h06-contextual-observed-reservations/<id>.json` del commit anterior a
D42 `0e979b2b5ae161db7d60556363665616182df9c0`. El expediente debe vincular
el identificador y las huellas de la intención canónica y de su archivo de
reserva, sin publicar la intención completa. Las huellas no garantizan el
anonimato si el contenido puede adivinarse; no admite una declaración
actual o una fecha de archivo como prueba de antigüedad. El anclaje acredita el
contenido bajo la confianza en la historia Git del proyecto, sin autenticar por
sí mismo el origen externo de los datos. La
falta de captura o vínculo en una publicación inconclusa se rechaza por defecto.
Dos integraciones sintéticas recuperaron una reserva sin tabla y otra con una
tabla comprometida sin añadir otro commit; en ambas se simuló el lector del
expediente Git. Tras reforzar la huella de la tabla histórica, ambos casos se
repitieron y una alteración inyectada antes del marcador impidió finalizar.
Una prueba integrada de la captura
contrastó la intención y la huella v2, rechazó una alteración física y leyó la
publicación tras retirar la
captura sintética. Una sustitución por otra tabla Delta sintética también fue
rechazada, conservando ambas tablas. Otra prueba conservó la evidencia TZif de
`UTC`. Una regresión
rechazó una modificación de metadatos durante la materialización; también se
leyó una publicación sintética creada por el publicador anterior a D42 y se
comprobó la independencia de IQR. La validación
de una recuperación con evidencia histórica real
y el conjunto completo de regresiones siguen pendientes. Esta interfaz no
autoriza procesar datos reales.

## Evolución histórica

### Situación y nivel de evidencia — 31/10/2026

La tabla conserva la situación histórica del 31/10/2026. Las novedades posteriores
se conservan en las actualizaciones fechadas siguientes.

Implementación, pruebas, ejecución real, auditoría y aceptación académica son
dimensiones distintas. Una aceptación técnica no acredita la aprobación global
del hito ni una nueva ejecución de una revisión posterior del código.

| Hito | Diseño e implementación | Pruebas y contraste técnico | BDG2 real | Decisiones y aceptación académica |
| --- | --- | --- | --- | --- |
| H01 | Implementado | Persistencia Delta con registros sintéticos; auditorías de entorno, Spark e Ivy | No es una validación BDG2 | No acredita resultados analíticos ni aceptación académica global |
| H02 | Bronze implementado | Regresiones sintéticas; correcciones AUD-01/AUD-02/AUD-05/AUD-08 | Expediente histórico `data/checks/bronze-hardening/`; sin reingesta en estas correcciones | No acredita calidad energética ni aceptación académica global |
| H03 | Subentregables técnicos disponibles | Evidencia desglosada a continuación | Ejecuciones históricas identificadas | **Cierre académico global pendiente** |
| Verificación de procedencia | Verificación de procedencia, variantes y SHA-256 documentada | Huellas contrastadas, recogidas en configuraciones y expedientes H03 | Identidad de los originales recibidos; sin inventar una ejecución independiente adicional | Aceptación global de fuentes y contraste con la memoria pendientes |
| Caracterización eléctrica y de metadatos (expediente histórico v1) | Caracterización eléctrica y de metadatos implementada | Pruebas y contraste independiente; cierre técnico con observaciones aceptado el 30/10/2026 | `2026-10-30-h03-2`, contrato `h03.2-v1` | Selección final, exclusiones y comparabilidad pendientes |
| Evolución técnica de la caracterización eléctrica (v2) | AUD-03/AUD-04 implementados: vinculación de procedencia y publicación exclusiva | Validado mediante pruebas sintéticas y regresiones | **No ejecutado nuevamente sobre BDG2 real** | No atribuir al expediente v1 las garantías automáticas v2 |
| Caracterización meteorológica y temporal | Meteorología y diagnóstico temporal implementados | Pruebas sintéticas y auditoría independiente con observaciones aceptada | `2026-10-30-h03-3`, contrato histórico `h03.3-v1` | Política temporal, integración y selección definitiva pendientes |
| H04 | Diseñado en su guía; no iniciado | Pendiente | No ejecutado | Selección del caso y contratos por aprobar |
| H05 | Planificado; no implementado | Pendiente | No ejecutado | Depende de H04 |
| H06 | Planificado; no implementado | Pendiente | No ejecutado | Integración temporal por justificar |
| H07 | Planificado; no implementado | Pendiente | No ejecutado | Fórmulas y comparabilidad por aprobar |
| H08 | Planificado; no implementado | Pendiente | No ejecutado | Tareas analíticas por definir |
| H09 | Planificado; no iniciado | Pendiente | No ejecutado | Recuperación entre capas por diseñar |
| H10 | Planificado; sin benchmark validado | Pendiente | No ejecutado | Protocolo de evaluación por aprobar |
| H11 | Planificado; no iniciado | Pendiente | No ejecutado | Validación funcional y analítica pendiente |
| H12 | Planificado; no iniciado | Pendiente | No ejecutado | Cierre del TFM pendiente |

### Actualización documental — 06/11/2026 (antecedente)

La transformación eléctrica a formato largo está implementada y versionada como entrega técnica preliminar. La [guía H05](hitos-5.md#verificación-sintética-y-límites)
registra 498 pruebas superadas el 01/11/2026, con datos sintéticos; esta revisión
documental no vuelve a ejecutarlas. No interpreta números o tiempos ni persiste
Silver, y no se ha validado con BDG2 real. H04 sigue pendiente de aprobación
conjunta; H05 formal, la interpretación numérica y temporal, H06 y Gold continúan pendientes.

La [guía de caracterización eléctrica](h03-2-characterization.md) y la
[guía de caracterización meteorológica](h03-3-weather-timezones.md) describen métodos y límites. Sus
`execution.json` privados identifican fecha, revisión y hashes originales;
no se regeneran para actualizar este registro. Los cambios de comentarios,
pruebas o contratos posteriores no se consideran nuevas ejecuciones reales.

La evolución conserva el plan inicial, las ejecuciones del 30/10/2026 y las
correcciones posteriores como hechos distintos. El resultado histórico de
AUD-05/AUD-08 —449 pruebas superadas y tres excluidas— se conserva en la
[guía H01–H02](hitos-1-2.md); no es un resultado atribuido a AUD-06/AUD-07.

### Actualización documental — 07/11/2026 (antecedente)

**Nota posterior:** la guía H04 registra la ratificación conjunta comunicada de
los 16 identificadores. Las afirmaciones de esta actualización sobre una
aprobación pendiente reflejan el estado anterior a esa confirmación; H04 sigue
sin cerrarse. La selección ya se reprodujo con los CSV locales y quedan
pendientes el contraste con el libro y la procedencia externa.

**Actualización de integración eléctrica en Silver:** se ha incorporado la composición de las 16 columnas
aprobadas con las etapas de transformación a formato largo e interpretación numérica y temporal, sin lectura automática de snapshots ni
publicación Silver. El estado y la validación sintética constan en la guía H05;
el procesamiento real y el cierre formal siguen pendientes.

**Actualización del vínculo con la publicación Bronze:** el vínculo con una publicación Bronze identificada se
ha incorporado para configuraciones sintéticas. La guía H05 detalla los controles
y sus límites; no acredita procedencia externa ni autoriza procesamiento real.

**Actualización de publicación sintética en Silver:** se ha incorporado una publicación Silver Delta limitada
a directorios temporales sintéticos, con validación de contenido y recuperación
conservadora. La guía H05 recoge sus límites; no existe publicación Silver real.

H01 y H02 están implementados; las evidencias de H01 corresponden a Delta con
datos sintéticos y las de H02 incluyen regresiones sintéticas y expedientes
históricos de procedencia. No se infiere de esas pruebas calidad analítica ni una
nueva ejecución sobre BDG2. H03 dispone de caracterización y ejecuciones reales
históricas identificadas; la evolución técnica de la caracterización eléctrica conserva validación sintética, sin nueva
ejecución real, y el cierre académico global continúa pendiente.

H04 documenta el embudo y un conjunto final de 16 edificios seleccionados.
Según la confirmación metodológica comunicada por el autor informante el
07/11/2026, los límites de superficie son inclusivos
(`3.500 ≤ area_m2 ≤ 6.000`); se excluyen los tres
edificios de subcategoría `Hotel` y la regla de ausencia es estrictamente superior
al 8 %. Esta confirmación no equivale a ratificación conjunta. Siguen pendientes
la aprobación conjunta de la selección de edificios y la reproducción
verificable de la selección y procedencia desde las fuentes originales.

La transformación eléctrica a formato largo está implementada y validada con
datos sintéticos. La etapa siguiente añade la interpretación numérica y temporal
nominal, con validación sintética obligatoria completada: la evidencia histórica
registra 127 pruebas H05 superadas y una prueba del lector que escribe Delta
excluida. Esos resultados no son ejecuciones nuevas de esta actualización. No se
ha autorizado la validación Silver con BDG2 real ni la publicación de tablas
Silver en Delta. H05 formal sigue pendiente; integración meteorológica y Gold
permanecen fuera del alcance implementado.

Esta actualización describía la situación de su fecha sin reescribir la tabla histórica
del 31/10, la actualización del 06/11 ni los resultados H03/H05 previamente
registrados. La revisión documental no ejecuta pruebas ni procesos de datos.

Los estados reflejan la evidencia disponible en el repositorio, **no** el
estado de GitHub. El cierre de H01 y H02 deberá revisarse antes de darlos por
completados en la herramienta de seguimiento.

### Motor estadístico IQR — H05

El incremento autorizado incorpora un motor sin persistencia para las 16 series
eléctricas, con cuartiles lineales exactos, señales conservativas y controles
separados de cobertura de observaciones y horas nominales. El contrato y la
verificación sintética se recogen en la [guía H05](hitos-5.md#motor-estadístico-iqr-y-controles-de-aceptación--h05).
En ese incremento quedaron pendientes la persistencia IQR versionada y su
vinculación con Silver, implementadas después según la actualización siguiente.
No constituye validación con BDG2 real ni cierre formal de H05.

### Publicación versionada IQR — H05 — 09/11/2026

El incremento añade publicación IQR sintética independiente, verificación del
padre Silver, replay, recuperación controlada y lectura histórica compatible.
La recencia utiliza el timestamp persistido del commit Delta verificado, según
el criterio aprobado; no la fecha de finalización o de modificación de archivos.
El contrato y las comprobaciones se recogen en la [guía H05](hitos-5.md#publicación-versionada-iqr--h05).
En ese incremento quedaron pendientes la orquestación, la ejecución BDG2 real
y el cierre formal de H05.

## Riesgo técnico transversal AUD-09

`AUD-09 — Evaluado; tratamiento propuesto; aceptación del riesgo residual pendiente.`

El [registro técnico AUD-09](hitos-1-2.md#aud-09-riesgo-residual-de-dependencias-jvm)
documenta las versiones JVM afectadas, la comprobación estática complementaria,
las restricciones del perfil local CSV/Delta y los disparadores de reevaluación.
La propuesta es mantener provisionalmente ese perfil, sujeta a decisión expresa
de Carlos e Irantzu. Este estado no equivale a cierre ni aceptación del riesgo,
no modifica la aceptación académica de H03 y, por sí solo, no autorizó avanzar a H04.
Las pruebas funcionales históricas permanecen separadas de esta revisión estática.

## Secuencia principal

```mermaid
flowchart TD
    A["H01–H02: Entorno y Bronze"] --> B["H03: Caracterización BDG2"]
    B --> C["H04: Caso y contratos"]
    C --> D["H05: Electricidad Silver"]
    D --> E["H06: Integración contextual"]
    E --> F["H07: Indicadores Gold"]
    F --> G["H08: Consultas y visualización"]
    G --> H["H09: Integridad integral"]
    H --> I["H10: Evaluación técnica"]
    I --> J["H11: Validación analítica"]
    J --> K["H12: Cierre del TFM"]
```

El diagrama representa las principales condiciones de avance. Las pruebas,
la revisión de resultados y la documentación se realizan durante todo el
proyecto. Algunas inspecciones de solo lectura pueden solaparse. La inclusión
de una actividad en esta planificación no autoriza automáticamente su ejecución.

## Guías detalladas

| Hito | Documento |
| --- | --- |
| H01–H02 | [`hitos-1-2.md`](hitos-1-2.md) |
| H03 | [`hitos-3.md`](hitos-3.md) |
| H04 | [`hitos-4.md`](hitos-4.md) |
| H05 | [`hitos-5.md`](hitos-5.md) |
| H06 | [`hitos-6.md`](hitos-6.md) |
| H07 | [`hitos-7.md`](hitos-7.md) |
| H08 | [`hitos-8.md`](hitos-8.md) |
| H09 | [`hitos-9.md`](hitos-9.md) |
| H10 | [`hitos-10.md`](hitos-10.md) |
| H11 | [`hitos-11.md`](hitos-11.md) |
| H12 | [`hitos-12.md`](hitos-12.md) |

## Responsabilidades

- **Carlos:** arquitectura Spark/Delta, procesos, persistencia, trazabilidad,
  reproducibilidad, recuperación y evaluación técnica.
- **Irantzu:** caracterización BDG2, selección del caso, modelo analítico,
  reglas de calidad, integración contextual, indicadores y validación analítica.
- **Ambos:** aprobación de contratos entre capas, revisión cruzada,
  interpretación de resultados y validación de la memoria.

Estas atribuciones son ámbitos de responsabilidad y no equivalen a asignaciones
automáticas a cuentas de GitHub.

## Condiciones generales para cerrar un hito

1. Alcance, dependencias, responsables, contratos y criterios de aceptación
   aprobados antes de implementar cambios sustanciales.
2. Documentación pública y prosa académica en español de España. Código,
   identificadores, nombres de pruebas, docstrings y comentarios técnicos
   necesarios en inglés, conservando comandos y contratos.
3. CSV originales BDG2 inalterados y excluidos de Git; entradas y salidas
   separadas. Las pruebas sintéticas no acreditan conclusiones energéticas.
4. Pruebas pertinentes realmente ejecutadas, con comando, revisión de código,
   entorno, resultados y comprobaciones no realizadas.
5. Estado de la evidencia declarado con precisión: **diseñado**,
   **implementado**, **probado con datos sintéticos**,
   **validado con BDG2 real** o **medido**.
6. Anomalías, registros excluidos, versiones, linaje, recuperación y límites
   de comparabilidad documentados cuando correspondan.
7. Revisión cruzada de decisiones analíticas y afirmaciones de la memoria.
   Los mensajes de cambios siguen Conventional Commits. El versionado y la
   publicación del repositorio corresponden a los autores.

## Convención de nomenclatura

Los únicos identificadores de hito son H01–H12. Las actividades de cada hito se
identifican mediante títulos descriptivos, sin códigos jerárquicos nuevos. Los
códigos históricos o técnicos solo se conservan cuando forman parte de una
referencia reproducible y se explicita su función.

## Uso de milestones e issues en GitHub

Crear un *milestone* por resultado técnico aceptado y dividir su trabajo en
*issues* pequeños y verificables. Los títulos recomendados figuran en la tabla
anterior y las tareas sugeridas, en cada guía de hito.

El archivo [`github-milestones.json`](github-milestones.json) contiene 12
objetos independientes con `title`, `state` y `description`, preparados para
una futura creación mediante la API de GitHub. **No crea hitos por sí mismo**;
antes de utilizarlo es necesario revisar los milestones existentes para evitar
duplicados. Tampoco debe enviarse el array completo como si fuera un único
milestone.

Recomendaciones de seguimiento:

- Mantener una única responsabilidad clara por issue y revisar las
  dependencias antes de iniciarlo.
- Incorporar el prefijo `[H03]`, `[H04]`, etc. al título de las tareas.
- No establecer fechas ficticias: deben aprobarse según el calendario real
  de entregas académicas.
- Cerrar cada issue con sus pruebas y evidencias; cerrar un milestone
  únicamente cuando se hayan satisfecho todos los criterios obligatorios.
- Registrar por separado los bloqueos que necesiten una decisión conjunta.

Las definiciones incluidas aquí **no significan que ya existan milestones
o issues creados en GitHub**.

## Siguiente actividad recomendada — antecedente de planificación

**`[H03] Contrastar las evidencias técnicas con la memoria y resolver las decisiones abiertas`**.

La primera verificación de rutas y huellas era la recomendación del plan inicial
y ya tiene evidencia. No se propone repetirla como si estuviera sin iniciar.
Quedaban por aceptar globalmente las fuentes, justificar la selección y las
exclusiones y acordar la política temporal antes de avanzar a H04. La situación
actual de H04 y H05 se recoge en la actualización del 07/11/2026; esta sección
conserva la recomendación de planificación original. Los criterios se detallan en
[`hitos-3.md`](hitos-3.md).
