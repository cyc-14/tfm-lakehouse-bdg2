# Hito 5 — Silver de electricidad y controles de calidad

**Estado:** implementados y verificados con sintéticos la transformación,
interpretación numérica y temporal, vínculo Bronze, publicación Silver,
diagnóstico de calidad, motor IQR y publicación IQR versionada. La orquestación
Silver → IQR, la validación real y el cierre formal siguen pendientes; el
[registro de hitos](milestones.md) mantiene el resumen actual.
**Responsabilidad:** Carlos implementa el flujo;
Irantzu define y valida las reglas analíticas. **Objetivos:** OE3, OE4.
**Requisito previo para H05 formal:** contratos aceptados en H04.

## Finalidad

Implementar un flujo mínimo desde una **publicación Bronze coherente** hasta
electricidad Silver en formato largo, con trazabilidad y anotaciones de
calidad explícitas, mediante los componentes y contratos descritos a continuación.

## Plan inicial de actividades

La lista y los issues iniciales describen la descomposición del trabajo, no su
estado actual. Las casillas de aceptación global no certifican el cierre formal.

1. Definir la configuración de Silver y consumir las versiones Bronze
   seleccionadas mediante `read_bronze_snapshot(...)`.
2. Transformar a formato largo únicamente las columnas eléctricas aprobadas,
   manteniendo identificadores, granularidad y multiplicidad justificadas.
3. Interpretar fechas y números conforme a H04, preservando texto original,
   fallos de conversión, procedencia y ambigüedades horarias.
4. Aplicar indicadores de ausencia, cero, negativo, duplicidad o valor
   extremo según las reglas aprobadas, sin confundir incidencias con errores.
5. Persistir y releer Delta Silver con pruebas de funcionamiento,
   reejecución, fallo y conciliación de recuentos.

## Entregables previstos

- Módulos de procesamiento y calidad únicamente cuando exista código
  probado que alojar, sin estructuras vacías anticipadas.
- Salidas Delta Silver versionadas y separadas de `raw` y Bronze.
- Pruebas sintéticas de casos válidos, ausentes, malformados, duplicados y
  situaciones temporales límite que correspondan al contrato.

## Criterios de aceptación

- [ ] Las filas resultantes respetan granularidad, claves, tipos y
      procedencia; Bronze y los CSV originales permanecen inalterados.
- [ ] La conciliación explica multiplicidad, exclusiones y registros
      rechazados o apartados, sin pérdidas silenciosas.
- [ ] Los indicadores de calidad y sus casos límite están probados.
- [ ] Las reejecuciones y escrituras parciales tienen un comportamiento
      documentado; no se atribuye atomicidad entre tablas.
- [ ] Se han ejecutado pruebas pertinentes y se diferencian las realizadas
      con datos sintéticos de cualquier validación real autorizada.

## Issues de GitHub propuestos

- `[H05] Leer publicaciones Bronze coherentes para Silver`
- `[H05] Implementar transformación de electricidad a formato largo`
- `[H05] Interpretar fechas y valores con trazabilidad`
- `[H05] Añadir reglas de calidad y pruebas de casos límite`
- `[H05] Comprobar persistencia, reejecución y recuperación Silver`

## Límites y condición de avance

No incorporar otros combustibles, imputaciones no aprobadas, meteorología,
predicciones ni indicadores Gold. H06 requiere que el flujo eléctrico
mínimo cumpla su contrato y haya superado una revisión técnica.

## Transformación eléctrica a formato largo — H05

Incremento autorizado por separado el 01/11/2026, exclusivamente con datos
sintéticos. En aquel incremento la selección definitiva y los contratos
analíticos seguían pendientes. Su interfaz genérica recibe una lista técnica
de columnas; no sustituye el manifiesto académico de selección H04.

### Interfaces y esquema provisional

El módulo `src/tfm_lakehouse/silver/silver_electricity.py` ofrece:

```python
electricity_to_long(bronze, electricity_columns) -> DataFrame
read_electricity_long(spark, config, electricity_columns) -> (selection, DataFrame)
```

La primera función acepta un DataFrame con la columna exacta `timestamp`, las
cuatro columnas de linaje del [contrato Bronze](hitos-1-2.md) y las columnas
eléctricas solicitadas. Todos los campos de entrada deben ser `StringType`.
La selección debe ser una secuencia explícita no vacía, sin nombres duplicados
y con coincidencia exacta con las columnas recibidas. Se rechazan nombres de
entrada vacíos o ambiguos incluso sin distinguir mayúsculas, campos obligatorios
ausentes, tipos incompatibles y columnas técnicas `_bronze_` desconocidas. Una
evolución del esquema técnico requerirá revisar este contrato; no se descarta
linaje nuevo silenciosamente. `timestamp` y cualquier nombre con el prefijo
reservado `_bronze_` no pueden solicitarse como edificios.

La salida básica tiene estas columnas:

| Campo | Tipo y significado |
| --- | --- |
| `building_id` | Cadena: nombre original de la columna eléctrica solicitada |
| `timestamp_original` | Cadena original recibida en `timestamp`, incluido null |
| `electricity_value_original` | Cadena o null original de la celda eléctrica |
| `_bronze_source_uri` | Procedencia original recibida, sin reinterpretación |
| `_bronze_source_sha256` | Hash recibido de Bronze, sin recalcularlo a partir de celdas |
| `_bronze_data_kind` | Declaración original `real`/`synthetic` |
| `_bronze_dataset_reference` | Referencia original del dataset |

El grano es **una aparición de fila Bronze por columna eléctrica solicitada**.
No se establece clave primaria. Dos apariciones idénticas siguen siendo dos
filas idénticas: Bronze no aporta un ordinal original que permita asignarles
una identidad estable. No se generan índices, hashes de fila ni identificadores
dependientes del particionado para fingir esa identidad.

La función básica conserva el linaje recibido, pero no autentica un DataFrame
arbitrario como publicación Bronze. La segunda interfaz utiliza exclusivamente
`read_bronze_snapshot(...)`, que verifica todas las fuentes configuradas y lee
las versiones publicadas mediante `versionAsOf`. Devuelve su contexto original
`selection` y añade a cada fila eléctrica:

| Campo adicional | Procedencia |
| --- | --- |
| `_bronze_source_name` | Literal `electricity` |
| `_bronze_snapshot_run_id` | `selection["run_id"]` |
| `_bronze_delta_version` | Versión Delta de electricidad seleccionada por el lector, como entero |
| `_bronze_table_path` | Ruta de la tabla eléctrica registrada en la publicación |

La versión se refiere a la tabla eléctrica, no a una versión única compartida
entre tablas. El contexto conserva también las versiones de las demás fuentes,
la referencia del informe y la indicación de recuperación de una publicación
anterior cuando corresponda. El lector existente efectúa sus comprobaciones de
conteo; la transformación posterior sigue siendo una proyección diferida.
No se atribuye atomicidad entre Bronze y Silver. Las rutas del
linaje son información privada si describen una ejecución real; la herramienta
no publica informes ni extrae datos reales para Git.

### Conservación, multiplicidad y ejecución

La expansión conserva una salida por celda solicitada, incluso si es null:

```text
filas largas = apariciones Bronze × columnas eléctricas solicitadas
```

La igualdad se comprueba con datos sintéticos, incluidos nulos y duplicados.
La función básica no ejecuta conteos adicionales ni materializa el conjunto en
el driver. No impone orden físico. Las pruebas comparan multiconjuntos, no
secuencias ordenadas. No hay filtros, agregaciones, joins ni deduplicaciones.

Los nombres de columna se tratan como identificadores literales, sin ejecutar
su contenido como SQL. La transformación genérica admite nombres especiales
que el lector CSV Bronze puede rechazar al persistir Delta; ambos contratos
se prueban por separado.

Se conservan null, `""`, `"NaN"`, códigos especiales, ceros, negativos, extremos
y espacios sin conversión numérica. Una cadena vacía ya interpretada como null
por el lector CSV Bronze no puede reconstruirse: la garantía empieza en el
DataFrame recibido, no en la representación byte a byte del CSV.

La etiqueta temporal permanece como texto, incluidas fechas inválidas, etiquetas
repetidas y ejemplos asociados a cambios DST. No hay conversión UTC, deducción
de zona, resolución de ambigüedades ni asignación de `fold`. No se interpreta
la etiqueta como instante físico ni se atribuyen unidades al valor eléctrico.

El plan usa expresiones Spark, sin UDF de Python ni recogida de datos en el
driver, persistencia o caché. Produce N×K filas; el plan y el array por fila
crecen con K. No se han medido rendimiento, escalabilidad ni límites de anchura.

### Verificación sintética y límites

`tests/silver/test_silver_electricity.py` separa validaciones `unit` sin JVM de pruebas
`integration` con Spark/Delta y datos inventados. La integración crea una
publicación Bronze temporal válida y después una versión eléctrica física
divergente sin publicación completa. Verifica que la interfaz consume la versión
publicada, conserva el linaje y concilia 4×2=8 filas, incluidos los duplicados.
Esta prueba del transformador escribe solo Bronze sintética temporal.

Selección focal: `tests/silver/test_silver_electricity.py`; regresión general:
`-m "not real_data and not performance"`, con `TFM_REAL_CONFIG` ausente. Para
evitar descargas y cambios en Ivy se utiliza el [perfil de JAR locales](hitos-1-2.md#perfil-temporal-sin-resolución-maven),
con copias en un directorio temporal. En este incremento también se asignaron
destinos temporales a Ivy, al almacén Spark y a `SPARK_LOCAL_DIRS`, desactivando
la caché de pytest y la escritura de bytecode. El gestor Spark y Delta son los
reales; solo se sustituye el helper de resolución Maven en el proceso de prueba.

Verificación del 01/11/2026 sobre `main`, base
`ea71de4ab848dcb5b9fa58aba80dd410e9c60aaa`, con estos tres entregables locales.
Entorno sin cambios: Python 3.11.17, PySpark 3.5.7 y Delta 3.3.3.

| Comprobación ejecutada | Resultado observado |
| --- | --- |
| Primer intento focal dentro del sandbox | 39 pruebas unitarias superadas; siete errores de preparación por bloqueo del socket local Py4J, antes de ejecutar sus pruebas Spark |
| Focal con sockets locales permitidos y perfil temporal | 46 superadas: 39 `unit` y siete `integration`; 15,81 s |
| Suite general con el mismo perfil, en proceso nuevo | 498 superadas, una excluida por selección (`real_data`), cero fallos; 47,30 s |
| Selección de rendimiento | Excluida expresamente; no hay pruebas `performance` implementadas |
| Revisión independiente de código y documentación | Dos revisiones de solo lectura, sin defectos bloqueantes identificados |
| Integridad SHA-256 antes/después | Sin cambios en los cuatro archivos de originales/procedencia, 59 de Bronze, 39 de evidencias privadas y 23 de Ivy |

Los errores del primer intento corresponden a `java.net.SocketException:
Operation not permitted`, no a aserciones de la transformación. No motivaron
cambios en el código de Spark. Las duraciones describen estas ejecuciones de
pruebas, no constituyen un benchmark. La suite general incluye las regresiones
de Bronze, Spark, caracterización eléctrica y de metadatos y caracterización meteorológica y temporal. No se ejecutaron ingestas ni caracterizaciones
BDG2 reales, ni se modificaron dependencias o los archivos de Git.

El [perfil sintético canónico](hitos-1-2.md#perfil-temporal-sin-resolución-maven)
centraliza el procedimiento actual; admite la ruta focal como argumento. El
bloque exacto utilizado el 01/11/2026 se conserva en
`7d491f4cca156c05f7a8bba9eaad8f3f7f51c449:docs/hitos-5.md`
(consulta con `git --no-optional-locks show`). El perfil actual incorpora
además aislamiento de `TMPDIR`, desactivación de plugins externos y la exclusión
explícita de la comprobación de política que inspecciona rutas protegidas; no
se atribuyen estas diferencias a las 498 pruebas históricas.

El alcance de aquel transformador excluía: CLI y configuración Silver, selección de edificios reales,
persistencia Silver, reglas de calidad de negocio, interpretación numérica o
temporal, unidades verificadas por fuente, integración meteorológica,
imputación e indicadores Gold. Los componentes siguientes documentan los incrementos sintéticos posteriores;
las decisiones aplicables siguen correspondiendo a H04. No se ha validado con
BDG2 real ni modifica el riesgo residual JVM documentado en AUD-09.

## Interpretación numérica y temporal — H05

Las etapas `interpret_electricity_numeric` e `interpret_electricity_temporal`
añaden respectivamente cinco campos numéricos y tres temporales nominales al
formato largo. La segunda conserva el esquema y los valores recibidos, sin
asignar zona horaria ni interpretar las etiquetas como instantes físicos.

La verificación sintética de la interpretación numérica y temporal y las regresiones seleccionadas de las pruebas iniciales y de transformación a formato largo sumaron 126 pruebas superadas; se excluyó la prueba del lector que
escribe Delta. Las pruebas temporales cubren calendario, precedencia, esquema,
duplicados y composición con ambas etapas anteriores, con distintas opciones
ANSI, zonas horarias y políticas de análisis de fechas. No se han procesado
datos BDG2 reales ni publicado Silver. En aquel momento quedaba pendiente la
integración obligatoria de 10.000 filas largas sintéticas.

En la verificación posterior de integración sintética de 10.000 filas, 5.000 filas Bronze sintéticas y dos
columnas de edificio produjeron 10.000 filas largas. Se comprobaron las tres
etapas, los multiconjuntos, duplicados, nulos, linaje, metadatos y esquemas,
así como el resultado tras reordenar y reparticionar la entrada. La selección
de integración y regresión H05 ejecutó 127 pruebas con éxito y excluyó la del
lector que escribe Delta. Otras 65 pruebas sintéticas de fuentes y configuración
pasaron. La selección H05 cubre opciones ANSI, zonas UTC y con DST, y políticas
de análisis temporal. La prueba condicional de 100.000 filas no se ha ejecutado.
La revisión independiente detectó que faltaba materializar y comparar las
etapas intermedias; se añadieron esas comprobaciones y se repitió la selección.
Con estos resultados queda verificado sintéticamente el contrato de interpretación
numérica y temporal. Esto no autoriza procesar ni publicar Silver con BDG2 real.

## Integración Silver eléctrica — H05

`transform_selected_electricity` recibe un DataFrame Bronze ya suministrado y
compone la transformación a formato largo con las etapas de interpretación numérica y temporal. Exige las
16 columnas aprobadas, con coincidencia exacta; rechaza las ausentes y los
esquemas incompatibles del contrato Bronze. Otras columnas eléctricas no
seleccionadas no se transforman. No busca ni selecciona publicaciones, ejecuta
conteos, escribe datos o publica tablas. La salida es un plan Spark diferido.

El grano es una aparición de fila de entrada por cada uno de los 16 edificios.
La salida conserva `building_id`, `timestamp_original`,
`electricity_value_original` y las cuatro columnas originales `_bronze_*`.
Añade `electricity_value_numeric` como `DECIMAL(38,18)`, `value_parse_status`,
`is_missing`, `is_zero`, `is_negative`, `nominal_date`,
`nominal_second_of_day` y `timestamp_parse_status`. No inventa un identificador
de fila. Un contexto de publicación facilitado expresamente añade
`_bronze_source_name`, `_bronze_snapshot_run_id`, `_bronze_delta_version` y
`_bronze_table_path`; ese contexto declarado no autentica el DataFrame ni
demuestra que proceda de esa publicación. Si no se facilita, no se inventa.

Los valores y etiquetas originales, nulos y duplicados se conservan. La interpretación numérica y temporal
distingue ausencias, finitos, sintaxis inválida, literales no finitos, desborde,
subdesborde y exceso de escala; el texto original permanece incluso cuando no
cabe en el decimal. Sus indicadores de cero y negativo son diagnósticos. La
interpretación temporal es nominal y no resuelve zona, UTC ni DST. No se aplica
R5 de selección como sustituto de las categorías de interpretación, ni se imputan,
agregan o eliminan valores por calidad o por IQR.

El vínculo verificable y la publicación sintética se incorporaron en los
componentes siguientes; esta transformación aislada no los acredita. Procesar
Bronze real requiere autorización separada y las decisiones metodológicas
aplicables. La comparación de fuentes H04 sigue pendiente.

La verificación de la integración eléctrica en Silver y las regresiones H05 seleccionadas han superado 137
pruebas con datos sintéticos; se excluyó la prueba del lector que escribe Delta.
La integración nueva comprueba las 16 columnas, multiplicidad, estados de
calidad y contexto declarado. El primer intento en el entorno restringido no
pudo iniciar la pasarela local de Spark; las pruebas se repitieron con sockets
locales y directorios temporales. No se han procesado datos reales.

## Vínculo con una publicación Bronze sintética — H05

`transform_published_synthetic_electricity` exige un `run_id` explícito y
consulta únicamente su informe `<run_id>.json` en el directorio de ejecuciones
de la configuración. Rechaza informes ausentes, fallidos, incompatibles o con
otra identidad, tablas incompletas y metadatos inválidos; no busca otra ejecución
ni recurre a la versión más reciente. Esta interfaz solo admite configuraciones
declaradas `synthetic`; la declaración por sí sola no demuestra el origen de
los bytes y las pruebas utilizan exclusivamente recursos temporales sintéticos.

El informe H02 identifica las fuentes y la versión Delta de cada tabla. Se
reutilizan sus controles de configuración, rutas, estado, metadatos del commit,
esquema, recuento y lectura mediante `versionAsOf`. Sobre electricidad se exige
además el esquema de la integración eléctrica en Silver y que las cuatro columnas de linaje de todas las filas
coincidan con los datos declarados en el informe y la configuración, sin nulos.
El DataFrame leído y su contexto pasan juntos a la integración eléctrica en Silver; no se acepta un DataFrame
externo como prueba de pertenencia. La comparación de linaje y metadatos no
demuestra por sí sola la autenticidad de la fuente externa.

El lector comprueba los recuentos y el linaje durante la ejecución, pero la
transformación Silver devuelta es diferida. Su evaluación posterior depende de
que el snapshot y sus archivos sigan disponibles y no hayan sido alterados; no
se garantiza retención ni protección frente a manipulación coordinada. La
evaluación informa por separado la compatibilidad, el vínculo, la propagación y
las garantías no establecidas; no certifica automáticamente la corrección de
cada resultado Silver. Las pruebas con lectores simulados comprueban la
orquestación y las versiones solicitadas, no autentican una tabla Delta real.

La verificación del vínculo con la publicación Bronze y las regresiones H05 y H02 ejecutadas han superado 172
pruebas sintéticas; se excluyó una prueba del lector que escribe Delta. La
revisión independiente detectó y verificó la corrección de los controles de
linaje discordante o nulo y de informes con recuento vacío.

El procesamiento de Bronze real sigue sin autorizarse y requiere una extensión
y revisión específicas de esta interfaz sintética, además de las decisiones
metodológicas y autorizaciones correspondientes. La publicación sintética
descrita a continuación no resuelve la comparación Excel–BDG2 pendiente en H04.

## Publicación Silver sintética y recuperación conservadora — H05

`publish_synthetic_silver` admite únicamente configuraciones declaradas
`synthetic` y un directorio aislado bajo `/tmp`. Comprueba que las rutas Bronze
y Silver estén dentro de ese directorio, separadas entre sí y de las raíces
protegidas del proyecto. Rechaza enlaces, archivos especiales y enlaces duros
en los árboles Delta y archivos de control. Estas comprobaciones no prueban el
origen sintético de los bytes y no eliminan las carreras con procesos ajenos.

La identidad lógica combina el `run_id` Bronze y la versión de transformación
`h05.3-v1`; el identificador Silver reservado no puede cambiarse para publicar
otra copia de esa combinación. Solo se admite esa versión de transformación:
una versión futura requiere una implementación y un contrato nuevos. La
intención conserva la huella del informe Bronze, los nombres, rutas, hashes y
versiones declarados por tabla y los recuentos previstos. La vinculación con la publicación Bronze devuelve la
huella de los bytes exactos que interpretó. Antes de escribir, la publicación sintética en Silver contrasta esa huella.
El informe H02 no aporta un identificador inmutable de tabla Bronze, y sus
metadatos no acreditan la procedencia externa.

Un cerrojo local coordina escritores que usan esta interfaz. La reserva e
`intent.json` representan un intento en curso; el commit Delta es independiente;
`completed.json` solo se crea después de releer la versión comprometida y
validar esquema, recuento y multiconjunto completo de filas frente al resultado
esperado, incluidos nulos, duplicados y diagnósticos. La evidencia final incluye
el identificador real de tabla Delta, su versión, esquema, fuentes y recuentos.
No existe una transacción atómica entre commit y evidencia.

En un reintento se vuelve a validar el origen y se reconcilian identidad,
historial Delta y contenido antes de aceptar o finalizar el commit. Una intención
incompleta, tabla parcial, evidencia ilegible, identidad distinta o destino
ajeno bloquea la operación y requiere revisión; no se borra ni sobrescribe. Una
caída antes de completar la evidencia puede dejar estado indeterminado. No se
garantiza exactamente una publicación frente a escritores no cooperativos,
manipulación coordinada, pérdida de almacenamiento o cambios posteriores en las
versiones retenidas. El plan fuente sigue siendo diferido hasta materializarse.

La validación y las regresiones seleccionadas han superado 183 pruebas
sintéticas, con una prueba del lector excluida. Incluyen una publicación Bronze
nueva y una Silver en directorios temporales, reintentos y fallos antes, durante
y después del commit. Se han utilizado JAR locales y no se han procesado datos
reales.

Esta implementación y su evidencia son exclusivamente sintéticas. El uso con
datos o destinos reales necesita otro contrato, autorización expresa, decisiones
metodológicas y validación de recuperación; H05 no queda cerrado y H04 mantiene
su comparación pendiente.

## Diagnóstico conservador de calidad — H05

`summarize_electricity_quality` recibe un DataFrame con el esquema eléctrico de
integración eléctrica en Silver y devuelve un DataFrame de resumen, sin modificar ni publicar
las mediciones. Reutiliza los estados y banderas de la etapa de interpretación numérica y temporal. Comprueba el esquema,
los identificadores y las categorías, y rechaza valores de bandera o numéricos
incompatibles con el estado declarado. Recibe una salida íntegra de la integración eléctrica en Silver y confía
en las etiquetas de tiempo nominal de la etapa de interpretación; no vuelve a interpretar ni autenticar las
etiquetas o fechas facilitadas. La comprobación ejecuta una acción Spark;
la agregación posterior es diferida y requiere que la entrada siga disponible y
sin cambios hasta evaluarla. La función no autentica la procedencia ni autoriza
el acceso a datos reales.

El grano del resultado es cada edificio presente y combinación de las cuatro
columnas de linaje Bronze; si se suministra contexto de publicación, se exigen
sus cuatro campos y también forman parte del grano. Así no se mezclan
publicaciones o procedencias declaradas diferentes. Solo se aceptan los 16
identificadores aprobados; un identificador ajeno o nulo provoca error, no una
exclusión silenciosa. Una entrada vacía produce un resumen vacío y no se crean
filas para edificios ausentes. La salida no tiene orden garantizado.

Para cada grupo, `total_occurrences` es el número de apariciones Silver y es el
**denominador** de las cuentas siguientes; no se calculan porcentajes:

- `numeric_<estado>` cuenta cada uno de los ocho estados numéricos existentes;
  su suma coincide con el total. `missing_occurrences` cuenta las banderas de
  ausencia, mientras que `zero_occurrences` y `negative_occurrences` cuentan
  las banderas verdaderas de valores finitos. Ausencias, ceros y negativos no
  son errores confirmados.
- `timestamp_<estado>` cuenta cada uno de los seis estados temporales; su suma
  coincide con el total. `unclassifiable_timestamps` cuenta todos los estados
  distintos de `valid_nominal`, incluidas ausencias y etiquetas inválidas.
- `repeated_nominal_keys` cuenta etiquetas temporales originales válidas que
  aparecen más de una vez dentro del grupo; su denominador natural es el número
  de etiquetas nominales válidas distintas del grupo.
  `occurrences_in_repeated_nominal_keys` cuenta **todas** las apariciones
  pertenecientes a esas etiquetas; su denominador es `total_occurrences`.
  Los valores temporales no interpretables no forman claves repetidas.

Una etiqueta repetida puede contener lecturas idénticas o diferentes. La
igualdad exacta de registros requeriría comparar todas las columnas Silver;
este resumen no afirma que las etiquetas repetidas sean registros idénticos ni
calcula ese número. Tampoco demuestra errores de origen o instantes físicos
idénticos. No se resuelven zona ni DST y no se crean identificadores de fila,
imputaciones, exclusiones o correcciones de lecturas.

Este diagnóstico no calcula IQR. El motor independiente descrito a continuación
aplica el contrato autorizado para ese análisis. Una señal estadística no confirma
un error ni autoriza sustituir o eliminar observaciones.

Los edificios representan el caso de una cadena hotelera ficticia; las futuras
observaciones BDG2 reales no son datos sintéticos. H04 y H05 formal siguen
pendientes. La ejecución con fuentes reales requiere decisiones y autorización
expresas, revisión de procedencia y rutas y un contrato de publicación real;
este diagnóstico no las concede.

La verificación sintética de diagnóstico de calidad superó tres pruebas focales. Las regresiones
H05 y compatibilidad de módulos superaron 181 pruebas; las de H02, configuración
y Spark, 228, con una prueba real excluida. La selección sintética general
superó 858 pruebas y excluyó la prueba real y una prueba de política que consulta
rutas protegidas. Se usaron JAR locales y destinos temporales; no se procesaron
mediciones BDG2 reales ni se publicaron tablas reales. La revisión técnica
independiente no identificó defectos bloqueantes dentro del contrato descrito.

## Motor estadístico IQR y controles de aceptación — H05

`src/tfm_lakehouse/silver/silver_iqr.py` ofrece dos operaciones sin persistencia:

- `summarize_electricity_iqr(frame)` devuelve exactamente una fila por cada uno
  de los 16 edificios aprobados, incluso si no tiene observaciones elegibles.
- `annotate_electricity_iqr(frame)` conserva todas las columnas y apariciones de
  entrada y añade `is_iqr_outlier`, `iqr_evaluation_status` e `iqr_accepted`.

El esquema debe ser el de electricidad Silver integrada: valores
`DECIMAL(38,18)`, fechas nominales, segundos del día, estados, banderas y linaje.
Se rechazan edificios desconocidos o nulos, columnas ambiguas, tipos
incompatibles y colisiones con los nombres de salida o el prefijo `__iqr_`.
Los campos de contexto de publicación son opcionales, pero deben aparecer
completos y con sus tipos actuales. Los parsers numérico y temporal existentes
comprueban que las interpretaciones coinciden con los textos recibidos. Esta
validación ejecuta una acción Spark; no autentica una publicación ni su origen.

**Población.** Cada edificio tiene una distribución para el periodo nominal
completo del 01/01/2016 al 31/12/2017, sin separar años, meses, emplazamientos ni
procedencias. Participan todas las apariciones con estado numérico `finite`,
timestamp `valid_nominal` y fecha dentro del periodo: ceros, negativos, extremos
y repeticiones incluidos. Se excluyen del cálculo las ausencias, los valores
numéricos no interpretables, las etiquetas temporales inválidas y las fechas
fuera del periodo; esas filas siguen presentes en la vista anotada.

**Cuartiles y precisión.** Se usa la interpolación lineal de posición
`h = (n - 1) × p`, con `p = 0,25` y `p = 0,75`, equivalente al método lineal
por defecto de [pandas](https://pandas.pydata.org/docs/reference/api/pandas.Series.quantile.html).
Spark ordena los decimales y selecciona los cuatro valores de orden necesarios
mediante ventanas y posiciones enteras; no recoge poblaciones en el driver.
Una UDF interpola esos valores con `Decimal`, precisión 80 y rechazo de cualquier
redondeo inexacto. Los empates no requieren un identificador de fila: sus valores
son iguales y todas sus apariciones cuentan.

El [percentil nativo de Spark 3.5.7](https://github.com/apache/spark/blob/v3.5.7/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/percentiles.scala)
interpola en `Double`, por lo que puede perder diferencias entre decimales
grandes. Además, los cuartiles pueden necesitar escala 20 y los límites escala
21 y hasta 42 dígitos totales. Por ello `q1`, `q3`, `iqr`, `lower_limit` y
`upper_limit` son **cadenas decimales exactas**, sin notación exponencial, no
`Double` ni un nuevo `Decimal(38,18)`. Las comparaciones también usan `Decimal`
exacto. Los porcentajes son `Double`; sus numeradores y denominadores enteros
se conservan y los umbrales de aceptación se comparan con enteros.

**Evaluación.** Con menos de cuatro observaciones elegibles, el estado es
`insufficient_population`; cuartiles, límites, aviso de dispersión, denominador
evaluado, recuento de atípicos y porcentaje son nulos. Con cuatro o más se
calculan `IQR = Q3 - Q1`, `lower_limit = Q1 - 1,5 × IQR` y
`upper_limit = Q3 + 1,5 × IQR`. La señal exige `valor < lower_limit` o
`valor > upper_limit`: los límites exactos están incluidos entre los no atípicos.
Un IQR cero mantiene esas comparaciones y activa `iqr_zero_spread`; no impide
por sí solo la aceptación. Los errores de validación o cálculo interrumpen la
operación en lugar de producir un resultado aceptado.

**Cobertura.** La malla esperada contiene 17.544 etiquetas horarias nominales.
Los dos controles exigen al menos 16.667, equivalentes al techo del 95 %:

- `valid_observation_count` cuenta todas las apariciones elegibles, incluidas
  las repetidas y las que no coinciden con la malla. Coincide con
  `eligible_observation_count`; su porcentaje puede superar el 100 %.
- `covered_nominal_hours` cuenta pares distintos de fecha y segundo del día
  elegibles cuyo segundo es divisible por 3.600. Una repetición aporta una sola
  hora. Una lectura a las 11:37 participa en el IQR, pero no cubre las 11:00 ni
  se redondea. `off_grid_observation_count` permite diagnosticar esos casos.

La salida incluye `expected_nominal_hours`, `minimum_coverage_count`,
`valid_observation_coverage_percentage`, `nominal_hour_coverage_percentage` y
los booleanos `valid_observation_coverage_accepted` y
`nominal_hour_coverage_accepted`. Los recuentos de observaciones son `LongType`;
las dos constantes de cobertura son `IntegerType`. `iqr_accepted` solo es verdadero
si el cálculo se completa y ambos controles pasan. Una cobertura insuficiente
conserva los cuartiles y las señales calculadas.

**Señales y resultados.** `iqr_calculation_status` distingue `calculated` de
`insufficient_population`. Para un cálculo completado, `iqr_evaluable_count`
es el tamaño de la población elegible, `iqr_outlier_count` su número de señales y
`iqr_outlier_percentage = 100 × iqr_outlier_count / iqr_evaluable_count`.
`iqr_method = electricity-iqr-linear-decimal-v1` identifica el algoritmo; no
representa una publicación verificada ni una identidad persistida.

La anotación calcula internamente una tabla única por edificio y usa una unión
izquierda por esa clave; no admite resúmenes externos que puedan multiplicar
filas. `is_iqr_outlier` es booleano nullable: verdadero o falso solo cuando la
observación se evalúa; nulo en los demás casos. `iqr_evaluation_status` informa
`evaluated`, `numeric_not_eligible`, `invalid_nominal_timestamp`,
`outside_study_period` o `insufficient_population`; `calculation_error` protege
la rama inesperada sin resultado utilizable. `iqr_accepted` pertenece al edificio,
incluso en filas que individualmente no pueden evaluarse.

Ambas operaciones devuelven planes diferidos: la entrada debe seguir disponible
y estable hasta evaluarlos. No almacenan resultados ni crean cachés. Conservan texto,
linaje, nulos y multiplicidad; no crean claves de fila, instantes UTC ni
resoluciones DST. Tampoco modifican las interfaces existentes de publicación
Silver o diagnóstico de calidad.

La verificación usa exclusivamente poblaciones inventadas y Spark real, con un
oráculo independiente de cuartiles inclusivos sobre fracciones racionales.
Incluye extremos decimales, comparaciones estrictas, poblaciones insuficientes,
fechas nominales, repeticiones, conservación del multiconjunto y ambas fronteras
de cobertura. La persistencia IQR versionada, la vinculación física con Silver,
la orquestación y los reintentos quedaron pendientes en ese incremento. Este incremento no valida
BDG2 real ni cierra formalmente H05.

Verificación del 08/11/2026, sobre la base `8a17e6720b89814941cf4332eb16350d3575bff0`
y este incremento local, con el perfil de JAR locales y recursos temporales
descrito en esta guía. Todos los resultados finales siguientes tienen cero
fallos y cero pruebas omitidas durante la ejecución:

| Selección ejecutada | Superadas | Excluidas |
| --- | ---: | ---: |
| Base previa: Silver, integración eléctrica y compatibilidad de módulos | 181 | 0 |
| Nueva selección `tests/silver/test_silver_iqr.py` | 33 | 0 |
| Silver, integración eléctrica y compatibilidad tras el incremento | 214 | 0 |
| Bronze, configuración y Spark | 228 | 1 |
| Suite sintética general | 891 | 2 |

La primera ejecución focal tuvo un fallo en la expectativa de una prueba de
extremos; se corrigió la población inventada para que sus límites excedieran
realmente el rango original. Las selecciones finales incluyen esa corrección
y el refuerzo de aceptación y cardinalidad indicado en la revisión independiente.
Se excluyeron `real_data`, `performance` y, mediante
`-k "not test_checkout_excludes_private_data_without_hiding_synthetic_fixtures"`,
la comprobación que inspecciona rutas protegidas. Se mantuvo `TFM_REAL_CONFIG`
ausente al arrancar los procesos, sin caché pytest ni bytecode. Las revisiones
técnica y documental independientes finalizaron sin observaciones abiertas.

## Publicación versionada IQR — H05

`src/tfm_lakehouse/silver/silver_iqr_publication.py` mantiene separado el cálculo
del publicador Silver y ofrece:

- `publish_synthetic_iqr(spark, config, silver_publication_id, publication_id, workspace)`:
  publica o reconcilia los resultados IQR de una Silver explícita con el método
  `electricity-iqr-linear-decimal-v1`.
- `read_synthetic_iqr_pair(spark, config, workspace, *, silver_publication_id=None,
  iqr_publication_id=None)`: devuelve el estado de compatibilidad, la Silver y
  sus límites persistidos; sin IQR devuelve `None` y estado `pending`.
  No inicia un cálculo IQR para completar ese estado pendiente.
- `annotate_synthetic_iqr_pair(...)`: devuelve el estado y una vista derivada de
  una pareja verificada con las tres columnas de anotación existentes. Si falta
  IQR, rechaza la operación; no busca límites de otra Silver.

### Uso de la API de publicación

La secuencia es: crear una publicación Bronze finalizada, publicar Silver con
su `run_id`, publicar IQR para el ID Silver devuelto y leer la pareja explícita.
Las publicaciones Silver e IQR requieren llamadas separadas; no hay un comando
CLI u orquestador que las encadene:

```python
silver = publish_synthetic_silver(
    spark, config, bronze_run_id, "silver-demo", workspace
)
iqr = publish_synthetic_iqr(
    spark, config, silver["publication_id"], "iqr-demo", workspace
)
state, silver_frame, limits = read_synthetic_iqr_pair(
    spark, config, workspace,
    silver_publication_id=silver["publication_id"],
)
state, annotated = annotate_synthetic_iqr_pair(
    spark, config, workspace,
    silver_publication_id=silver["publication_id"],
)
```

Es un fragmento de uso, no una demostración ejecutable aislada: `config` debe
ser una configuración `synthetic` con todas sus rutas dentro de `workspace`,
un directorio absoluto nuevo bajo `/tmp`. `bronze_run_id` debe proceder del
informe de una publicación Bronze finalizada que contenga las 16 columnas
eléctricas aprobadas. `publish_synthetic_silver` verifica ese informe y sus
versiones; no elige un `run_id` por su cuenta. La configuración pública
`bronze.synthetic.toml` y sus fixtures solo tienen dos columnas eléctricas, por
lo que sirven para la demostración Bronze del README, pero no para publicar
Silver/IQR. No hay actualmente un fixture público completo de 16 columnas que
permita un recorrido Silver/IQR autocontenido sin preparar otra entrada.

`read_synthetic_iqr_pair` devuelve el estado, el DataFrame Silver verificado y
los límites persistidos o `None`. Antes de publicar IQR, el estado es `pending`;
después, `available` significa que todos los edificios pasan, y `not_accepted`
que el resultado se publicó, pero algún edificio no cumple la aceptación por
población insuficiente o cobertura. No es un fallo de publicación. Los límites
exponen `iqr_calculation_status`, `valid_observation_coverage_accepted`,
`nominal_hour_coverage_accepted` e `iqr_accepted`; la vista anotada añade
`is_iqr_outlier` e `iqr_evaluation_status`. Una señal es diagnóstica y no altera
el valor original. La API conserva la identidad de la Silver exacta para la que
se publicó IQR. La lectura actual puede elegir la pareja más reciente según la
Decisión 29; la lectura histórica exige identificar la Silver y, opcionalmente,
su publicación IQR compatible. El informe Silver identifica `publication_id`,
`source_run_id` y versión Delta; el IQR conserva la identidad padre en su
intención. Las lecturas devuelven DataFrames diferidos, sujetos a que sus archivos permanezcan
disponibles y sin cambios.

**Identidad de origen.** Solo admite configuración `synthetic` y workspace
absoluto bajo `/tmp`, con Bronze, Silver e IQR separados y las guardas de rutas,
enlaces y archivos especiales del publicador Silver. Sin leer CSV originales,
reconstruye el resultado desde las versiones Bronze registradas y verifica
intención y finalización Silver, identidad física Delta, versión, esquema,
recuento y multiconjunto. Las fuentes y sus informes deben seguir disponibles;
la declaración sintética no autentica procedencia externa.

La identidad incluye el contexto Silver/Bronze y SHA-256 de evidencia final,
commit y directorio Silver. El snapshot se materializa temporalmente; sus
huellas se contrastan antes y después del cálculo y antes de finalizar. Un
cambio interrumpe la operación y conserva evidencia parcial. Las cachés propias
se liberan al terminar.

**Recencia aprobada — decisión 29.** Solo participan publicaciones finalizadas y verificadas.
Se lee una única acción `commitInfo` del JSON del commit Delta exacto y se
contrasta su `userMetadata`, versión e identidad física con la evidencia. Su
`timestamp` persistido se compara como un entero de milisegundos desde época,
independiente de la zona horaria de la sesión. No se usa `history().timestamp`:
en Delta 3.3.3 puede proceder de la fecha de modificación del fichero del log.
Tampoco se usan nombres, orden de directorios ni números de versión entre tablas.
Esta versión del lector admite los commits originales del perfil local y
rechaza expresamente tablas con `inCommitTimestamp` activado.

La recuperación conserva el timestamp original. Fechas ausentes, inválidas o
ambiguas y empates en el máximo causan error. Se elige primero la Silver más
reciente y después el IQR compatible más reciente de esa identidad exacta.
La lectura histórica exige
indicar Silver; un ID IQR explícito debe pertenecerle. La evidencia final
corrupta se rechaza, sin recurrir a una publicación anterior. Un resultado
reciente no aceptado sigue visible con `iqr_status = not_accepted`, aviso
`iqr_not_accepted` y edificios afectados: no se reemplaza por otro aceptado ni
habilita Gold.

**Persistencia.** Cada ID IQR reserva
`iqr-publications/<publication_id>/intent.json` y publica una tabla Delta
independiente en `silver_iqr_limits`, con un único commit de versión cero.
Conserva exactamente las 16 filas y todos los campos estadísticos del motor,
incluidos edificios no evaluables o no aceptados y cadenas decimales extremas.
Añade `_iqr_publication_id`, `_silver_publication_id`, `_silver_delta_table_id`,
`_silver_delta_version` y `_silver_identity_sha256`. No convierte los límites
a `Double` ni modifica los valores Silver.

La intención (contrato versión 1) registra padre Silver, método, parámetros,
edificios y huella determinista de las 16 filas; el commit la incluye en
`userMetadata`. `completed.json` añade identidad física y versión IQR, timestamp
y huella del commit, esquema, recuento y conciliación. Solo se crea tras releer
y comparar esquema, 16 edificios, cadenas decimales y multiconjunto. No existe
atomicidad entre Delta y JSON.

**Replay y recuperación.** Un cerrojo coordina escritores locales cooperativos.
El mismo ID y contrato verifica y devuelve el resultado sin otro commit; otro
origen, método, parámetros o contenido se rechaza. Distintos IDs pueden conservar
ejecuciones compatibles con la misma Silver, ordenadas por su propio commit.
Los parámetros devueltos no comparten estado mutable con solicitudes posteriores.

Una invocación explícita puede continuar una reserva válida sin tabla o
reconciliar un commit completo sin marcador final. Tabla parcial, intención
ausente, evidencia ilegible o incompatible, identidad distinta o contenido ajeno
bloquean el destino sin borrarlo ni sobrescribirlo. No hay reintentos automáticos
ni publicación Silver en este módulo.

**Anotación y límites.** La vista utiliza los límites persistidos verificados y
la lógica existente del motor; comprueba cardinalidad y proyección multiconjunto
sobre las columnas Silver. Conserva nulos, repeticiones, textos y linaje. Las
salidas son planes diferidos: los archivos deben permanecer disponibles y sin
cambios después de devolverlos. No se garantiza retención histórica ni protección
frente a escritores externos, alteración coordinada del almacenamiento o relojes
desajustados. El timestamp establece un orden operativo, no un reloj global
autenticado. La orquestación Silver → IQR, H06, Gold y BDG2 real siguen fuera
del alcance; H05 no queda cerrado.

Verificación del 09/11/2026 sobre la base
`7d491f4cca156c05f7a8bba9eaad8f3f7f51c449` y este incremento local, con Python
3.11.17, PySpark 3.5.7, Delta 3.3.3 y pytest 8.4.2. Se utilizaron JAR locales,
directorios temporales aislados, `TFM_REAL_CONFIG` ausente, caché pytest
desactivada y ningún bytecode. Las selecciones finales no tuvieron fallos ni
pruebas omitidas durante la ejecución.

| Comprobación | Superadas | Excluidas |
| --- | ---: | ---: |
| Base previa de H05, integración eléctrica y compatibilidad | 214 | 0 |
| Publicador Silver y motor IQR existentes | 44 | 0 |
| Regresión H05 existente, sin repetir la batería nueva | 214 | 0 |
| Bronze, configuración y Spark | 228 | 1 |
| Suite sintética general, incluidos los 38 casos nuevos | 929 | 2 |

La batería nueva reúne 17 pruebas unitarias y 21 integraciones Spark/Delta
sintéticas. Historial focal: 36 superadas y un fallo de configuración de rutas
de la fixture; tras corregirla, 37 superadas; refuerzos posteriores de cuatro,
dos y una pruebas superados. La selección general incluye los 38 casos finales.

[Las pruebas de publicación](../tests/silver/test_silver_iqr_publication.py)
cubren T16–T23: identidad, anotación, método, replay, conflictos, ocho puntos de
interrupción, historia incompatible, estados pendientes y no aceptados. Los
commits, lecturas y conciliaciones usan Delta real; los fallos se inyectan.
La recuperación conserva un commit o bloquea sin borrar. También se verifica
el rechazo de contenido falsificado con igual esquema y recuento y la
independencia de la selección frente a cambios de `mtime`.

Se excluyeron los marcadores `real_data` y `performance` y la prueba de política
que inspecciona rutas protegidas. Las dos exclusiones de la suite general son
la prueba real y esa comprobación de política; no son pruebas superadas.
Las revisiones técnica y documental independientes no identificaron defectos
bloqueantes abiertos. No se ha ejecutado BDG2 real ni se ha medido rendimiento.
