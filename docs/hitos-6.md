# Hito 6 — Integración de metadatos y meteorología

**Estado:** diagnóstico y vista sintética de metadatos implementados; aceptación
del enriquecimiento pendiente. **Responsabilidad:** Irantzu valida significados y
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
de forma conservadora. La interpretación de campos y el tratamiento de conflictos
siguen pendientes de aprobación.

La selección sintética de este diagnóstico y las regresiones de vínculo y
publicación Silver superaron 43 pruebas. La distinción entre cadena vacía y
`null` se comprobó con un DataFrame modificado en prueba; Bronze representa
como `null` los campos CSV sin valor según su contrato.

### Vista sintética de metadatos

El autor ha aprobado para sintéticos H06-D01 y D02: bloquear
duplicados idénticos o conflictivos de los 16 edificios; D03: diagnosticar
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
versión de contrato 1, distinta de la versión 2 de la intención. El futuro
lector histórico deberá exigir la misma evidencia física. La ratificación
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
La validación sintética integral de este incremento aún está pendiente; no hay
lector histórico independiente ni selección automática de publicaciones.
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
para respetar H05 y la repetición pasó. La revisión independiente respalda
iniciar la lectura histórica explícita; ese lector aún no está implementado.
La regresión sintética general queda reservada a la verificación integrada H06.

### Diagnóstico sintético de meteorología

El autor ha aprobado H06-D09–D14 para sintéticos: diagnosticar sin enriquecer;
utilizar únicamente weather del manifiesto Bronze asociado a Silver; tratar
`site_id` como correspondencia nominal; conservar etiquetas nominales;
diagnosticar duplicados sin resolverlos; y contabilizar ausencias, variables,
unidades documentales y códigos sin imputar ni reinterpretar. La ratificación
académica conjunta sigue pendiente.

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
caracterización meteorológica superó 73 pruebas. El caso de weather disponible
con IQR aceptado no se ha materializado conjuntamente; la API no consulta IQR.
Tras reforzar el diagnóstico simultáneo de duplicados y columna ausente, la
selección afectada y las regresiones meteorológicas superaron 72 pruebas.

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
