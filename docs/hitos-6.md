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
