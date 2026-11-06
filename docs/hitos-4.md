# Hito 4 — Selección del caso de estudio y contratos analíticos

**Estado:** planificado. **Responsabilidad:** Irantzu dirige las decisiones
analíticas; Carlos verifica su viabilidad técnica. **Objetivos:** OE1, OE2,
OE4. **Requisito previo:** caracterización aceptada en H03.

## Finalidad

Definir un escenario multisede **ficticio**, justificable y trazable, junto
con los contratos de datos de Silver y las reglas de calidad que deberán
respetarse antes de transformar las mediciones.

## Evidencia disponible y límites

La [guía H03](hitos-3.md) distingue las ejecuciones históricas de su aceptación
académica, todavía pendiente. Las fuentes documentadas son BDG2 v1.0, con
electricidad `raw`; su identidad y hashes constan en las configuraciones y
expedientes privados. Esta revisión documental no reanaliza los CSV.

| Referencia | Evidencia existente | Límite para H04 |
| --- | --- | --- |
| [H03.2](h03-2-characterization.md) | Expediente `data/checks/h03_2/real/2026-10-30-h03-2/`, contrato histórico `h03.2-v1`. | Utilizó 3.000–7.000 m² y obtuvo 65 candidatos por superficie; no es una verificación del nuevo intervalo o de una cohorte final de 16. |
| [H03.3](h03-3-weather-timezones.md) | Expediente `data/checks/h03_3/real/2026-10-30-h03-3/`, contrato histórico `h03.3-v1`. | Diagnóstico meteorológico y temporal, sin unión horaria validada. |
| [H05.1](hitos-5.md#h051--transformación-técnica-preliminar) | Transformación preliminar implementada y probada con datos sintéticos. | No selecciona el caso académico, no aprueba H04 y no acredita una ejecución Silver con BDG2 real. |

El material exploratorio descrito más abajo identifica 16 columnas eléctricas
y permite contrastar sus ausencias con los agregados H03.2. Sigue pendiente
documentar el origen del subconjunto y reproducir sus filtros desde las fuentes.
Los 14 perfiles detallados de H03.2 no constituyen esa lista definitiva. La
revisión de rachas de la cohorte completa tampoco queda cubierta por esos
perfiles ni por las salidas guardadas del cuaderno.

H03.2 registra 17.544 etiquetas horarias nominales de 2016–2017. Su regularidad
no demuestra instantes físicos inequívocos ni resuelve DST. Los porcentajes de
ausencia usan registros recibidos; los de ceros, lecturas numéricas finitas.
Una baja ausencia no descarta episodios prolongados de ceros. Las señales y
extremos del expediente no confirman por sí solos errores de medida.

## Decisiones previas

- Criterios de selección: identificadores originales, usos y subusos
  verificados, disponibilidad eléctrica, emplazamiento, periodo, cobertura
  mínima y motivos de exclusión.
- Correspondencia entre edificios BDG2 y establecimientos simulados, sin
  alterar identificadores ni afirmar que constituyen hoteles reales.
- Granularidad de electricidad, claves, semántica del medidor, unidades,
  horarios locales, zonas horarias y tratamiento de horas duplicadas o
  inexistentes por cambios de horario.
- Política de ausencias, ceros, negativos, extremos y duplicados: distinguir
  avisos, errores confirmados y exclusiones autorizadas.
- Reglas de integración de metadatos y meteorología, cardinalidades válidas
  y tratamiento de correspondencias no resueltas.

## Relación con el contrato Silver existente

H05.1 conserva una aparición de fila Bronze por cada columna eléctrica
seleccionada: `filas largas = apariciones Bronze × columnas seleccionadas`.
Mantiene identificadores originales, texto, null, duplicados y linaje. No aporta
un identificador original único de fila. El esquema y la lectura de versiones
publicadas se mantienen en la [guía H05](hitos-5.md#interfaces-y-esquema-provisional)
y en el [contrato Bronze](hitos-1-2.md#salida-estrategia-de-escritura-y-trazabilidad).

La interpretación numérica, las unidades aplicables, la semántica temporal,
las reglas de calidad y la persistencia siguen requiriendo los contratos de
H04. La etiqueta original no fija offsets, `fold` ni el inicio o final de un
intervalo energético. Los alias académicos y los identificadores ficticios
deberán documentarse por separado del identificador BDG2, sin reemplazarlo.
Las banderas de calidad y la aptitud para un indicador son decisiones distintas;
no se autorizan exclusiones, imputaciones o conversiones UTC implícitas.

## Actividades incrementales

1. Comparar subconjuntos candidatos con las evidencias de H03, dejando
   constancia de los edificios elegidos y descartados.
2. Aprobar un manifiesto del caso: edificios, medidores, periodo,
   correspondencia ficticia, versiones de origen y limitaciones.
3. Especificar tablas Silver, atributos, tipos, claves, granularidades,
   campos de trazabilidad y cardinalidades esperadas.
4. Documentar una tabla de reglas de calidad con motivo, severidad y
   tratamiento, y las hipótesis temporales por fuente.
5. Definir ejemplos positivos y negativos para las pruebas de H05 y H06.

## Contraste con el análisis exploratorio

Se han inspeccionado en lectura los archivos aportados como material de Irantzu,
sin copiarlos al repositorio ni ejecutar el cuaderno. Estas huellas identifican
los archivos revisados; no certifican su derivación de los CSV originales.

| Archivo privado | SHA-256 |
| --- | --- |
| `consumo_electrico_seleccion.xlsx` | `91e806e478df9256a2b5ad7876fe69a6cf4fc9ec6d31051f7e9d1e1a5478ef87` |
| `Silver.ipynb` | `2a02354c5607e63bc6b3a82c79179818a320980a2ed6261c687f7570bb24d648` |

La hoja `Hoja1` contiene 17.544 filas de datos, `timestamp` y 16 columnas con
identificadores BDG2. La lista y los recuentos de ausencia coinciden con las
salidas guardadas del cuaderno; los 16 recuentos de ausencia también coinciden
con `metrics.area_cohort_quality` del informe histórico H03.2. Es un contraste
parcial, sin comprobar identidad celda a celda con los originales.

El cuaderno carga el Excel con pandas y contiene estadísticas descriptivas,
ausencias, señales IQR y diagramas de caja. Las opciones de imputación están
comentadas. Las salidas almacenadas no son una ejecución nueva ni demuestran
el flujo Silver: no hay lectura de publicaciones Bronze, transformación a
formato largo o persistencia Delta en su código.

Falta documentar la extracción del Excel: fuentes, variantes y hashes de
origen, filtros de selección y transformaciones aplicadas. El nombre del
cuaderno no acredita aprobación de contratos. Este material complementa el
contraste de H04; no sustituye las fuentes originales ni la publicación Bronze.

## Entregables previstos

- Justificación de la selección y manifiesto de procedencia, conservando
  cualquier información sensible en ubicaciones locales excluidas de Git.
- Correspondencia explícita entre establecimientos simulados y edificios BDG2.
- Contratos versionados de Silver, calidad, integración y criterios de prueba.

## Criterios de aceptación

- [ ] Todos los edificios y periodos elegidos satisfacen reglas explícitas;
      se documentan exclusiones y sesgos de selección.
- [ ] La cadena hotelera se identifica como ficticia; los datos de BDG2
      conservan su procedencia y características originales.
- [ ] Unidades, granularidades, claves, nulos, zonas horarias y uniones
      tienen fundamento o figuran como cuestiones pendientes.
- [ ] Las anomalías señaladas se distinguen de los errores confirmados;
      no se autorizan limpiezas o imputaciones implícitas.
- [ ] Ambos autores aprueban los contratos y el alcance antes de implementar
      el siguiente incremento Silver.

## Issues de GitHub propuestos

- `[H04] Justificar el subconjunto de edificios de alojamiento`
- `[H04] Definir la correspondencia con establecimientos ficticios`
- `[H04] Aprobar esquema Silver y reglas temporales`
- `[H04] Aprobar criterios de calidad e integración`

## Límites y condición de avance

H05.1 fue autorizado por separado como transformación técnica preliminar.
Su existencia no completa H05 ni habilita fases posteriores. H04 formaliza
las decisiones pendientes; el siguiente incremento necesita alcance y contratos
aprobados por ambos autores. La integración meteorológica queda para
[H06](hitos-6.md), con semántica temporal y cardinalidad justificadas.
