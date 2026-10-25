# Hito 5 — Silver de electricidad y controles de calidad

**Estado:** planificado. **Responsabilidad:** Carlos implementa el flujo;
Irantzu define y valida las reglas analíticas. **Objetivos:** OE3, OE4.
**Requisito previo:** contratos aceptados en H04.

## Finalidad

Implementar un flujo mínimo desde una **publicación Bronze coherente** hasta
electricidad Silver en formato largo, con trazabilidad y anotaciones de
calidad explícitas. Se comenzará por una transformación acotada; las reglas
adicionales se introducirán mediante incrementos verificables.

## Actividades incrementales

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
