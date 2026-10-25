# Hito 4 — Selección del caso de estudio y contratos analíticos

**Estado:** planificado. **Responsabilidad:** Irantzu dirige las decisiones
analíticas; Carlos verifica su viabilidad técnica. **Objetivos:** OE1, OE2,
OE4. **Requisito previo:** caracterización aceptada en H03.

## Finalidad

Definir un escenario multisede **ficticio**, justificable y trazable, junto
con los contratos de datos de Silver y las reglas de calidad que deberán
respetarse antes de transformar las mediciones.

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
- [ ] Ambos autores aprueban los contratos antes de implementar Silver.

## Issues de GitHub propuestos

- `[H04] Justificar el subconjunto de edificios de alojamiento`
- `[H04] Definir la correspondencia con establecimientos ficticios`
- `[H04] Aprobar esquema Silver y reglas temporales`
- `[H04] Aprobar criterios de calidad e integración`

## Límites y condición de avance

Este hito formaliza decisiones; no demuestra que Silver o Gold estén
implementadas. H05 se iniciará con una transformación vertical limitada,
basada en contratos aprobados.
