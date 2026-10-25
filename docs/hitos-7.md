# Hito 7 — Indicadores Gold y comparativas

**Estado:** planificado. **Responsabilidad:** Irantzu aprueba métricas y
comparabilidad; Carlos implementa los cálculos y la persistencia.
**Objetivos:** OE3, OE5. **Requisito previo:** integración aceptada en H06
o contrato Silver aprobado sin meteorología.

## Finalidad

Obtener productos Gold acotados, con indicadores bien definidos para
estudiar tendencias y comparar de forma **condicionada** los establecimientos
ficticios. Se validará primero un único indicador antes de ampliarlo.

## Actividades incrementales

1. Comprobar el significado de la medición eléctrica, su unidad y si
   representa energía de intervalo susceptible de agregarse mediante suma.
2. Aprobar granularidad Gold, correspondencia de edificios, periodos,
   registros elegibles y denominadores de cobertura.
3. Implementar una agregación por periodo con recuento, cobertura prevista
   y observada, incidencias y versiones de origen.
4. Incorporar, si están justificados, perfiles horarios, tendencias
   diarias/mensuales e intensidad por superficie `sqm` positiva y verificada.
5. Documentar las limitaciones de comparación entre tipologías, periodos,
   zonas horarias y distintos niveles de información contextual.

## Entregables previstos

- Tablas o vistas Gold con esquema y granularidad definidos.
- Diccionario de indicadores: fórmula, unidad, periodo, denominador,
  tratamiento de ausencias y condiciones de validez.
- Pruebas de conciliación con datos sintéticos controlados; validación
  con BDG2 real únicamente mediante ejecuciones aprobadas.

## Criterios de aceptación

- [ ] Cada indicador tiene fórmula, unidad, denominador, periodo y
      limitaciones comprobables.
- [ ] Los periodos incompletos se identifican y las ausencias no se
      interpretan como consumo cero.
- [ ] Las superficies inválidas no generan intensidades numéricas.
- [ ] Se han probado casos con nulos, ceros, periodos incompletos,
      transiciones horarias y coberturas desiguales.
- [ ] Los resultados concilian con Silver y conservan las versiones de origen.
- [ ] No se confunde un mayor consumo o intensidad con una menor eficiencia
      demostrada sin contexto suficiente.

## Issues de GitHub propuestos

- `[H07] Aprobar fórmulas, unidades y denominadores de cobertura`
- `[H07] Implementar primera agregación Gold verificada`
- `[H07] Incorporar perfiles e intensidad por superficie justificables`
- `[H07] Conciliar indicadores y documentar comparabilidad`

## Límites y condición de avance

No incorporar ahorros económicos, emisiones, ocupación ni predicciones sin
datos y alcance aprobados. H08 requiere al menos un producto Gold válido.
