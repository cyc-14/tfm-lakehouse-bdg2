# Hito 10 — Evaluación técnica controlada

**Estado:** planificado; este documento no implica resultados medidos.
**Responsabilidad:** Carlos. **Objetivo:** OE6.
**Requisito previo:** flujo y procedimiento reproducibles aceptados en H09.

## Finalidad

Evaluar el comportamiento del procesamiento local mediante **mediciones
repetibles y condiciones documentadas**. Analizar el efecto del volumen
de entrada y de la configuración sin presentar las pruebas en WSL como
evidencia de escalabilidad en un clúster distribuido.

## Actividades incrementales

1. Aprobar preguntas, línea base, equipo, condiciones WSL, versiones de
   Java/Python/Spark/Delta, carga del sistema y métricas de evaluación.
2. Definir tamaños de trabajo reproducibles sin inventar mediciones
   energéticas ni confundir datos duplicados con observaciones independientes.
3. Preparar pruebas de rendimiento separadas de las funcionales, registrando
   calentamiento, cachés, número de repeticiones y tiempo transcurrido.
4. Recoger recuentos, tamaños, rendimiento y memoria cuando existan medidas
   fiables; distinguir variabilidad observada de relaciones causales.
5. Interpretar resultados respecto a los requisitos del TFM y declarar
   limitaciones, incertidumbres y posibles factores de confusión.

## Entregables previstos

- Protocolo de evaluación aprobado y comandos reproducibles.
- Resultados locales con tamaños, hashes, repeticiones, entorno, revisión
  de código, fecha y medidas realmente observadas.
- Tablas y figuras académicas elaboradas únicamente tras contrastar los
  resultados y revisar su procedencia.

## Criterios de aceptación

- [ ] Las comparaciones controlan las condiciones relevantes o explican
      las variables que no pudieron aislarse.
- [ ] Los tamaños y ensayos están registrados; no se inventan tiempos,
      tasas, aceleraciones ni volúmenes de observaciones.
- [ ] Las conclusiones se limitan al **procesamiento local por lotes**.
- [ ] Se informa de la dispersión cuando procede y de los efectos de
      recursos, caché, ejecuciones frías y validez externa.
- [ ] Las comprobaciones de corrección siguen activas: la velocidad no
      demuestra calidad analítica.

## Issues de GitHub propuestos

- `[H10] Aprobar protocolo de rendimiento local`
- `[H10] Preparar pruebas de rendimiento repetibles y aisladas`
- `[H10] Medir cargas y registrar las condiciones del entorno`
- `[H10] Interpretar resultados técnicos y limitaciones`

## Límites y condición de avance

No introducir un clúster o servicios en la nube para atribuir capacidades
que no se han evaluado. H11 valorará la utilidad y validez de la información
de forma independiente del rendimiento del software.
