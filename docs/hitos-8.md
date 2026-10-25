# Hito 8 — Consultas y visualización analítica

**Estado:** planificado. **Responsabilidad:** Irantzu define preguntas e
interpretaciones; Carlos revisa el acceso reproducible a los datos.
**Objetivos:** OE5, OE7. **Requisito previo:** Gold aceptada en H07.

## Finalidad

Proporcionar una forma **local y de solo lectura** para consultar tendencias,
cobertura y comparaciones entre establecimientos. Se elegirá la solución
de presentación más sencilla que satisfaga el objetivo del TFM, sin
presuponer la necesidad de una plataforma de cuadros de mando compleja.

## Actividades incrementales

1. Acordar entre tres y cinco preguntas analíticas concretas, sus
   destinatarios y los indicadores Gold necesarios para responderlas.
2. Implementar consultas parametrizadas y verificadas por edificios,
   establecimientos y periodos comparables.
3. Incorporar figuras estáticas o visualizaciones locales mínimas,
   justificando las herramientas y dependencias que se introduzcan.
4. Mostrar unidades, zonas horarias, registros ausentes y condiciones
   de calidad junto a cada resultado.
5. Documentar una tarea representativa y comprobar que las salidas
   publicables no contienen rutas privadas ni registros restringidos.

## Entregables previstos

- Consultas analíticas de solo lectura y ejemplos reproducibles.
- Mecanismo de visualización con leyendas, procedencia y limitaciones.
- Pruebas o protocolo verificable de filtros, selecciones inválidas,
  resultados vacíos y comparaciones entre periodos.

## Criterios de aceptación

- [ ] Se responde al menos una pregunta completa desde una publicación
      Gold identificable y coherente.
- [ ] Tablas y figuras muestran unidades, periodos, fuente y cobertura;
      las comparaciones no ocultan periodos parciales.
- [ ] Parámetros desconocidos o inválidos reciben un tratamiento explícito.
- [ ] Los resultados pueden reproducirse en el entorno local declarado,
      sin depender de servicios externos no aprobados.
- [ ] Existen tareas de evaluación definidas para H11.

## Issues de GitHub propuestos

- `[H08] Definir preguntas de análisis y contratos de consulta`
- `[H08] Implementar consultas Gold reproducibles y seguras`
- `[H08] Visualizar tendencias, cobertura y comparaciones`
- `[H08] Verificar etiquetas, casos límite y reproducibilidad`

## Límites y condición de avance

No añadir alojamiento en la nube, autenticación compleja ni plataformas
comerciales sin una justificación específica. H09 comprobará la integridad
del flujo completo que sustenta los resultados presentados.
