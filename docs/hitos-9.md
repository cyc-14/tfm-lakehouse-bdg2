# Hito 9 — Integridad y recuperación integral

**Estado:** planificado. **Responsabilidad:** Carlos dirige la validación
técnica; Irantzu revisa la semántica y trazabilidad.
**Objetivos:** OE3, OE6. **Requisitos previos:** flujo H05–H08 implementado.

## Finalidad

Comprobar que una ejecución local produce resultados Silver y Gold
**coherentes entre versiones, auditables y recuperables**, incluso tras
reejecuciones normales y fallos controlados. Deben respetarse las garantías
de publicación ya establecidas para Bronze.

## Actividades incrementales

1. Definir dependencias de ejecución y contratos de lectura y escritura,
   fijando cada entrada a publicaciones y versiones compatibles.
2. Establecer un mecanismo de publicación o manifiesto de resultados entre
   capas que impida interpretar tablas parcialmente actualizadas como
   pertenecientes a una única ejecución satisfactoria.
3. Probar reejecuciones, versiones ausentes u obsoletas y fallos entre
   escrituras de tablas, sin alterar los CSV originales.
4. Comprobar recuperación desde publicaciones completas anteriores y
   documentar las condiciones de conservación de versiones.
5. Ejecutar, si se autoriza, el flujo sobre el subconjunto BDG2 revisado,
   conservando evidencias privadas y resultados agregados publicables.

## Entregables previstos

- Procedimiento reproducible de ejecución y dependencias entre capas.
- Manifiesto de ejecución, versiones de tablas y documentación de fallos.
- Pruebas de integración y regresión sintéticas; acta separada de cualquier
  validación realizada con datos reales.

## Criterios de aceptación

- [ ] Todas las tablas derivadas remiten a las versiones correctas de sus
      entradas y a los contratos analíticos aprobados.
- [ ] Un fallo entre varias tablas no se comunica como transacción global
      satisfactoria; la recuperación identifica un conjunto completo anterior.
- [ ] Las rutas de entrada y salida permanecen separadas e inalteradas.
- [ ] Las reejecuciones presentan recuentos conciliados y cambios de versión
      documentados.
- [ ] Las pruebas cubren operaciones normales, fallos parciales, versiones
      inaccesibles y manifiestos inválidos, con resultados ejecutados.

## Issues de GitHub propuestos

- `[H09] Definir contratos de versiones y manifiesto entre capas`
- `[H09] Implementar publicaciones y lecturas coherentes`
- `[H09] Probar fallos parciales, reejecuciones y recuperación`
- `[H09] Revisar evidencias de ejecución integral con BDG2`

## Límites y condición de avance

Delta garantiza transacciones **por tabla**, no de forma conjunta para
Bronze, Silver y Gold. No ejecutar `VACUUM` ni eliminar versiones necesarias
para la recuperación. H10 requiere un flujo integral repetible.
