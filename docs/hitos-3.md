# Hito 3 — Caracterización de las fuentes BDG2

**Estado:** subentregables técnicos disponibles; cierre académico global pendiente.
El [registro principal de hitos](milestones.md) distingue implementación,
pruebas, ejecuciones reales históricas y aceptación. Esta guía conserva el
plan y los criterios; actualizarla no acredita una nueva caracterización.
**Responsabilidad principal:** Irantzu; Carlos colabora en procedencia y
comprobaciones técnicas. **Objetivo:** OE1.

## Finalidad

Obtener una base de evidencias reproducible sobre los **CSV originales de
BDG2 disponibles localmente**, sin confundir las cifras de la publicación,
los análisis preliminares de la memoria y los datos sintéticos de prueba con
resultados verificados de esta instalación.

## Plan original y decisiones previas

- Confirmar las rutas en WSL, la procedencia y la variante de cada archivo:
  `raw`, `cleaned`, `kaggle` o derivación documentada.
- Identificar la publicación o revisión de origen y cualquier extracción local.
- Acordar el destino privado de los informes. La inspección inicial será de
  solo lectura y no ejecutará recargas Bronze.

## Actividades incrementales

1. Verificar rutas, formato, codificación, cabeceras, tamaños y SHA-256;
   detectar y rechazar los punteros Git LFS que no contengan CSV reales.
2. Inventariar registros, edificios, medidores, emplazamientos y periodos,
   diferenciando claramente cada tipo de recuento.
3. Caracterizar cobertura temporal, valores ausentes, ceros, negativos,
   extremos, duplicados y posibles discontinuidades **sin corregirlos**.
   Distinguir ausencias aparentes de cambios de horario estacional.
4. Contrastar los resultados con las fuentes BDG2 y el apartado 2.2.6 de
   la memoria; registrar diferencias, método de cálculo e incertidumbres.
5. Identificar edificios candidatos del ámbito del alojamiento para H04,
   sin clasificarlos todavía como hoteles reales.

## Entregables y evolución del plan

- Inventario privado y reproducible, por ejemplo en `data/checks/`, sin
  incluir los originales ni extractos de registros reales en Git.
- Métodos y herramientas ya implementados en
  [H03.2](h03-2-characterization.md) y [H03.3](h03-3-weather-timezones.md).
  La verificación de procedencia H03.1 queda recogida en configuraciones y
  expedientes. La previsión inicial de crear código reutilizable ya tiene
  implementación; no se presenta como trabajo técnico sin iniciar.
- Resumen publicable de procedencia, cobertura, limitaciones y decisiones
  pendientes para la selección del caso.

## Criterios de aceptación

Los criterios originales se mantienen para revisión conjunta. Las casillas no
marcadas no niegan las comprobaciones técnicas existentes: falta consolidar la
aceptación global y el contraste académico. El expediente real H03.2 es v1;
el contrato v2 de AUD-03/AUD-04 está probado con sintéticos, sin nueva ejecución
BDG2. El expediente real H03.3 también es histórico. No se reescriben sus hashes.

- [ ] Cada fuente utilizada tiene ruta, variante, procedencia, hash,
      estructura y resultado de verificación conocidos.
- [ ] Los recuentos de edificios, medidores y registros se definen por
      separado y pueden reproducirse mediante métodos documentados.
- [ ] Las incidencias de cobertura y calidad se describen sin limpieza
      automática ni inferencias temporales injustificadas.
- [ ] Las discrepancias con la memoria se presentan para revisión conjunta.
- [ ] No se han modificado ni publicado los datos originales.

## Issues de GitHub propuestos

- `[H03] Verificar procedencia, variantes y SHA-256 de BDG2`
- `[H03] Caracterizar cobertura de electricidad y metadatos`
- `[H03] Revisar disponibilidad meteorológica y zonas horarias`
- `[H03] Contrastar la caracterización con la memoria del TFM`

## Límites y condición de avance

No crear Silver, seleccionar definitivamente edificios, imputar observaciones
ni sobrescribir tablas existentes. H04 comenzará cuando ambos autores
acepten el inventario y las cuestiones abiertas.
