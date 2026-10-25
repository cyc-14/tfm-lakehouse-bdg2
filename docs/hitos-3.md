# Hito 3 — Caracterización de las fuentes BDG2

**Estado:** planificado; este documento no acredita una nueva caracterización.
**Responsabilidad principal:** Irantzu; Carlos colabora en procedencia y
comprobaciones técnicas. **Objetivo:** OE1.

## Finalidad

Obtener una base de evidencias reproducible sobre los **CSV originales de
BDG2 disponibles localmente**, sin confundir las cifras de la publicación,
los análisis preliminares de la memoria y los datos sintéticos de prueba con
resultados verificados de esta instalación.

## Decisiones previas

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

## Entregables previstos

- Inventario privado y reproducible, por ejemplo en `data/checks/`, sin
  incluir los originales ni extractos de registros reales en Git.
- Método de comprobación documentado y, si se aprueba, código de inspección
  reutilizable. No se presupone que dicho código exista todavía.
- Resumen publicable de procedencia, cobertura, limitaciones y decisiones
  pendientes para la selección del caso.

## Criterios de aceptación

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
