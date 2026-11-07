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

### Propuesta de contrato H05.2 v0.3

La propuesta define una transformación posterior a H05.1. Su revisión técnica
está completada, con dictamen **READY FOR JOINT REVIEW** y revisión independiente.
Ese dictamen acredita la preparación del diseño para revisión conjunta; la
aprobación de los autores y la autorización de implementación siguen separadas:

| Aspecto | Estado |
| --- | --- |
| Primer autor | Aprobación provisional del diseño técnico detallado |
| Irantzu | Acuerdo con los principios generales de tratamiento, comunicado por el primer autor |
| Ratificación conjunta detallada | No consta evidencia independiente que la acredite |
| Implementación H05.2 | Todavía no autorizada |
| Ejecución BDG2 real y publicación Delta | Requieren autorizaciones separadas |

**Alcance y conservación.** La transformación será pura, conservativa,
determinista y sin persistencia. Su implementación y validación iniciales
utilizarán exclusivamente datos sintéticos. Quedan fuera BDG2 real,
persistencia Delta, imputación, limpieza destructiva, correcciones automáticas,
detección avanzada de anomalías, análisis de rachas de ceros, meteorología y
cálculos Gold.

Cada aparición de entrada producirá exactamente una aparición de salida. Se
conservarán todas las columnas recibidas de H05.1: nombres, valores, tipos,
nulabilidad, nulls, duplicados, multiplicidad, linaje y campos de versión de la
publicación. No se inventarán claves primarias ni se impondrá unicidad por
edificio y etiqueta temporal. La proyección sobre los campos originales deberá
ser igual al multiconjunto de entrada, además de conservar el número de filas.

**Interpretación decimal exacta.** Se interpretará `electricity_value_original`
como `DECIMAL(38,18)` únicamente cuando su valor sea representable exactamente.
La gramática será la siguiente, con coincidencia de toda la cadena:

```text
[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?
```

Admite dígitos ASCII, punto decimal, signos y notación científica. No admite
espacios exteriores, separadores regionales ni normalizaciones implícitas.
No habrá conversiones intermedias a coma flotante binaria, redondeos ni
truncamientos. Se definen `q = 10^-18` y `M = 10^20 - 10^-18`; un valor es
exactamente representable si `|x| ≤ M` y `x/q` es entero.

Los ocho estados de `value_parse_status` se asignarán con esta precedencia:

1. NULL recibido → `missing_null`.
2. Token exacto `""` o `"NaN"` → `missing_token`.
3. Literal exacto `"Infinity"`, `"+Infinity"` o `"-Infinity"` → `nonfinite_literal`.
4. Incumplimiento de la gramática completa → `invalid_syntax`.
5. Cero matemático → `finite`.
6. `|x| > M` → `overflow`.
7. `0 < |x| < q` → `underflow`.
8. Valor dentro del rango, pero sin representación exacta → `scale_exceeded`.
9. Resto → `finite`.

`electricity_value_numeric` contendrá el decimal exacto solo para `finite`;
**será NULL para cualquier otro estado**. El texto original permanecerá intacto.
Se admitirán ceros finales prescindibles, ceros con signo y exponentes válidos
extremos sin clasificación incorrecta ni expansión numérica desproporcionada.

**Ausencias e indicadores.** Solo NULL, `""` y `"NaN"` exactos serán ausencias.
`is_missing` será verdadero únicamente para `missing_null` y `missing_token`.
Para `finite`, `is_zero` indicará igualdad con cero e `is_negative`, valor menor
que cero; para los demás estados ambos serán NULL. No se imputarán ni eliminarán
lecturas. Un cero o un negativo no se clasificará automáticamente como error
físico.

**Temporalidad nominal.** Se conservará `timestamp_original`. El formato
admitido será exactamente `yyyy-MM-dd HH:mm:ss`, con 19 caracteres ASCII y
separadores literales. Se aplicará el calendario gregoriano proléptico, años
0001–9999, horas 00–23 y minutos y segundos 00–59.

Los seis estados de `timestamp_parse_status`, por orden de evaluación, serán:

1. NULL recibido → `missing_null`.
2. Cadena exactamente vacía → `missing_empty`.
3. Estructura incorrecta → `invalid_format`.
4. Fecha imposible o año fuera del dominio → `invalid_calendar`.
5. Hora, minuto o segundo fuera de rango → `invalid_time`.
6. Fecha y hora nominales válidas → `valid_nominal`.

Solo para `valid_nominal` se derivarán `nominal_date` y
`nominal_second_of_day = 3600 * hora + 60 * minuto + segundo`, en el rango
0–86399. Para cualquier otro estado ambos derivados serán NULL. Se conservarán
etiquetas repetidas sin inferir UTC, zona horaria, `fold` de DST, instante físico
ni continuidad física.

**Esquema y validación estructural.** Se añadirán exactamente ocho campos:

| Campo | Tipo Spark | Nullable |
| --- | --- | --- |
| `electricity_value_numeric` | DECIMAL(38,18) | Sí |
| `value_parse_status` | STRING | No |
| `is_missing` | BOOLEAN | No |
| `is_zero` | BOOLEAN | Sí |
| `is_negative` | BOOLEAN | Sí |
| `nominal_date` | DATE | Sí |
| `nominal_second_of_day` | INTEGER | Sí |
| `timestamp_parse_status` | STRING | No |

Se aceptará el esquema largo de H05.1 descrito en la guía H05: la salida básica
pasará de siete a quince columnas y la del lector, de once a diecinueve. Se
preservarán la versión Delta entera y todos los campos adicionales cuyos nombres
no sean ambiguos ni colisionen. No se aplicará directamente el validador Bronze
que exige cadenas en todas las columnas.

Se rechazarán campos obligatorios ausentes, tipos incompatibles, nombres
duplicados o ambiguos y colisiones con los ocho nombres nuevos, incluidas las
detectadas sin distinguir mayúsculas mediante `casefold`. No se renombrarán ni
sobrescribirán campos. Los tipos y la nulabilidad declarada de salida se
comprobarán también con DataFrames vacíos.

**Pruebas previstas y aceptación.** Todas las pruebas H05.2 están pendientes de
ejecución; los resultados históricos de H05.1 no se atribuirán a este incremento.
Se establecen tres niveles:

- **Nivel 1, desarrollo focalizado:** datos sintéticos para gramática y exactitud
  decimal, ocho estados numéricos, seis temporales, ausencias e indicadores,
  calendario, esquema, colisiones y conservación de originales.
- **Nivel 2, cierre obligatorio:** pruebas focalizadas completas, propiedades
  deterministas, igualdad de multiconjuntos originales, cardinalidad y
  multiplicidad de duplicados; referencia matemática decimal exacta e
  independiente del conversor, sin aproximaciones de coma flotante; fronteras,
  exponentes extremos, ceros con signo y ceros finales prescindibles; bisiestos,
  fechas inválidas, errores simultáneos y etiquetas nominales asociadas a DST;
  esquema y nulabilidad completos, incluida entrada vacía; determinismo frente
  a reordenaciones y reparticionados; configuraciones pertinentes de ANSI y
  zona horaria de sesión Spark; regresiones de H05.1 e integración con
  **10.000 filas largas sintéticas**. El caso propuesto parte de 5.000 filas
  Bronze y dos columnas de edificio, expandidas mediante H05.1.
- **Nivel 3, ampliación condicional:** ejecución separada con 100.000 filas
  largas sintéticas, según recursos disponibles. Los tiempos y consumos serán
  observaciones, sin demostrar escalabilidad ni imponer umbrales arbitrarios.

Entre los casos decimales obligatorios figurarán `M + 10^-19` → `overflow`,
`10^-19` → `underflow`, `1.1e-18` → `scale_exceeded` y cero matemático con
exponente válido extremo → `finite`. Se comprobarán ambos extremos del rango;
el cero negativo será `finite`, con `is_zero = true` e `is_negative = false`.

El cierre exigirá todas las pruebas obligatorias superadas, conservación exacta
de originales, tipos, nulabilidad y multiplicidad, clasificación numérica y
temporal correcta, rechazo de errores estructurales y colisiones, determinismo
y ausencia de persistencia o ejecución real.

**Límites y aprobación pendiente.** El escenario hotelero es ficticio y no
modifica los usos reales documentados en BDG2. La propuesta no cierra H04 ni
autoriza implementar H05.2: falta acreditar la ratificación conjunta detallada
del alcance y contrato. La ejecución BDG2 real requerirá aprobar cohorte,
variantes de origen, filtros, procedencia de la extracción del Excel, hashes y
unidades aplicables. La persistencia y publicación Delta necesitarán otro
contrato y autorización específica.

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
