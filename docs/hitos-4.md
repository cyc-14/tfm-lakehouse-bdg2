# Hito 4 — Selección del caso de estudio y contratos analíticos

**Estado:** metodología de selección contrastada con las tablas de la memoria y
selección de 16 edificios documentada; cierre de H04 pendiente de aprobación
conjunta de la selección y verificación de procedencia. H05.2 completó su
validación sintética obligatoria. No se ha autorizado la validación con BDG2
real ni la publicación Silver en Delta. **Responsabilidad:** Irantzu dirige
las decisiones analíticas; Carlos verifica su viabilidad técnica. **Objetivos:**
OE1, OE2, OE4. **Requisito previo:** caracterización aceptada en H03.

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
| [H03.2](h03-2-characterization.md) | Expediente `data/checks/h03_2/real/2026-10-30-h03-2/`, contrato histórico `h03.2-v1`. | Utilizó 3.000–7.000 m² y obtuvo 65 candidatos por superficie; no verifica el nuevo intervalo ni la selección final de 16 edificios. |
| [H03.3](h03-3-weather-timezones.md) | Expediente `data/checks/h03_3/real/2026-10-30-h03-3/`, contrato histórico `h03.3-v1`. | Diagnóstico meteorológico y temporal, sin unión horaria validada. |
| [H05](hitos-5.md) | H05.1 transforma a formato largo; H05.2 implementa e interpreta valores decimales y fechas nominales, con validación sintética completada. | No aprueba la selección H04 ni acredita validación con BDG2 real o publicación Silver en Delta. |

### Selección final: contraste visual con la memoria

Las capturas de las páginas aportadas de la memoria permiten cotejar el embudo
descrito en el apartado 2.2.6 y sus tablas 3 y 4: 168 edificios con
`primaryspaceusage` `Lodging/residential`, 107 tras el filtro de subcategorías,
24 con superficie entre 3.500 y 6.000 m² y 16 tras excluir los casos
con más del 8 % de lecturas ausentes. Este cotejo verifica lo que muestran las
tablas y el cálculo con sus conteos; no reproduce la selección desde los CSV
originales.

La memoria indica que solo tres de los 168 edificios tienen subcategoría `Hotel`
y menciona, además, `Dormitory`, `Lodging /residential` y
`Other - Lodging/residential`. H03.2 registra históricamente
`Hotel` (3), `Dormitory` (93), `Lodging/residential` (4) y
`Other - Lodging/Residential` (10): las cuatro categorías suman 110 y las tres
distintas de `Hotel`, 107. Según la aclaración comunicada por uno de los autores
el 07/11/2026, los tres edificios `Hotel` se excluyeron; esa exclusión concilia
aritméticamente los recuentos 110 y 107. Su justificación metodológica concreta
no se ha establecido, por lo que no se afirma que sus mediciones fueran
inadecuadas. No se ha reproducido el filtro desde los CSV originales ni se
alteran los resultados históricos de H03.2. La captura no precisa si el
intervalo de superficie incluye sus extremos.

La tabla 3 muestra 24 conteos de valores no ausentes y usa 17.544 horas como
denominador. Con la regla estricta
`100 × (17544 − count) / 17544 > 8`, se excluyen los conteos inferiores a
17.544 − 0,08 × 17.544 = 16.140,48, es decir, `count` ≤ 16.140. Los 16 edificios
restantes tienen `count` entre 17.489 y 17.544 (hasta 55 ausencias); los ocho
excluidos son:

| Edificio BDG2 | Valores no ausentes | Ausencias | % ausente mostrado |
| --- | ---: | ---: | ---: |
| `Cockatoo_lodging_Katharine` | 16.113 | 1.431 | 8,16 % |
| `Cockatoo_lodging_Lana` | 16.103 | 1.441 | 8,21 % |
| `Cockatoo_lodging_Alicia` | 16.061 | 1.483 | 8,45 % |
| `Cockatoo_lodging_Carmen` | 16.119 | 1.425 | 8,12 % |
| `Cockatoo_lodging_Eric` | 16.119 | 1.425 | 8,12 % |
| `Cockatoo_lodging_Olga` | 16.119 | 1.425 | 8,12 % |
| `Cockatoo_lodging_Linwood` | 16.119 | 1.425 | 8,12 % |
| `Cockatoo_lodging_Johnathan` | 16.119 | 1.425 | 8,12 % |

Así, los conteos enteros confirman el paso 24→16; los ocho excluidos pertenecen
al emplazamiento Cockatoo. Los porcentajes de la tabla, redondeados a dos
decimales, concuerdan con esos conteos. Esto verifica la aplicación aritmética
del umbral a los datos tabulados, no el cálculo original de las estadísticas.

La tabla 4 atribuye a los 16 seleccionados el tipo `Lodging/residential` y el
subtipo `Dormitory`. Sus identificadores, superficies, emplazamientos y zonas
horarias son:

| ID | Edificio BDG2 | Superficie (m²) | Emplazamiento | Zona horaria |
| --- | --- | ---: | --- | --- |
| `ID_020` | `Robin_lodging_Dorthy` | 5.824,9 | Robin | `Europe/London` |
| `ID_024` | `Robin_lodging_Pricilla` | 4.607,0 | Robin | `Europe/London` |
| `ID_026` | `Robin_lodging_Armand` | 5.246,0 | Robin | `Europe/London` |
| `ID_030` | `Fox_lodging_Wallace` | 5.885,3 | Fox | `US/Mountain` |
| `ID_032` | `Fox_lodging_Winifred` | 5.582,2 | Fox | `US/Mountain` |
| `ID_035` | `Fox_lodging_Warren` | 4.562,3 | Fox | `US/Mountain` |
| `ID_041` | `Peacock_lodging_Chloe` | 4.120,4 | Peacock | `US/Eastern` |
| `ID_042` | `Peacock_lodging_Terrie` | 4.367,1 | Peacock | `US/Eastern` |
| `ID_043` | `Peacock_lodging_Nova` | 4.507,6 | Peacock | `US/Eastern` |
| `ID_052` | `Bull_lodging_Leonard` | 3.831,4 | Bull | `US/Central` |
| `ID_053` | `Bull_lodging_Terence` | 3.780,9 | Bull | `US/Central` |
| `ID_055` | `Bull_lodging_Lettie` | 3.683,4 | Bull | `US/Central` |
| `ID_056` | `Bull_lodging_Carie` | 3.683,5 | Bull | `US/Central` |
| `ID_065` | `Bull_lodging_Jeremiah` | 5.326,5 | Bull | `US/Central` |
| `ID_067` | `Bull_lodging_Anibal` | 4.160,6 | Bull | `US/Central` |
| `ID_068` | `Bull_lodging_Danielle` | 4.188,8 | Bull | `US/Central` |

Los 16 nombres y su orden coinciden con las cabeceras B–Q del Excel revisado.
La coincidencia documental confirma que la selección enumerada allí es la de la
tabla 4. Aunque uno de los autores comunicó el 07/11/2026 haber comprobado la
correspondencia del Excel, la comparación celda a celda con las mediciones
originales queda pendiente de un procedimiento y resultados reproducibles.
La memoria describe el caso como una cadena hotelera ficticia y presenta los
edificios como residencias de estudiantes; no deben interpretarse como hoteles
reales. H03.2 registra
`Dormitory` para los ocho nombres que
coinciden con sus 14 perfiles, pero no se ha verificado aquí el subuso individual
de los otros ocho frente a metadatos originales.

Los 16 nombres aparecen en el conjunto histórico de 65 edificios candidatos
del intervalo 3.000–7.000 m². Ocho también figuran entre los 14
perfiles de aquella configuración: seis del grupo inicialmente seleccionado y
dos del grupo previamente descartado (`Bull_lodging_Leonard` y
`Bull_lodging_Jeremiah`). Los otros ocho no están entre esos 14. Son escenarios
y listas distintos; la exclusión histórica de dos candidatos no invalida por
sí sola su inclusión bajo el criterio comunicado para la memoria.

La zona horaria de la tabla 4 es metadato declarado: por sí sola no demuestra
la semántica de los timestamps ni resuelve cambios estacionales. No consta una
aprobación conjunta explícita de la selección final ni de la secuencia de
selección. La validación sintética de H05.2 no sustituye esa aprobación ni una
validación con BDG2 real.

H03.2 registra 17.544 etiquetas horarias nominales de 2016–2017. Su regularidad
no demuestra instantes físicos inequívocos ni resuelve DST. Los porcentajes de
ausencia usan registros recibidos; los de ceros, lecturas numéricas finitas.
Una baja ausencia no descarta episodios prolongados de ceros. Las señales y
extremos del expediente no confirman por sí solos errores de medida.

## Decisiones y verificaciones pendientes

- Reproducir en los CSV originales los criterios de selección documentados en
  la memoria y aprobar conjuntamente la selección de edificios, incluida la
  justificación
  de excluir `Hotel` y la interpretación de los límites de superficie.
- Verificar la variante, publicación, versiones y hashes de las fuentes y la
  procedencia de la extracción del Excel.
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

La interpretación decimal y temporal nominal ya están definidas por H05.2 v0.3
y validadas sintéticamente; este contrato no determina unidades físicas ni su
procedencia. La etiqueta original tampoco fija offsets, `fold` ni el inicio o
final de un intervalo energético. Siguen pendientes la aprobación de la
selección de edificios y sus fuentes, las reglas de calidad de negocio, la
integración contextual y un contrato específico para persistir Silver en Delta.
Los alias académicos y los identificadores ficticios deberán documentarse aparte
del identificador BDG2, sin reemplazarlo. Las banderas de calidad y la aptitud para un indicador son
decisiones distintas; no se autorizan exclusiones, imputaciones ni conversiones
UTC implícitas.

### Contrato H05.2 v0.3

El contrato define una transformación posterior a H05.1. Según la comunicación
del primer autor, ambos autores han confirmado conjuntamente el diseño v0.3 y
el alcance de las pruebas sintéticas. Las entregas autorizadas comprenden la
interpretación numérica y temporal nominal con pruebas sintéticas.

| Aspecto | Estado |
| --- | --- |
| Diseño v0.3 y pruebas sintéticas | Confirmados por ambos autores, según comunicación del primer autor |
| Implementación H05.2 | Etapas numérica y temporal nominal implementadas |
| Validación sintética obligatoria | Completada: integración de 10.000 filas largas; detalle y regresiones indicados abajo |
| Prueba sintética opcional de 100.000 filas | No ejecutada |
| Validación BDG2 real y persistencia/publicación Delta | Pendientes de aprobación y autorización específicas |

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

**Pruebas y aceptación del contrato.** La verificación sintética obligatoria del
contrato H05.2 se completó en el incremento H05.2.3: la integración de 10.000
filas largas y la selección H05 superaron 127 pruebas; se excluyó
intencionadamente una prueba de escritura Delta. Otras 65 pruebas sintéticas de
fuentes y configuración también pasaron. La revisión independiente no dejó
hallazgos accionables; el detalle consta en `docs/hitos-5.md`. La prueba
condicional de 100.000 filas no se ejecutó. Esta verificación no comprende
procesamiento BDG2 real ni publicación Delta, que requieren autorizaciones
separadas. Los resultados
históricos de H05.1 no se atribuyen a H05.2. Se establecieron tres niveles:

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
y ausencia de persistencia o ejecución real. Estos criterios quedaron
verificados sintéticamente en H05.2.3; la ejecución BDG2 y la publicación Delta
permanecen pendientes de autorización específica.

**Límites y autorizaciones pendientes.** El escenario hotelero es ficticio y no
modifica los usos reales documentados en BDG2. H05.2 completó su alcance
sintético, pero H04 no está cerrado: ambos autores deben aprobar conjuntamente
la selección de edificios. Quedan por verificar con los originales su extracción
y procedencia, la publicación y versiones fuente, así como las unidades
aplicables y su origen.

La ejecución real requiere autorización específica. No se autorizan la
integración meteorológica ni el desarrollo Gold; la persistencia Silver en
Delta necesita además contrato y autorización propios.

## Actividades incrementales

1. Reproducir la selección documentada con las fuentes originales y aprobar
   conjuntamente la selección de edificios, dejando constancia de los elegidos y
   descartados.
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

La hoja `Hoja1` contiene 17.544 filas de datos, `timestamp` y las 16 columnas
enumeradas arriba. Sus cabeceras coinciden con las salidas guardadas del
cuaderno. Los 16 recuentos de ausencia guardados coinciden con el campo
`missing` del informe histórico H03.2 para esos edificios. Es un contraste
agregado; no comprueba identidad celda a celda con los originales ni permite
verificar los ocho excluidos del conjunto de 24.

Según la información comunicada por el autor, Irantzu preparó manualmente el
Excel examinando las fuentes BDG2 y seleccionando los datos de los edificios
relevantes. No se ha documentado ni reproducido esa extracción. El cuaderno
carga el Excel con pandas y contiene estadísticas descriptivas, recuentos de
ausencias, señales IQR y diagramas de caja; no contiene una selección desde los
CSV ni instrucciones de generación o exportación del libro. Las opciones de
imputación están comentadas. Las salidas guardadas no son una ejecución nueva
ni demuestran el flujo Silver.

Quedan por documentar las fuentes, variantes y hashes usados para preparar el
Excel, los pasos manuales, filtros y posibles cambios de orden o valores. Los
criterios de selección están documentados en la memoria, pero su aplicación no
se ha reproducido desde los CSV. Las coincidencias de cabeceras y ausencias
respaldan consistencia parcial. La comprobación adicional comunicada por el
autor el 07/11/2026 no incluye aquí un procedimiento ni resultados reproducibles
de comparación celda a celda; esa verificación sigue pendiente. El nombre del
cuaderno no acredita aprobación de contratos ni de la selección de edificios.

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
- `[H04] Aprobar esquema Silver persistente y semántica temporal por fuente`
- `[H04] Aprobar criterios de calidad e integración`

## Límites y condición de avance

H05.1 y H05.2 se autorizaron por separado y completaron sus alcances sintéticos;
H05 formal sigue pendiente de los contratos y tareas restantes, incluida la
persistencia. H04 mantiene pendiente la aprobación conjunta de la selección de
edificios y la verificación de procedencia necesaria para cualquier ejecución
real. Esto no autoriza fases posteriores. La integración meteorológica queda para
[H06](hitos-6.md), con semántica temporal y cardinalidad justificadas.
