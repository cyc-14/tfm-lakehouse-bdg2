# Hito 4 — Selección del caso de estudio y contratos analíticos

**Estado:** metodología de selección contrastada con las tablas de la memoria y
selección de 16 edificios aprobada conjuntamente; cierre de H04 pendiente de
reproducción y verificación de procedencia. H05.2 completó su
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
24 dentro del intervalo inclusivo de 3.500–6.000 m² y 16 tras excluir los casos
con más del 8 % de lecturas ausentes. Este cotejo verifica lo que muestran las
tablas y el cálculo con sus conteos; no reproduce la selección desde los CSV
originales.

La memoria indica que solo tres de los 168 edificios tienen subcategoría `Hotel`
y menciona, además, `Dormitory`, `Lodging /residential` y
`Other - Lodging/residential`. H03.2 registra históricamente
`Hotel` (3), `Dormitory` (93), `Lodging/residential` (4) y
`Other - Lodging/Residential` (10): las cuatro categorías suman 110 y las tres
distintas de `Hotel`, 107. Según la confirmación conjunta recibida para
H04.14.A2, ambos autores aprueban incluir exactamente las tres subcategorías
citadas y excluir `Hotel` y las restantes dentro de `Lodging/residential`.
Los recuentos 93, 4 y 10 son referencias históricas de H03.2, no resultados del
nuevo verificador. La justificación académica concreta de excluir `Hotel`
sigue pendiente. No se afirma que sus mediciones fueran inadecuadas ni se
alteran los resultados históricos de H03.2.

Según la confirmación comunicada para H04.14.B0, ambos autores aprueban los 16
identificadores enumerados en la tabla 4. Según la confirmación conjunta recibida
para H04.14.A2, ambos autores aprueban el uso de
`sqm` con el intervalo inclusivo `3500 <= sqm <= 6000`, calculado con
aritmética decimal exacta. `sqm` es el campo
de metadatos observado en el código y la documentación H03; la expresión resume
el criterio aprobado, pero su aplicación final no se ha reproducido desde los
metadatos BDG2 originales. El código H03 consultado conserva los límites de su
escenario histórico, por lo que no constituye implementación del filtro final
H04.

La tabla 3 muestra 24 conteos de valores no ausentes y usa 17.544 horas nominales
como denominador. El autor informante confirma la regla estricta
`100 × (17544 − count) / 17544 > 8`: exactamente un 8 % no activa la exclusión.
Se excluyen los conteos inferiores a 17.544 − 0,08 × 17.544 = 16.140,48, es
decir, `count` ≤ 16.140. Los 16 edificios restantes tienen `count` entre 17.489
y 17.544 (hasta 55 ausencias); los ocho excluidos son:

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

Así, los conteos enteros de la tabla confirman aritméticamente el paso 24→16;
los ocho excluidos pertenecen al emplazamiento Cockatoo. Los porcentajes,
redondeados a dos decimales, concuerdan con esos conteos. Esta comprobación de
la tabla no reproduce el cálculo original desde los ficheros fuente.

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
la semántica de los timestamps ni resuelve cambios estacionales. La aprobación
conjunta comunicada en H04.14.B0 ratifica los 16 identificadores de la tabla;
la secuencia aplicada y la procedencia todavía deben reproducirse con las fuentes.
La validación sintética de H05.2 no equivale a una validación con BDG2 real.

### Verificador sintético H04.14.A2

`src/tfm_lakehouse/selection/building_selection.py` aplica a registros y resúmenes
proporcionados explícitamente el filtro exacto de uso, las tres subcategorías
aprobadas, el intervalo de superficie inclusivo y la regla estricta de
ausencias mediante aritmética entera. Valida esquemas e identificadores,
conserva los IDs en resultados y exclusiones, informa recuentos por etapa y no
modifica las entradas. Sus pruebas son exclusivamente sintéticas. Los conteos
históricos 168 → 107 → 24 → 16 no se presentan como una ejecución del
verificador.

El verificador recibe `non_missing_count` ya clasificado para los edificios
que superan el filtro de superficie; usa el denominador fijo de 17.544 y excluye
solo cuando `100 × missing_count > 8 × 17544` (1.403 ausencias pasan; 1.404
excluyen). No interpreta CSV ni Excel ni reproduce o valida los datos originales.

### Comparación sintética Excel–BDG2 H04.14.A3

Según la confirmación conjunta recibida para H04.14.A3, ambos autores aprueban
R5: son ausencias los valores null, la cadena vacía exacta y el texto exacto
`NaN`. Los ceros y negativos finitos no son ausencias; el texto no numérico se
diagnostica por separado, sin recortes, normalización ni imputación. Un null
producido por un lector CSV se distingue de un null explícito: el primero no
permite recuperar el token serializado original.

También aprueban R6: contrastar las 16 series del Excel exploratorio con las
series BDG2 originales correspondientes, con 17.544 observaciones horarias y
280.704 posiciones de valor esperadas. La comparación requiere identidad de
edificio y alineación temporal; solo admite representaciones numéricas distintas
si se demuestra su igualdad exacta, sin redondeo ni tolerancias. Debe informar
diferencias de ausencia, fechas, duplicados y ambigüedades.

El núcleo en `src/tfm_lakehouse/comparison/excel_bdg2_comparison.py` recibe registros en
memoria y celdas tipadas. Valida columnas e identificadores, etiquetas nominales,
cadencia y cobertura, y compara por separado el orden. Si hay etiquetas inválidas,
la igualdad de cobertura queda indeterminada. Conserva la multiplicidad y contrasta
los grupos de fechas repetidas como multiconjuntos de filas completas; no atribuye
sus celdas a parejas y mantiene la alineación como ambigua. Los resultados por
celda y las diferencias de representación solo cuentan posiciones comparadas;
se informan aparte las posiciones sin pareja. Utiliza `Decimal` para
valores exactos aportados como tales, distingue equivalencia de ausencias y
diferencias de representación, y limita los ejemplos de incidencias. Los valores
`float` y las fórmulas quedan como ambiguos, sin evaluar cachés. La exactitud de
una celda declarada exacta depende de la evidencia que aporte quien la suministra.
El resultado se refiere a los registros y dimensiones suministrados, no acredita
procedencia ni identifica por sí solo el periodo correcto.

La validación ejecutada en H04.14.A3 comprende 24 casos sintéticos del comparador
y 26 del verificador de selección. En aquel incremento no había lector XLSX
instalado ni declarado y faltaba un adaptador para el tipo y la representación
de cada celda, así como su política de fórmulas y precisión. El núcleo de
comparación no lee archivos y, por sí solo, no verifica ambos orígenes.
La comparación real requiere verificar variante, hashes e identidad de las
fuentes y de la publicación, establecer el periodo y autorizar expresamente la
ejecución con rutas y efectos revisados. Sigue pendiente el contraste con los
originales; la selección de los 16 identificadores se aprobó después en H04.14.B0.

### Adaptador CSV sintético H04.14.A4

`src/tfm_lakehouse/comparison/comparison_csv.py` lee CSV UTF-8 con BOM opcional y sintaxis
estándar de comillas, mediante un iterador de solo lectura. Exige `timestamp` y
los identificadores solicitados con coincidencia exacta; admite otras columnas de
origen, pero valida la cabecera completa y la anchura de cada registro. Rechaza
cabeceras vacías, duplicadas o ambiguas, registros de anchura incorrecta y errores
de sintaxis detectados por el lector CSV de Python, que no comprueba todos los
casos contrarios a RFC 4180. Una línea física vacía se rechaza por anchura cero;
Bronze documenta que las omite. Devuelve las columnas elegidas en el
orden original de las filas, incluidos duplicados, como `Cell("text", valor)`
para el comparador. Conserva
campos vacíos y `NaN` exactos sin pasar por `float`; los valores son los del
lector CSV, no una copia de su serialización. La validación completa exige
consumir el iterador entero; el comparador actual materializa los registros.

En H04.14.A4 se han superado 24 casos sintéticos del adaptador y 50 casos de
regresión del comparador y la selección. Una prueba integra el CSV sintético con
celdas preparadas en memoria; no se comparó ningún libro real. En aquel momento
`openpyxl` no estaba instalado ni declarado y quedaba pendiente el adaptador XLSX.
Las aprobaciones conjuntas R5 y R6 y las condiciones de procedencia y
autorización indicadas arriba continúan vigentes.

### Adaptador XLSX sintético H04.14.A5

`src/tfm_lakehouse/comparison/comparison_xlsx.py` lee una hoja y una lista de edificios
proporcionadas explícitamente, con `Hoja1` y 17.544 filas como valores por
defecto. Exige cabeceras exactas, una columna temporal y las columnas indicadas;
rechaza columnas adicionales, filas ausentes y estructuras no admitidas. Conserva
orden y duplicados, proporciona los registros al comparador y mantiene aparte
las representaciones observadas. Usa `openpyxl` 3.1.5 y su dependencia
`et-xmlfile` 2.0.0, fijadas en el entorno y las declaraciones del proyecto.

Los valores numéricos se recuperan del token OOXML guardado y se interpretan
mediante `Decimal`, sin utilizar el `float` de `openpyxl` ni aplicar tolerancias.
La igualdad se refiere al token guardado y no demuestra que Excel conservara el
valor anterior al guardado. Las fórmulas son ambiguas y sus cachés no se usan;
los errores de Excel se conservan como diagnóstico no numérico. Se distinguen
el texto vacío y una celda ausente. El lector no ejecuta fórmulas ni macros y
desactiva los enlaces externos. Rechaza cadenas compartidas que el lector
normalizaría y celdas tipadas cuya estructura no permite interpretarlas.

Las fechas de texto se conservan como etiquetas nominales. Para fechas Excel se
aplica el sistema 1900 o 1904 al serial guardado y se rechaza el día ficticio
1900-02-29. En H04.14.A5 se exigían segundos matemáticamente exactos y se
rechazaban seriales habituales para horas como las 01:00; H04.14.A5.1 sustituye
esa restricción por el criterio sintético descrito abajo. Las fechas numéricas
sin formato de fecha, los instantes con zona y las fechas en celdas de medida
quedan fuera del alcance. Las fracciones de segundo no quedan verificadas.
Las fechas ISO tipadas deben incluir fecha y hora con segundos
completos. No se deduce UTC ni se resuelve DST. El lector limita tamaño
y expansión ZIP, rechaza declaraciones de entidades XML y solo admite partes
XML y RELS; por ello tampoco acepta libros con imágenes, macros u otras partes
binarias. Se mantiene esta restricción para evitar que el lector procese partes
que no han pasado la validación XML; no se ha verificado una excepción segura
para imágenes en este incremento.

La validación H04.14.A5 ha superado 28 casos sintéticos XLSX y 74 pruebas de
regresión de CSV, comparación y selección. La equivalencia del libro real, la
identidad y hashes de las fuentes y la autorización expresa para procesarlos
siguen pendientes. Los 16 identificadores se aprobaron después en H04.14.B0.

### Decodificación temporal sintética H04.14.A5.1

Un serial de Excel es un número de días; fracciones como 1/24 no tienen
representación decimal finita. Excel documenta precisión de 15 cifras y uso de
doble precisión binaria ([Microsoft](https://learn.microsoft.com/en-us/troubleshoot/microsoft-365-apps/excel/floating-point-arithmetic-inaccurate-result)).
Como criterio técnico conservador para este incremento, se acepta el segundo
nominal más cercano solo cuando la diferencia absoluta, calculada con fracciones
exactas, no supera `86400 × (0,5 × 10^(e10 − 14) + 2^(e2 − 52))` segundos,
donde `e10 = suelo(log10(serial))` y `e2 = suelo(log2(serial))` para un serial
positivo. El primer término cubre media unidad decimal de 15 cifras; el segundo,
una unidad de precisión binaria64. El serial cero exacto no necesita ajuste.
Por ejemplo, para un serial cercano a 43.831 el límite es aproximadamente
4,95 microsegundos. Este margen es una política de interpretación, no una prueba
de que el valor original careciera de fracciones de segundo.

El adaptador conserva el token y registra si hubo ajuste, junto con el error y el
límite como fracciones exactas de segundo. Los casos fuera del margen se rechazan;
no se convierten seriales que pertenecen al día ficticio ni ajustes que entren en
él. `compare_xlsx_electricity` compara las etiquetas nominales y conserva ese
resultado por separado, pero deja la verificación global en falso si se ajustó
alguna fecha: un desplazamiento real inferior al margen no puede distinguirse de
un error de representación. La salida incluye el número de fechas ajustadas y
ejemplos limitados. Pasar `records` directamente al núcleo omite esta protección;
el resultado del núcleo por sí solo no certifica el tiempo original. La tolerancia
temporal no se aplica a los consumos, que se comparan exactamente. H04.14.A5.1 ha
superado 53 casos sintéticos XLSX y 74 regresiones de CSV, comparación y selección.
Según la confirmación comunicada para H04.14.B0, ambos autores aprueban recuperar
la etiqueta horaria nominal mediante diferencias de serialización pequeñas y
rigurosamente acotadas, registrando cada ajuste y manteniendo exacta la igualdad
de consumos. Esta confirmación general no prueba por sí sola la semántica de las
fechas del libro ni la equivalencia de sus datos. La procedencia y la ejecución
real siguen pendientes de verificación y autorización específicas.

### Preauditoría H04.14.B0

La selección conjunta comprende exactamente los 16 identificadores de la tabla
4. Las aprobaciones comunicadas no acreditan la equivalencia con los originales
ni autorizan su lectura. El flujo permanece **no preparado para certificar una
comparación real reproducible** hasta resolver estos controles:

- Vincular las identidades y hashes esperados a los bytes realmente leídos y
  verificar su estabilidad. B2 implementa un vínculo sintético con los bytes
  parseados; la procedencia y estabilidad posteriores siguen sin verificarse.
- Comprobar por separado la rejilla horaria y los 16 identificadores aprobados;
  el perfil `bdg2` del núcleo identifica dimensiones, no periodo ni lista. B1
  implementa estos controles sintéticamente; falta aplicarlos a fuentes reales.
- Obtener y justificar de forma reproducible los conteos R5 suministrados al
  verificador de selección y contrastar su salida con los 16 aprobados. B2
  implementa y prueba ese cálculo con fuentes sintéticas.
- Verificar en una fase autorizada la estructura y procedencia del libro. El
  rechazo actual de partes binarias y los límites del lector pueden impedir su
  lectura; esta auditoría no comprueba si el libro contiene esas partes.

La igualdad de etiquetas nominales, las posiciones de consumo comparadas y la
verificación global son resultados distintos. Los ajustes temporales mantienen
`verified=False`; `nominal_comparison_verified` resume el núcleo completo, no
es un indicador independiente de igualdad de consumos. Hay que conciliar todas
las posiciones y registrar las ausencias, discrepancias y ambigüedades.

### Perfil estricto sintético H04.14.B1

`src/tfm_lakehouse/comparison/bdg2_comparison.py` define una única lista ordenada con los
16 identificadores aprobados. El perfil exige esa lista, la hoja `Hoja1` y el
orden original de su cabecera; el CSV ancho puede contener otras columnas, que
el adaptador proyecta sin incorporarlas a la selección. La rejilla nominal va
del 01/01/2016 00:00:00 al 31/12/2017 23:00:00, incluye el 29 de febrero y
contiene 17.544 etiquetas y 280.704 posiciones para los 16 edificios.

El perfil comprueba cada fuente frente a la rejilla y comunica por separado
ausencias, etiquetas adicionales, duplicados y diferencias de orden, con
recuentos exactos y ejemplos limitados. Distingue estructura, acuerdo temporal
nominal, equivalencia de valores en posiciones emparejadas y ajustes Excel;
informa el número y el máximo error de estos ajustes. Los grupos duplicados se
comparan como multiconjuntos de filas completas, sin inventar parejas de celdas.
Los valores eléctricos mantienen la comparación decimal exacta y R5.

La elegibilidad de los registros suministrados es distinta de la verificación
final: esta permanece en falso porque el perfil no vincula los datos a hashes ni
acredita la identidad de las fuentes. También permanece en falso si existen
ajustes temporales, discrepancias o ambigüedades. Los controles se han probado
exclusivamente con datos sintéticos; no reproducen la selección BDG2 ni prueban
equivalencia con el libro real. Siguen pendientes el vínculo con los hashes, la
derivación reproducible de conteos R5, la compatibilidad del XLSX real y la
autorización explícita para cualquier lectura de fuentes reales.

La verificación de B1 ha superado 25 casos sintéticos del perfil estricto y 127
pruebas de regresión de los adaptadores, el comparador y la selección.

### Procedencia y selección sintéticas H04.14.B2

`source_provenance.py` calcula SHA-256 sobre los bytes que recibe el parser CSV;
solo devuelve su identidad cuando el parser ha terminado, se ha alcanzado el fin
del flujo y coincide la huella esperada suministrada por quien llama. El tamaño
máximo es explícito y la lectura es por bloques. El cálculo eléctrico hace dos
pasadas sobre el mismo flujo: primero valida la rejilla completa y después cuenta
las celdas; ambas pasadas deben consumir bytes con la misma huella esperada. Un
error, lectura incompleta o huella distinta impide devolver el resultado.

Para XLSX se comprueba el SHA-256 de una instantánea inmutable y acotada, y el
adaptador interpreta esos mismos bytes. Estas garantías identifican el contenido
parseado: no demuestran por sí solas el origen declarado ni que una ruta o archivo
permanezcan inalterados después de la lectura. El parser facilitado debe procesar
completamente el flujo y no publicar resultados provisionales. No se han obtenido
huellas nuevas de fuentes reales ni se han creado copias persistentes de ellas.

`selection_reproduction.py` reutiliza los filtros R1–R3 y, tras validar la
rejilla, clasifica con R5 las celdas de los edificios que superan esos filtros;
no evalúa la calidad de las demás columnas eléctricas. Conserva conteos de
ausencias, inválidos, ambiguos, ceros y negativos; compara el umbral R4 con
aritmética entera: 1.403 ausencias pasan y 1.404 excluyen. Los valores inválidos
o ambiguos no se convierten en ausencias y bloquean la elegibilidad. El resultado
registra cada etapa, exclusiones y diferencias de conjunto y orden frente a los
16 aprobados.
El embudo histórico 168 → 107 → 24 → 16 es una referencia, no un resultado
atribuido a las fuentes sintéticas.

`synthetic_selection_workflow.py` integra identidades, selección y comparación
estricta para flujos suministrados expresamente. Conserva una traza completa de
ajustes temporales, separada de los ejemplos limitados, y deja `verified=False`:
una coincidencia de huellas suministradas y un resultado sintético elegible no
acreditan procedencia real ni equivalencia de los originales. La vía no constituye
una autorización ni un comando de ejecución real. Siguen pendientes la identidad
y procedencia reales, la compatibilidad del libro, el contraste de las fuentes y
la autorización específica antes de procesarlas.

La validación de B2 ha superado 183 pruebas sintéticas y regresiones, incluida
una integración inventada con 17.544 filas y 16 edificios; sus resultados no
constituyen evidencia de una ejecución sobre BDG2 real.

### Preparación controlada sintética H04.14.B3.A

`controlled_synthetic_verification.py` ofrece una evaluación en memoria para
fuentes declaradas sintéticas. Exige tres identificadores de fuente distintos,
tres SHA-256 esperados y límites de bytes explícitos, versión de fixture,
`Hoja1`, la lista ordenada aprobada y la rejilla nominal completa. Solo acepta
objetos `BytesIO` y rechaza la presencia de `TFM_REAL_CONFIG` y cualquier destino
de evidencia. No abre rutas ni persiste informes; que los bytes suministrados en
memoria sean realmente sintéticos depende de quien llama.

El informe separa configuración, hashes y esquemas, filtros, rejilla, conteos,
conciliación, estructura XLSX y comparación. Cada etapa queda como superada,
fallida, pendiente o no ejecutada según corresponda. Los digest comprobados se
identifican incluso si una etapa posterior falla. Incluye embudo, edificios,
conteos R5, cobertura, posiciones comparadas, resultados exactos o ambiguos y
traza de ajustes. Los ejemplos de discrepancia son limitados y omiten valores de
consumo; no se escriben mensajes ordinarios con mediciones. Los avisos del
lector se cuentan sin reproducir su contenido y dejan la evaluación pendiente.

La coincidencia nominal y de valores se informa por separado de la elegibilidad
estricta. Los ajustes temporales siguen visibles y mantienen `verified=False`;
la identidad de serialización temporal, el origen de las fuentes y la estabilidad
posterior de las rutas no quedan acreditados. La verificación completa permanece
en falso, incluso si los datos sintéticos coinciden. Se validan la hoja y el orden
de cabecera XLSX también cuando falla la conciliación de edificios.

La validación de B3.A ha superado 203 pruebas y regresiones sintéticas, incluida
la comparación completa de 280.704 posiciones inventadas. No se han procesado
fuentes reales.

Para preparar una ejecución real futura haría falta, como contrato separado:

1. Facilitar y revisar las rutas exactas del CSV de metadatos, del CSV eléctrico
   BDG2 v1.0 `raw` y del XLSX exploratorio, con el SHA-256 esperado de cada uno.
2. Confirmar variante y versión, `Hoja1`, los 16 identificadores aprobados y el
   periodo nominal de 2016–2017; comprobar el vínculo entre las huellas y los
   bytes leídos, así como la procedencia y estabilidad de cada fuente.
3. Obtener autorización explícita para esa ejecución y sus efectos, además de
   la configuración `TFM_REAL_CONFIG` exigida por el proyecto. Esta preparación
   no implementa ni concede ese acceso.
4. Si se autorizan salidas, revisar por separado un destino privado exacto y sin
   solapamiento con las entradas. Sin destino autorizado no se generarían
   informes persistentes; la vía sintética actual no admite ningún destino.
5. Detener la certificación ante errores de identidad, esquema, selección,
   calendario o valores; conciliar las 280.704 posiciones y conservar por
   separado ausencias, duplicados, discrepancias y ajustes temporales.

La compatibilidad del libro real no se ha comprobado en esta fase: nombre y
cabecera de hoja, tipos de fecha, números, fórmulas, partes binarias y límites
de tamaño podrían exigir revisión. Las huellas suministradas identifican bytes,
no acreditan su origen; tampoco se ha diseñado ni autorizado todavía un acceso
real con vínculo de ruta y fuente durante toda la ejecución. **NO-GO para la
ejecución real** hasta resolver estas condiciones y revisar el procedimiento.

### Entrada controlada H04.14.B3.B1

La entrada `verify_real_sources` recibe rutas absolutas, tres SHA-256 esperados,
límites de bytes, versión y variante, hoja, selección y periodo explícitos. Exige
`TFM_REAL_CONFIG` con una configuración del proyecto que coincida con los dos
CSV, y una declaración separada de autorización para ese alcance y las
operaciones de lectura, cálculo de huellas y comparación en memoria. La
declaración refleja lo indicado por quien llama: no demuestra por sí sola una
aprobación humana. No hay rutas de datos incorporadas ni salida persistente.

La configuración y las fuentes se abren en lectura sin seguir enlaces; se
rechazan alias, se comprueba la estabilidad observable y los parsers reciben los
mismos bytes vinculados a las huellas. Las comprobaciones no prueban el origen
externo ni la estabilidad posterior. El sistema de archivos podría actualizar
la fecha de acceso. Un fallo, discrepancia o ambigüedad mantiene la verificación
global sin acreditar. Las salidas diagnósticas omiten valores de consumo.

La validación de esta entrada y sus regresiones ha superado 280 pruebas con
datos sintéticos, incluida una integración de 17.544 filas y 16 edificios en
archivos temporales. No se han leído ni comparado fuentes reales.

La compatibilidad real del libro y las rutas concretas permanecen sin comprobar.
Antes de cualquier ejecución real se necesitan una revisión técnica completa,
las rutas y huellas confirmadas, las decisiones académicas aplicables y una
autorización humana explícita para esa ejecución y sus efectos. La aprobación
metodológica anterior no autoriza la lectura.

H03.2 registra 17.544 etiquetas horarias nominales de 2016–2017. Su regularidad
no demuestra instantes físicos inequívocos ni resuelve DST. Los porcentajes de
ausencia usan registros recibidos; los de ceros, lecturas numéricas finitas.
Una baja ausencia no descarta episodios prolongados de ceros. Las señales y
extremos del expediente no confirman por sí solos errores de medida.

## Decisiones y verificaciones pendientes

- Reproducir en los CSV originales la secuencia de selección documentada y
  contrastar su resultado con los 16 identificadores aprobados conjuntamente.
- Documentar la justificación académica de excluir la subcategoría `Hotel`.
- Acordar la política conservadora de preservar y señalar lecturas, y el
  tratamiento de registros malformados, etiquetas repetidas y exclusiones
  analíticas.
- Decidir conjuntamente si se adopta un análisis estadístico de valores
  atípicos y con qué propósito.
- Verificar variante, publicación, versiones y hashes de las fuentes, unidades
  aplicables y procedencia de la extracción del Excel.
- Para cerrar H04, aportar reproducción desde las fuentes originales y contraste
  reproducible de la selección con la aprobación conjunta comunicada. Esta
  evidencia no autoriza por sí sola ejecución real ni publicación Silver.
- Correspondencia entre edificios BDG2 y establecimientos simulados, sin
  alterar identificadores ni afirmar que constituyen hoteles reales.
- Granularidad de electricidad, claves, semántica del medidor, unidades,
  horarios locales, zonas horarias y tratamiento de horas duplicadas o
  inexistentes por cambios de horario.
- Reglas de integración de metadatos y meteorología, cardinalidades válidas
  y tratamiento de correspondencias no resueltas.

La persistencia Silver en Delta, la integración meteorológica y Gold son
decisiones e incrementos posteriores. La aprobación de un método de detección de
valores atípicos no es requisito para completar el alcance sintético H05.2.

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
final de un intervalo energético. Siguen pendientes la verificación de la
selección aprobada y sus fuentes, las reglas de calidad de negocio, la
integración contextual y un contrato específico para persistir Silver en Delta.
Los alias académicos y los identificadores ficticios deberán documentarse aparte
del identificador BDG2, sin reemplazarlo. Las banderas de calidad y la aptitud para un indicador son
decisiones distintas; no se autorizan exclusiones, imputaciones ni conversiones
UTC implícitas.

### Política provisional de calidad de lecturas

La tabla separa las capacidades ya implementadas en H05.2 de los tratamientos
metodológicos propuestos. Las propuestas requieren aprobación conjunta y no
amplían el contrato ni la implementación actuales.

| Situación | Criterio de detección | Tratamiento propuesto | Estado y decisión pendiente |
| --- | --- | --- | --- |
| Ausencia de lectura | Estados `missing_null` o `missing_token` en `value_parse_status`, correspondientes a NULL, `""` o `"NaN"` exactos en el valor recibido. | Conservar el original y señalar `is_missing`; no imputar ni eliminar filas automáticamente. | Clasificación e indicador H05.2 implementados y validados sintéticamente. Acordar conjuntamente la política de conservación. La ausencia usada para seleccionar edificios se calcula sobre sus conteos y 17.544 observaciones nominales; no es el indicador por lectura. |
| Cero | Valor con estado `finite` e indicador `is_zero = true`. | Conservar la medición y señalarla; frecuencia y persistencia temporal son análisis separados. Un cero no implica error. | Indicador H05.2 implementado y validado sintéticamente. Análisis de frecuencia y rachas no implementado; acordar si procede. |
| Valor negativo | Valor con estado `finite` e indicador `is_negative = true`. | Conservar y señalar; evaluar el contexto antes de calificarlo como error. No sustituirlo por cero ni eliminarlo automáticamente. | Indicador H05.2 implementado y validado sintéticamente. El tratamiento analítico requiere aprobación conjunta. |
| Valor inválido o no representable | Estados H05.2 distintos de `finite` y de los estados de ausencia: sintaxis inválida, literal no finito, desbordamiento, subdesbordamiento o escala excedida. | Conservar la cadena original y tratarlo como problema de interpretación, no como medición válida ni como ausencia. | Estados de parseo H05.2 implementados y validados sintéticamente. Acordar el tratamiento analítico y de registros malformados. |
| Ausencia o etiqueta temporal no válida | Cualquier estado de `timestamp_parse_status` distinto de `valid_nominal`, incluidos `missing_null`, `missing_empty`, `invalid_format`, `invalid_calendar` e `invalid_time`. | Conservar el texto original; no inferir UTC, resolver ambigüedad DST ni construir instantes físicos. | Clasificación nominal H05.2 implementada y validada sintéticamente. Una política de tiempo físico requiere decisión separada. |
| Etiquetas edificio/tiempo repetidas | Diagnóstico propuesto de combinaciones repetidas de `building_id` y etiqueta nominal temporal. | Preservar todas las apariciones y multiplicidad; no deduplicar automáticamente. La repetición nominal no demuestra duplicación en origen ni equivalencia de tiempo físico. | H05.1 preserva multiplicidad y H05.2 conserva etiquetas; diagnóstico específico no implementado. Acordar revisión y posible elegibilidad analítica. |
| Valores atípicos estadísticos | Método estadístico aún por definir; el EDA documenta señales exploratorias IQR. | Análisis futuro; una señal atípica no confirma un error de medida. No corregir, eliminar, winsorizar ni imputar por este criterio. | No implementado y no aprobado. Definir conjuntamente el método y su uso antes de desarrollarlo. |

Antes de adoptar un análisis IQR u otro método se deben acordar: cálculo por
edificio o agrupado; periodo y población de referencia; método exacto de
cuantiles; multiplicador (por ejemplo, 1,5 × IQR); inclusión de ausencias,
ceros y negativos en la referencia; significado y uso de la señal; y necesidad
de análisis por estacionalidad o patrón operativo. No se autoriza corregir,
excluir, winsorizar o imputar mediciones como consecuencia de una señal.

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
sintético, pero H04 no está cerrado: la selección aprobada conjuntamente debe
contrastarse con los originales. Quedan por verificar su extracción
y procedencia, la publicación y versiones fuente, así como las unidades
aplicables y su origen.

La ejecución real requiere autorización específica. No se autorizan la
integración meteorológica ni el desarrollo Gold; la persistencia Silver en
Delta necesita además contrato y autorización propios.

## Actividades incrementales

1. Reproducir la selección documentada con las fuentes originales y contrastar
   los edificios elegidos y descartados con la selección aprobada conjuntamente.
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
relevantes. El autor informa de que ese trabajo apoyó la selección y analizó
estadísticas descriptivas, ausencias, ceros y posibles valores atípicos sin
modificar las mediciones originales. No se ha verificado aquí contra los ficheros
fuente. El cuaderno
carga el Excel con pandas y contiene estadísticas descriptivas, recuentos de
ausencias, señales IQR y diagramas de caja; no contiene una selección desde los
CSV ni instrucciones de generación o exportación del libro. Las opciones de
imputación están comentadas. Las salidas guardadas no son una ejecución nueva
ni demuestran el flujo Silver.

El Excel y el cuaderno pueden servir de apoyo exploratorio y referencia para
contrastes agregados, pero no sustituyen una publicación Bronze verificada ni
acreditan equivalencia completa, celda a celda, con BDG2. Silver debe partir de
la publicación Bronze cuya procedencia y versión se hayan verificado.

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
persistencia. H04 mantiene pendientes la reproducción de la selección aprobada
y la verificación de procedencia necesaria para cualquier ejecución
real. Esto no autoriza fases posteriores. La integración meteorológica queda para
[H06](hitos-6.md), con semántica temporal y cardinalidad justificadas.
