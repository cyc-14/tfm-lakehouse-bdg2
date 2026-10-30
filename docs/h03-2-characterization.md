# H03.2 — Caracterización reproducible de electricidad BDG2

## Alcance y estado de la evidencia

Herramienta local, por lotes y de solo lectura para caracterizar CSV originales.
Implementa el diseño aprobado de H03.2. Su existencia no acredita por sí sola una
ejecución real ni el cierre del hito: los resultados de cada ejecución se guardan
en un directorio privado, con sus hashes y comandos.

No modifica fuentes, Bronze ni tablas Delta. No limpia, imputa, convierte a UTC
ni selecciona definitivamente edificios. Meteorología queda fuera de este hito.
Los grupos A y B reproducen la solicitud de los autores: inicialmente
seleccionados y descartados en su EDA, respectivamente. La herramienta no ha
consultado la memoria y no certifica documentalmente esa clasificación.

## Ejecución

Desde la raíz del proyecto en Ubuntu/WSL:

~~~bash
.venv/bin/python -m tfm_lakehouse.characterization \
  --config config/characterization.example.toml \
  --output data/checks/h03_2/real/ejecucion-nueva
~~~

Desde PowerShell, sustituye la ruta de ejemplo por la ubicación del repositorio
dentro de Ubuntu/WSL. El comando `wsl` se ejecuta en una sola línea:

~~~powershell
$RepositoryWslPath = '/ruta/al/repositorio/tfm-lakehouse'
wsl -d Ubuntu --cd $RepositoryWslPath -- .venv/bin/python -m tfm_lakehouse.characterization --config config/characterization.example.toml --output data/checks/h03_2/real/ejecucion-nueva
~~~

El destino debe ser **nuevo** y estar dentro de
<output_root>/<dataset.kind>/. Una repetición requiere otro nombre de ejecución;
nunca sobrescribe informes anteriores. Los originales y los destinos Lakehouse
están protegidos frente a solapamientos. Se rechazan alias mediante enlaces en
los destinos. Las salidas reales situadas dentro del repositorio deben estar
excluidas de Git. El ejemplo escribe en data/checks/h03_2/real/.

## Configuración y parámetros

Todas las rutas del TOML se resuelven respecto a ese archivo. Las dos fuentes se
resuelven respecto a raw_root. --output se resuelve respecto al directorio de
trabajo. No se importa ni modifica la configuración Bronze.

| Parámetro | Interpretación |
| --- | --- |
| paths.raw_root | Raíz que contiene los originales |
| paths.output_root | Raíz de informes; namespace real o synthetic obligatorio |
| paths.lakehouse_root | Directorio protegido; la herramienta no escribe en él |
| dataset.kind | real o synthetic; declaración del operador |
| dataset.reference | Versión y procedencia declaradas |
| dataset.unit, unit_reference | Unidad declarada y fuente que la respalda |
| sources.metadata, electricity | Archivos CSV originales distintos dentro de raw_root |
| expected_sha256.metadata, electricity | Obligatorios para fuentes reales; huellas esperadas |
| analysis.primary_usage | Coincidencia literal del uso principal |
| analysis.min_sqm, max_sqm | Límites inclusivos de superficie; por defecto 3000 y 7000 |
| analysis.thresholds | Escenarios independientes de ausentes; 5, 8 y 10 por defecto |
| analysis.missing_tokens | Tokens exactos considerados ausentes; vacío y NaN por defecto |
| analysis.timestamp_format | Formato local sin zona; %Y-%m-%d %H:%M:%S |
| analysis.iqr_multiplier | Multiplicador exploratorio de IQR; 1,5 por defecto |
| analysis.max_grid_slots | Límite de etiquetas esperadas materializadas; 1000000 |
| candidates | Lista configurable de case_id, building_id y group, sin duplicados |

La coincidencia con hashes esperados comprueba identidad de bytes. Una referencia
de versión o una unidad declaradas no verifican por sí solas la procedencia. El
ejemplo contiene las huellas contrastadas con los objetos oficiales v1.0 en
H03.1; nunca sustituye raw por cleaned o kaggle.

## Procedimiento y denominadores

Se valida íntegramente UTF-8 y CSV estricto, cabeceras y anchura de registros.
Se rechazan punteros LFS, archivos vacíos, cabeceras ambiguas y building_id de
metadatos vacíos o duplicados. Las líneas físicas vacías se omiten, como en el
contrato CSV existente. No se normalizan nombres ni categorías: por ejemplo,
Residence Hall y Residence Hall con un tabulador final se cuentan por separado.

La selección progresiva distingue:

1. Registros de metadatos con claves únicas.
2. Coincidencia exacta con el uso principal configurado.
3. Superficie dentro de los límites inclusivos.
4. Presencia de columna eléctrica exacta.
5. Escenarios independientes de porcentaje de ausentes.

La completitud se calcula para toda la cohorte que supera superficie y columna.
Se conserva en memoria el detalle únicamente de los candidatos configurados,
además del eje temporal; las restantes columnas eléctricas no se acumulan. No se
infiere una regla para reducir esa cohorte a los 14 aportados ni se inventan
exclusiones retrospectivas.

Cada celda se clasifica de forma excluyente:

- Ausente: coincidencia exacta con missing_tokens.
- No interpretable: float no puede interpretar el texto.
- No finita: float produce NaN o infinito y el token no era un ausente configurado.
- Numérica finita: incluye ceros y negativos.

La clasificación es una **convención analítica explícita**, no una nueva
afirmación sobre el contrato de serialización de BDG2. Se conservan recuentos de
los tokens ausentes e inválidos. No se cambia ninguna celda.

N es el número de registros CSV recibidos, sin cabecera e incluyendo posibles
timestamps repetidos. Se publican ausentes/N, inválidos/N y
(ausentes+inválidos)/N. El umbral se compara sin redondeo mediante aritmética
decimal: un edificio pasa si ausentes/N es menor o igual al umbral.

Las proporciones de ceros, negativos y señales IQR usan como denominador las
lecturas numéricas finitas. Las estadísticas incluyen ceros y negativos:
media, mediana, desviación poblacional, mínimo, máximo y percentiles 1, 25, 75 y
99. Los percentiles usan interpolación lineal tipo 7. Sin valores finitos se
devuelve null; con uno, la desviación poblacional es cero.

## Eje temporal y cobertura

Se preservan las cadenas originales y se interpreta una copia como fecha local
naive, sin convertirla a UTC. Se informan etiquetas inválidas, duplicadas,
desordenadas, transiciones superiores a una hora y primeras/últimas etiquetas.
No se deduplica ninguna observación.

Cuando las etiquetas son horarias y el intervalo no supera max_grid_slots,
se construye una rejilla entre el mínimo y máximo observados. Se cuentan por
mes/año las etiquetas esperadas ausentes de la fuente, separadas de las celdas
ausentes dentro de registros existentes. También aparecen los meses enteros sin
filas dentro del intervalo. En meses iniciales/finales parciales, el denominador
solo abarca el tramo observado de la fuente. Las fechas inválidas se asignan a
unassigned, sin atribuirles un mes inventado.

La cobertura sobre etiquetas esperadas usa etiquetas distintas con al menos
una lectura finita; las filas repetidas no inflan ese porcentaje. Con etiquetas
fuera de la hora exacta, sin fechas interpretables o con un intervalo excesivo,
esa rejilla no se afirma: el resultado y su limitación se publican explícitamente.

Una etiqueta local horaria no demuestra una hora física transcurrida. La
documentación consultada de electricidad raw no establece una política DST
inequívoca; no se extrapola la de meteorología. Los alias US/* se comprueban
mediante ZoneInfo; su falta de resolución se informa como limitación ambiental,
sin modificar los originales ni instalar paquetes. Los destinos canónicos
documentados por IANA solo se incluyen como contexto.

## Ceros y posibles atípicos

Se distinguen rachas de registros consecutivos y rachas con etiquetas locales
únicas, válidas y separadas exactamente una hora. Las etiquetas repetidas,
incluida su primera aparición, se excluyen de la segunda continuidad; huecos,
retrocesos y fechas inválidas la interrumpen. Se publican todas las rachas,
las diez mayores y recuentos con al menos 24, 72 y 168 observaciones. Una racha
de 24 observaciones horarias abarca 23 horas entre sus etiquetas extremas:
no se presenta como duración física confirmada ni como fallo de sensor.

Se señalan valores estrictamente fuera de Q1 − k·IQR y Q3 + k·IQR, con k=1,5
por defecto. Se informa frecuencia, distribución mensual, secuencias horarias,
aislamiento/recurrencia y ejemplos extremos limitados. Si IQR es cero,
el método se declara degenerado y no concluyente; el recuento de señales IQR
es null, no cero. Se publican además las colas estrictas respecto a P1/P99.

Ni un histograma, ni los ceros, ni una señal IQR justifican automáticamente
eliminar lecturas o edificios. El análisis de sensibilidad 5/8/10 % no prueba
que el 8 % sea óptimo: permanece como propuesta metodológica sujeta a revisión.

## Informes privados y reproducibilidad

report.json contiene execution y metrics. Las métricas son deterministas;
la fecha UTC de ejecución, comando, versión Python, HEAD, estado de Git y
hashes de código/configuración se registran aparte. Los CSV derivados son:

- source_inventory.csv, primary_categories.csv y lodging_subcategories.csv.
- selection_funnel.csv y area_cohort_quality.csv.
- building_quality.csv y period_coverage.csv.
- zero_sequences.csv y exploratory_outliers.csv.
- threshold_sensitivity.csv y candidate_comparison.csv.

Una tabla sin filas puede no producir CSV; su sección sigue presente en JSON.
execution.json registra la ejecución y academic_summary.md es un borrador
para revisión. Los hashes de fuentes se verifican antes y después del análisis
y antes de finalizar la publicación. SUCCESS.json se escribe al final:
su ausencia indica un conjunto incompleto y nunca debe interpretarse como éxito.
No hay transacción conjunta de informes ni recuperación automática de un
directorio incompleto; se conserva y se usa otro nombre para repetir.

Los informes y sus valores reales permanecen fuera de Git. El comando de
pruebas y sus resultados deben registrarse junto a la ejecución correspondiente.
La memoria no se reescribe; el apartado 2.2.6 debe aportarse para su contraste.

## Cierre técnico y observaciones de revisión

Los autores aprobaron el cierre técnico de H03.2 con observaciones el 30/10/2026,
tras la revisión del código, las pruebas sintéticas y la reproducción de las
métricas sobre los originales. El cierre se limita a la caracterización; no
aprueba una selección definitiva ni acredita causas de las señales observadas.

El expediente privado original se conserva sin reescrituras, junto con sus
huellas registradas. Las observaciones se documentan aquí:

- `tests.log` existe en el expediente privado y recoge comandos y resultados,
  también documentados en `validation.json`. La observación preliminar sobre su
  ausencia queda rectificada: los listados que respetan las exclusiones de Git
  pueden omitirlo.
- Los niveles numéricos de confianza del informe histórico carecen de un método
  de calibración y no deben interpretarse como probabilidades estadísticas.
  La revisión expresa confianza alta en los recuentos, cálculos e identidad de
  bytes comprobados; la interpretación causal permanece sin demostrar.

Siguen pendientes el contraste con el apartado 2.2.6, la trazabilidad de la
selección histórica, la justificación del umbral del 8 %, la interpretación de
las señales y la comparabilidad energética. Este cierre no autoriza avanzar
automáticamente a H03.3.

## Pruebas sintéticas

~~~bash
.venv/bin/python -m pytest tests/test_characterization.py -q
.venv/bin/python -m pytest -m unit -q
~~~

Las pruebas no solicitan la fixture Spark. Cubren partición de celdas, ceros y
negativos, extremos, rachas 24/72/168, porcentajes límite, percentiles, poblaciones
vacías, IQR cero, duplicados no adyacentes, huecos, retrocesos, fechas inválidas,
meses sin filas, límites de superficie, CSV inválidos, hashes, rutas, alias y
repeticiones. Sus fixtures son sintéticos y no acreditan conclusiones energéticas.

## Fuentes metodológicas

- [BDG2, descripción de datos raw](https://pmc.ncbi.nlm.nih.gov/articles/PMC7591488/#Sec14):
  energía acumulada por registro horario en kWh y fechas locales. Raw ya incluye
  preparación inicial de los autores; no equivale a salida de sensor sin tratamiento.
- [BDG2, variantes de medidores](https://github.com/buds-lab/building-data-genome-project-2/wiki/Meters-data-features).
- [BDG2, metadatos](https://raw.githubusercontent.com/wiki/buds-lab/building-data-genome-project-2/Metadata-features.md):
  sqm representa superficie en m².
- [R, definición de cuantiles](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html):
  interpolación tipo 7.
- [NIST, box plots](https://www.itl.nist.gov/div898/handbook/eda/section3/boxplot.htm)
  y [tratamiento de posibles atípicos](https://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm):
  señales exploratorias que requieren investigación.
- [IANA, enlaces de compatibilidad](https://data.iana.org/time-zones/tzdb/backward).

Estas fuentes no aportan una justificación específica del umbral del 8 % para
esta cohorte. La selección final, la comparabilidad entre emplazamientos y los
contratos analíticos corresponden a revisión posterior de ambos autores.
