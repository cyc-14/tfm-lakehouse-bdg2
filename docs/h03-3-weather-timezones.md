# H03.3 — Caracterización meteorológica y diagnóstico temporal

## Alcance y estado

**Estado:** implementado y con pruebas sintéticas superadas. La ejecución con
originales BDG2 se realizó el **30/10/2026**; su expediente privado
`data/checks/h03_3/real/2026-10-30-h03-3/` fue contrastado independientemente.
La auditoría técnica se ha superado con observaciones y ha sido aceptada por
los autores.

Herramienta local y por lotes que inspecciona CSV originales sin modificarlos.
Durante la implementación se utilizaron pruebas sintéticas; la ejecución real
posterior y su auditoría constituyen evidencias distintas. Los informes reales
permanecen fuera de Git.

Estas correcciones preparan el cierre técnico de H03.3 y su versionado. Este
entregable no cierra académicamente H03: el contraste con la memoria, la
aceptación global de las fuentes y la actualización de la planificación general
se tramitan por separado. La integración temporal, Silver y la selección
definitiva de edificios continúan pendientes.

No modifica H03.2, Bronze ni los originales. No integra mediciones eléctricas y
meteorológicas, elimina repeticiones, selecciona edificios, convierte series a
UTC, asigna `fold` a observaciones ni aplica desplazamientos horarios fijos.

La meteorología se caracteriza por `site_id`, identificador del emplazamiento.
El CSV no acredita por sí mismo el número de estaciones físicas. La relación
con los candidatos se comprueba mediante claves exactas de metadatos, sin
deducirlas del nombre del edificio ni atribuirles representatividad espacial.

## Reutilización y límites respecto a H03.2

El módulo importa de `characterization.py` los helpers existentes de SHA-256,
CSV estricto, clasificación numérica, porcentajes, perfiles de etiquetas y
serialización CSV. Reutiliza su validador de destinos mediante los mismos
atributos de configuración, añadiendo la protección del directorio de informes
H03.2 y de su TOML de contexto. **No modifica ese módulo ni su comportamiento.**

La configuración y el publicador propios responden a un esquema distinto:
emplazamiento, variable y calendario, sin aplicar filtros de superficie ni
umbrales de selección eléctricos. Se registran las huellas de ambos módulos.
No se importa el lector Bronze, que depende de PySpark; no se inicia una JVM.

## Configuración

Todas las rutas del TOML se resuelven respecto a ese archivo; las fuentes se
resuelven respecto a `raw_root`. El destino CLI se resuelve respecto al directorio
de trabajo.

| Campo | Contrato |
| --- | --- |
| `paths.raw_root` | Raíz de los tres originales locales |
| `paths.output_root` | Raíz de informes H03.3 separada de originales, Lakehouse y H03.2 |
| `paths.lakehouse_root` | Directorio protegido; no se escribe en él |
| `dataset.kind`, `reference` | Declaración `real`/`synthetic` y procedencia |
| `sources.metadata`, `electricity`, `weather` | Tres CSV distintos dentro de `raw_root` |
| `expected_sha256` | SHA-256 esperado por fuente; obligatorio para `real` |
| `context.characterization_config` | TOML H03.2 del mismo tipo de datos; aporta candidatos y raíz protegida |
| `analysis.timestamp_format` | Formato de etiquetas locales sin zona |
| `analysis.nominal_start`, `nominal_end` | Extremos inclusivos de un calendario nominal horario explícito |
| `analysis.missing_tokens` | Tokens exactos ausentes; vacío y `NaN` por defecto; se rechazan tokens numéricos finitos |
| `analysis.max_grid_slots` | Límite de etiquetas materializadas por calendario |

Los candidatos se leen del contexto, sin duplicar la lista ni reinterpretar los
grupos declarados por los autores. Deben existir en metadatos y en las columnas
eléctricas. La ausencia de meteorología de su emplazamiento se informa, sin
eliminar al candidato ni inventar observaciones.

## Método y denominadores

Las fuentes se validan íntegramente como UTF-8/CSV estricto, incluidas cabeceras
y anchura. Se rechazan archivos vacíos, punteros LFS, claves de metadatos vacías
o repetidas y `site_id` meteorológicos vacíos. Las líneas físicas vacías se
omiten conforme al contrato compartido; el índice de registro cuenta las filas
CSV recibidas, sin cabecera. Electricidad aporta únicamente sus etiquetas y la
presencia de columnas de candidatos, sin análisis ni unión de mediciones.

Cada celda meteorológica pertenece exclusivamente a ausente, texto no numérico,
numérico no finito o numérico finito. Los tokens se conservan y los valores
finitos no se consideran automáticamente físicamente válidos.
Los tokens numéricos finitos no pueden configurarse como ausentes: esto protege
los códigos `−1` y `9`, además de ceros y otras observaciones, frente a una
reinterpretación mediante configuración.

- Ausentes, inválidos y finitos usan como denominador los registros recibidos
  del emplazamiento y variable, **incluidas las repeticiones**.
- Ceros y negativos usan las celdas numéricas finitas. Son recuentos descriptivos,
  no reglas de error; una temperatura negativa puede ser válida.
- La cobertura temporal usa etiquetas distintas, intersectadas con el calendario
  correspondiente. Varias filas de una etiqueta no inflan el porcentaje.
- Se distinguen ausencia de registros del calendario y valores ausentes dentro
  de filas existentes. Se publican calendarios del intervalo observado y del
  periodo nominal, sin presentar etiquetas como horas físicas transcurridas.
- Los perfiles mensuales/anuales incluyen meses enteros sin filas dentro del
  calendario. Las fechas inválidas se agrupan en `unassigned`.

Las filas `observed_span` y `nominal_calendar` conservan el mismo número de
registros recibidos del periodo; cambian la rejilla y su denominador de etiquetas.
Las etiquetas recibidas fuera de esa rejilla se cuentan aparte y no aumentan
la cobertura sobre el calendario.

Se publican mínimos y máximos numéricos por emplazamiento y variable; no se
aplica IQR a nubosidad codificada, dirección circular ni precipitación. No se
interpreta un extremo como error confirmado ni se aplica el umbral eléctrico
del 8 % a meteorología.

## Claves repetidas y preservación de observaciones

La clave candidata se audita como `(site_id, texto timestamp original)`.
Compartir una fecha entre dos emplazamientos no es un duplicado.

Una clave repetida es `exact` si sus filas completas, una vez interpretada la
sintaxis CSV, contienen los mismos campos; es `conflicting` si hay más de un
contenido. Esto no afirma igualdad de bytes físicos de sus líneas. El caso
A, A, B es conflictivo y conserva además el recuento de una repetición exacta
adicional. Se informan claves, filas implicadas y adicionales, contenidos
distintos y ejemplos acotados de posiciones originales. No se elimina ninguna
observación. Las posiciones de los perfiles temporales se refieren al orden
dentro del emplazamiento; los ejemplos de duplicados usan registros globales.

El detector no contiene un número esperado de duplicados reales. Las pruebas
sintéticas incluyen un escenario de 37 claves conflictivas inventadas; ese
resultado de software no acredita un nuevo resultado BDG2.

## Zonas, DST y códigos especiales

Se distinguen cuatro niveles de evidencia:

1. **Observado:** etiquetas, repeticiones, tokens, correspondencia nominal y zonas
   recibidas en metadatos.
2. **Documentación oficial:** BDG2 describe electricidad y meteorología locales;
   la wiki meteorológica añade «DST is ignored». La meteorología UTC de la
   competición Kaggle no determina la semántica del CSV BDG2.
3. **Hipótesis diagnóstica:** sondeos del calendario civil IANA para identificar
   etiquetas regulares, ambiguas o inexistentes bajo esas reglas.
4. **Pendiente:** política temporal efectiva de cada fuente, límites de intervalos,
   interpretación física de repeticiones y cardinalidad de una integración.

Los sondeos prueban en memoria las dos posibilidades de `fold` y su retorno
calendárico mediante UTC. **No asignan ninguna posibilidad a una observación,
convierten una serie ni producen correspondencias horarias entre fuentes.**
La clasificación no presupone el signo de `dst()`, relevante para Dublín.

Los enlaces IANA `US/Eastern`, `US/Central`, `US/Mountain` y `US/Pacific` pueden
examinarse mediante sus destinos documentados, conservando el identificador
original. Los sondeos usan ese destino explícito; disponibilidad del nombre
original y del catálogo se registra aparte. No se instala `tzdata`, cambia
`TZPATH` ni corrige una zona de edificio a partir de su ubicación supuesta.
La falta de resolución ambiental se comunica sin concluir un error de BDG2.

Los valores numéricos `−1` en precipitación y `9` en nubosidad se cuentan con sus
tokens originales, sin transformarlos. NOAA describe códigos para trazas de
precipitación y cielo oscurecido/no estimable, pero debe comprobarse su
transformación concreta en BDG2 antes de reinterpretarlos. Los documentos PDF
NOAA enlazados pueden redirigir; no se afirma haber inspeccionado un PDF completo.

## Informes privados y reproducción

`report.json` separa métricas y ejecución. Las métricas son deterministas con
las mismas entradas, parámetros y catálogo temporal disponible. Las rutas
absolutas, fecha UTC de ejecución, Python, Git, huellas de configuraciones,
módulos y archivos del catálogo IANA se registran en `execution`.

Se derivan los CSV `source_inventory`, `site_coverage`, `variable_quality`,
`period_coverage`, `duplicate_keys`, `candidate_weather_availability`,
`timezone_inventory` y `dst_diagnostics`. Una tabla sin filas puede no producir
CSV, pero conserva su sección JSON. Los campos anidados se serializan como JSON.

Los hashes se comprueban antes y después del análisis y antes/después de escribir
los informes. El destino debe ser nuevo, bajo `<output_root>/<kind>/`, sin alias
mediante enlaces ni solapamientos. Los informes reales situados dentro del
repositorio deben estar excluidos de Git. `SUCCESS.json` se publica al final y
contiene la huella de métricas,
fuentes y artefactos; acredita publicación completa, **no calidad analítica ni
integración horaria validada**. El marcador se escribe primero en un archivo
temporal y se publica mediante renombrado atómico tras verificar las entradas.
Un fallo conserva el directorio parcial sin
`SUCCESS`; una repetición exige otro destino. No hay transacción conjunta.

### Revisión documental posterior a la ejecución real

El expediente `2026-10-30-h03-3` conserva sin cambios sus informes, resultados y
huellas de la ejecución del 30/10/2026. Las correcciones posteriores solo
actualizan documentación, comentarios y docstrings; no cambian parámetros, fórmulas,
interfaces ni contratos de entrada y salida.

Las traducciones del módulo y los comentarios del TOML cambian sus huellas de
archivo aunque preservan el comportamiento. Las huellas históricas de
`execution.json` identifican la versión ejecutada y no deben sustituirse por
las de esta revisión. La equivalencia se comprueba comparando el AST de ambos
archivos Python sin docstrings y el contenido TOML interpretado, además de las
pruebas de regresión. No se ha repetido la ejecución real para estas correcciones.

## Pruebas y reproducción controlada

Pruebas exclusivamente sintéticas, sin JVM:

~~~bash
.venv/bin/python -m pytest tests/test_weather_characterization.py -q
.venv/bin/python -m pytest tests/test_characterization.py -q
~~~

La ejecución real está documentada en el expediente privado citado; su contraste
independiente se recoge en la revisión técnica. El comando siguiente es una
plantilla para una
**nueva ejecución que requiere aprobación expresa** y un destino nuevo.
El destino histórico no debe reutilizarse ni sobrescribirse:

~~~bash
.venv/bin/python -m tfm_lakehouse.weather_characterization \
  --config config/weather-characterization.example.toml \
  --output data/checks/h03_3/real/ejecucion-nueva
~~~

No requiere recargar Bronze ni generar Silver/Gold. La existencia de candidatos
con meteorología no aprueba una selección definitiva ni demuestra que sean
hoteles reales: el escenario empresarial continúa siendo simulado.

## Referencias metodológicas

- [BDG2: variables meteorológicas](https://github.com/buds-lab/building-data-genome-project-2/wiki/Weather-features).
- [BDG2: medidores](https://github.com/buds-lab/building-data-genome-project-2/wiki/Meters-data-features).
- [BDG2: metadatos](https://github.com/buds-lab/building-data-genome-project-2/wiki/Metadata-features).
- [Artículo BDG2](https://pmc.ncbi.nlm.nih.gov/articles/PMC7591488/).
- [IANA: enlaces históricos](https://data.iana.org/time-zones/tzdb/backward).
- [Python: zoneinfo](https://docs.python.org/3.11/library/zoneinfo.html) y [PEP 495](https://peps.python.org/pep-0495/).
- [NOAA: formato ISD-Lite](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.pdf) y [documentación técnica](https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-technical-document.pdf).
