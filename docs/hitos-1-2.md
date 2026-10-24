# Guía técnica: hitos 1 y 2

## Alcance y responsabilidades

Implementación local de la base Spark/Delta y de tres fuentes Bronze
(electricidad, metadatos y meteorología opcional). Carlos dispone
de módulos separados para configuración, sesión, ingesta, ejecución y pruebas.
Las decisiones de Irantzu sobre selección de edificios, calidad, tipos analíticos,
integración, indicadores y validación del caso real quedan pendientes de acuerdo conjunto.

El contrato Bronze no selecciona edificios ni fija un esquema analítico. La estructura
de carpetas futuras de processing, quality y analytics se creará cuando haya código
verificado que alojar. No se han añadido módulos vacíos.

## Arquitectura implementada

```text
Archivos CSV originales inalterados (raw_root)
  -> validación UTF-8/CSV completa, cabecera, conteo independiente y SHA-256
  -> Spark: columnas originales string, sin inferencia
  -> validación/materialización de todas las fuentes configuradas
  -> Delta Bronze: snapshot completo por fuente + trazabilidad
  -> recuperación de cada versión escrita y comprobación de tipos y conteos
  -> publicación JSON atómica del conjunto de versiones verificadas
```

Se separan los CSV originales de su representación tabular Delta. Electricidad
permanece en formato ancho; los nombres originales de edificios se conservan.
No se normalizan fechas ni se convierten a UTC. Las fechas UTC de los informes
describen la ejecución del software, nunca las mediciones de origen.

## Estructura

```text
config/
  bronze.example.toml          # entradas reales locales, no versionadas
  bronze.synthetic.toml        # demostración de software
src/tfm_lakehouse/
  config.py                   # configuración y separación de rutas
  spark.py                    # Spark local y extensiones Delta
  environment.py              # escritura/lectura efectiva de verificación
  cli.py, __main__.py          # comandos
  ingestion/
    csv_source.py             # inspección y lectura CSV
    bronze.py                 # snapshots, trazabilidad e informes
    snapshot.py               # consulta de publicaciones completas
scripts/check_environment.py  # alternativa al comando check-environment
tests/
  test_config.py, test_sources.py, test_environment.py
  test_bronze.py, test_bronze_safety.py, test_real_data.py
  fixtures/synthetic_bdg2/data/{meters/raw,metadata,weather}/
docs/
  hitos-1-2.md
data/                         # generado/local; excluido de Git
  raw/bdg2/data/...            # originales reales inalterados
  lakehouse/
    synthetic/{bronze,_runs}/
    real/{bronze,_runs}/       # cargas con fuentes reales
  checks/                     # resultados de verificación locales
```

## Instalación y versiones

La instalación se realiza dentro de Ubuntu/WSL, no con Python de Windows.
Java 17 debe estar disponible mediante PATH o JAVA_HOME. No se modifica el Java
global ni se actualizan Python, Spark o Delta.

```bash
cd /home/yanez/tfm-lakehouse
export PATH="$HOME/.local/bin:$PATH"
# Si no existe el entorno:
uv venv --python 3.11.17
uv pip install --python .venv/bin/python -r requirements.lock -e .
uv pip check --python .venv/bin/python
.venv/bin/python --version
java -version
uv --version
```

`requirements.txt` mantiene los dos pines originales. `pyproject.toml` declara
el paquete y los comandos; `requirements-dev.txt` añade pytest 8.4.2.
`requirements.lock` fija todas las dependencias Python del entorno comprobado
(incluidas las transitivas originales), y el backend de construcción se fija
en pyproject. No se necesita pip dentro de .venv: uv puede instalar directamente.

La [matriz oficial de Delta](https://docs.delta.io/releases/) contempla Delta 3.3.x
con Spark 3.5.x. El comando check-environment permite registrar la comprobación
local en data/checks/environment.json; estos resultados generados no se versionan.

## Spark y Maven

`spark_session()` abre una sesión local exclusiva y la cierra al salir. Por defecto:
`local[2]`, dos particiones de shuffle y de snapshots Delta, UI desactivada.
Rechaza un master distribuido y una SparkSession ya existente.

Se configuran expresamente:

- `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`.
- `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`.
- `configure_spark_with_delta_pip`, que para la versión instalada selecciona
  `io.delta:delta-spark_2.12:3.3.3`.
- Workers con `sys.executable` a través de la configuración Spark y de
  `PYSPARK_PYTHON` acotado a la sesión; su valor anterior se restaura.

La [guía oficial](https://docs.delta.io/quick-start/) explica el helper Python;
en este proyecto se usa la versión 3.3.3 instalada, sin copiar los números de versión
de los ejemplos de la documentación actual. Se comprobó el código del helper instalado.

En la primera ejecución se descargaron de Maven Central:
`delta-spark_2.12:3.3.3`, `delta-storage:3.3.3` y `antlr4-runtime:4.9.3`.
Ivy guarda sus JAR y metadatos en `~/.ivy2/`; `spark.ivy_dir` permite elegir
otra caché. La siguiente ejecución reutilizó los tres artefactos.
Un entorno aislado necesita preparar esa caché previamente; instalar delta-spark
en Python por sí solo no proporciona los JAR de ejecución.

```bash
.venv/bin/python -m tfm_lakehouse check-environment --report data/checks/environment.json
# Alternativa equivalente ejecutada durante el desarrollo:
.venv/bin/python scripts/check_environment.py --report data/checks/environment.json
```

El criterio de aceptación incluye: tres filas conocidas, esquema y valores exactos
(con cero y null), archivos Parquet y registro JSON en _delta_log. La tabla vive
en un directorio temporal y se elimina al terminar. No es un benchmark.

## Configuración Bronze

Usar `--config` explícitamente. Todas las rutas relativas se resuelven respecto
al TOML; las rutas de fuentes se resuelven respecto a raw_root.

| Campo | Significado |
| --- | --- |
| paths.raw_root | Raíz local de CSV originales |
| paths.lakehouse_root | Raíz separada de salidas |
| dataset.kind | Obligatorio: real o synthetic |
| dataset.reference | Declaración de procedencia y versión, no comprobación remota |
| sources.electricity | Un CSV ancho con timestamp y al menos un edificio |
| sources.metadata | Un CSV con building_id |
| sources.weather | Opcional: un CSV con timestamp y site_id |
| spark.master | local[2] por defecto; solo ejecución local |
| spark.ivy_dir | Opcional, caché de dependencias Java |

No se permiten fuentes fuera de raw_root, entradas idénticas, solapamientos entre
raw y salidas ni alias mediante enlaces en los destinos bajo lakehouse_root.
También se rechazan alias internos entre tablas, informes o namespaces.
Los namespaces `real/` y `synthetic/` mantienen separadas las salidas; dataset.kind es una
declaración del operador y no certifica por sí misma que un archivo sea BDG2.

Variables de entorno: JAVA_HOME/PATH para Java/uv; PYSPARK_PYTHON se establece
temporalmente por la aplicación. `TFM_REAL_CONFIG` solo habilita la prueba
opcional real. No se carga .env, no se requieren credenciales.

## Contrato de entrada y conservación

Una fuente es un archivo local UTF-8 (BOM admitido), separado por comas, con cabecera
y comillas/escape CSV estándar mediante comillas dobles. Se admite texto multilínea.
No se aceptan directorios, patrones ni nombres de ruta con metacaracteres de glob,
ni punteros Git LFS: Spark podría interpretar un nombre literal como un patrón.

Se valida todo el flujo UTF-8 y CSV antes de leerlo con Spark, incluida la anchura
de cada registro. El conteo independiente debe coincidir con Spark y sus archivos
de entrada deben corresponder exactamente al CSV previsto.
Solo se exigen timestamp y al menos una columna de edificio para electricidad,
building_id para metadatos, y timestamp y site_id para meteorología. Se rechazan cabeceras vacías, duplicadas incluso
sin distinguir mayúsculas, el prefijo reservado _bronze_ y caracteres incompatibles
con Delta sin column mapping. No se renombran columnas de forma implícita.

Todas las columnas de origen son `string nullable`: sin inferencia numérica o temporal.
Se conserva orden de columnas, ceros iniciales, espacios, textos NaN, identificadores,
duplicados, ceros, negativos y extremos. Los campos CSV sin valor se representan
como null según el lector de Spark; líneas físicas vacías se omiten. Las comillas
de escape se interpretan como sintaxis CSV. La copia byte a byte permanece en raw;
la tabla Delta representa registros CSV, no su formato físico.

No se asigna una unidad a columnas ni se aplica limpieza, filtrado, unpivot, unión
o deduplicación. La [documentación oficial de medidores](https://github.com/buds-lab/building-data-genome-project-2/wiki/Meters-data-features)
describe raw con energía en kWh, frecuencia horaria y hora local; distingue raw,
cleaned y kaggle. Cualquier archivo incorporado debe corresponder a la variante
declarada antes de usar esas unidades en Silver. En particular no debe sustituirse
raw por cleaned o kaggle sin justificarlo.

La [documentación de metadatos](https://raw.githubusercontent.com/wiki/buds-lab/building-data-genome-project-2/Metadata-features.md)
identifica building_id, site_id, sqm y timezone, entre otros atributos. No se fija
un esquema Silver definitivo. Las cabeceras se obtienen de los CSV locales
recibidos; una URL de GitHub que devuelva un puntero LFS no sirve como entrada.

Meteorología conserva sus columnas y valores originales sin asignar unidades,
normalizar tiempos ni vincular registros a edificios. Esa integración y la
interpretación de ausencias o códigos especiales corresponden a Silver.

## Salida, estrategia de escritura y trazabilidad

Tablas locales:

- `<lakehouse_root>/<kind>/bronze/electricity`.
- `<lakehouse_root>/<kind>/bronze/metadata`.
- `<lakehouse_root>/<kind>/bronze/weather`, si se configura.
- Informes: `<lakehouse_root>/<kind>/_runs/<run_id>.json`.

Cada tabla incorpora cuatro columnas de texto, además de todas las originales:

| Columna | Contenido |
| --- | --- |
| _bronze_source_uri | URI absoluta del CSV recibido |
| _bronze_source_sha256 | Huella SHA-256 del contenido recibido |
| _bronze_data_kind | real o synthetic |
| _bronze_dataset_reference | Referencia declarada en la configuración |

Cada entrada se materializa en caché DISK_ONLY con todos los campos y con
FAILFAST; se desactiva temporalmente el column pruning del parser. Esto permite
detectar filas cortas o con campos adicionales incluso durante los conteos.
Todas las fuentes configuradas se validan antes del primer commit. Las fuentes
deben permanecer inmutables durante la ejecución; se revalida su SHA-256 antes de escribir.
Un bloqueo local por namespace impide dos ingestas simultáneas de esta aplicación;
no coordina escritores Delta ajenos al proceso.

Se usa `overwrite` con `overwriteSchema=true` como recarga completa de la fuente
configurada. Repetir una carga mantiene las mismas filas y valores, aunque crea
una versión Delta nueva. No se hace append ni deduplicación; los duplicados del
CSV original también permanecen. Un CSV más corto reemplaza completamente el snapshot
anterior; no se deben pasar fragmentos esperando una carga incremental.

No se sobrescriben destinos existentes que no sean Delta. Un archivo sin cabecera,
vacío, con solo cabecera o malformado hace fallar la carga: nunca vacía silenciosamente
una tabla anterior. Se guardan informes de error, incluidos commits parciales.

Delta garantiza el commit por tabla. **No hay transacción conjunta** entre fuentes:
un fallo intermedio puede dejar distintas ejecuciones en las versiones más recientes
de las tablas. El informe conserva `cross_table_atomic=false`.

La consulta coherente se obtiene mediante la publicación: solo se escribe un informe
`status=passed` después de verificar todas las versiones y completar la limpieza.
El JSON se publica mediante sustitución atómica. Cada commit guarda el identificador
de ejecución en userMetadata; se identifica ese commit, no una versión posterior de
otro escritor. Los errores de limpieza no ocultan la causa principal y se intenta
guardar el informe aunque falle la liberación de cachés o la restauración de Spark.

`inspect-bronze` selecciona la publicación satisfactoria más reciente compatible
con kind, referencia y conjunto de fuentes. Lee cada tabla mediante versionAsOf,
verifica la identidad del commit, el esquema y el conteo y comunica si usa una
ejecución anterior por un fallo posterior. Si falta una versión publicada o no coincide,
falla expresamente. No sustituye la versión solicitada por la última disponible.

Para recuperar una carga fallida, corregir la causa y repetir la carga completa.
No borrar tablas ni ejecutar VACUUM sobre las versiones necesarias para recuperación.
Los informes antiguos sin contrato de publicación se conservan como historial;
requieren una nueva ingesta completa para ser consultados con este lector.
Añadir weather también requiere una publicación nueva con las tres fuentes:
una ejecución anterior de dos tablas no satisface esa configuración.

El informe contiene inicio/fin, referencia, huellas, tamaño, esquemas de entrada/salida,
conteos, rutas, versiones Delta, resultado de lectura posterior y errores. Estos
informes registran operaciones de software, no valoraciones de calidad energética.

## Ejecución y consulta

```bash
.venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.synthetic.toml
.venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.synthetic.toml
# También queda instalado el comando:
.venv/bin/tfm-lakehouse --help
```

inspect-bronze devuelve conteos, esquemas y hasta cinco filas por tabla. Ese límite
solo se aplica a la muestra mostrada: no trunca la ingesta. Una consulta propia puede
usar `read_bronze_snapshot(spark, config)` dentro de spark_session para obtener
los DataFrames del mismo conjunto publicado. Leer directamente la última versión
de cada ruta no ofrece esta garantía. No se necesita catálogo Hive ni se recopila
el conjunto completo en memoria del driver.

Errores de configuración, lectura, JVM o persistencia producen código de salida 1
en la CLI y un traceback en stderr. La ingesta, cuando puede crear el directorio
de informes, registra además el error en JSON. Los procesos que no llegan a iniciar
Spark solo pueden producir el registro CLI.

## Incorporación de BDG2

1. Obtener los archivos seleccionados de la
   [publicación de autores en Zenodo v1.0](https://zenodo.org/records/3887306)
   o del [repositorio oficial](https://github.com/buds-lab/building-data-genome-project-2).
   No se ha descargado el conjunto durante esta intervención.
2. Mantener la estructura original y los archivos inalterados:
   `data/raw/bdg2/data/meters/raw/electricity.csv`,
   `data/raw/bdg2/data/metadata/metadata.csv` y, si se configura,
   `data/raw/bdg2/data/weather/weather.csv`.
   Si provienen de GitHub/LFS, asegurarse de tener CSV reales y no punteros.
3. Copiar config/bronze.example.toml a una configuración local, adaptar raw_root y
   dataset.reference con versión/revisión y URL realmente usadas.
   Reexaminar las cabeceras y tipos originales; no asumir que los fixtures
   representan todas las columnas ni todas las incidencias del conjunto real.
4. Conservar el alcance de una posible extracción de prueba en un manifiesto:
   archivo original y SHA, filas/edificios/periodo elegidos, comando de extracción,
   huella del subconjunto y limitaciones. Guardar el subconjunto en otra carpeta;
   nunca editar el archivo original ni inventar o completar mediciones.
5. Ejecutar con esa configuración:

```bash
.venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.example.toml
.venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.example.toml
TFM_REAL_CONFIG=config/bronze.example.toml .venv/bin/python -m pytest -m real_data -q
```

El ejemplo requiere las tres fuentes; se puede omitir sources.weather para una
carga de dos. La prueba real_data valida y cuenta por completo las fuentes configuradas,
usando su master y caché Ivy. No sustituye la ejecución de ingesta ni demuestra
validez analítica o rendimiento.

Desde PowerShell, sin continuaciones de línea de Bash:

```powershell
wsl -d Ubuntu --cd /home/yanez/tfm-lakehouse -- .venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.example.toml
wsl -d Ubuntu --cd /home/yanez/tfm-lakehouse -- .venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.example.toml
```

## Pruebas y criterios de aceptación

```bash
.venv/bin/python -m pytest -m unit -q
.venv/bin/python -m pytest -m environment -q
.venv/bin/python -m pytest -m integration -q
.venv/bin/python -m pytest -q --junitxml=data/checks/pytest.xml
```

unit no inicia una JVM. environment prueba persistencia Delta efectiva. integration
usa exclusivamente copias/archivos sintéticos en directorios temporales independientes.
real_data se omite expresamente si TFM_REAL_CONFIG no está definido.
El marcador performance está reservado; no hay benchmarks implementados.

Se comprueban: fuente válida/inexistente, cabeceras, cero bytes, solo cabecera,
conservación exacta de valores/columnas (incluidos duplicados), linaje, persistencia,
recuperación, recarga repetida, sustitución por snapshot menor, entradas malformadas,
protección de tablas ante validación fallida y registro de escritura parcial.
Las regresiones incluyen alias de rutas, glob literales, UTF-8 y comillas inválidas
en el cuerpo, diferencias de conteo, errores de limpieza/publicación, escritores
ajenos y recuperación del conjunto anterior tras una carga parcial.
No se modifican datos reales para probar errores.

## Git y límites actuales

.venv, data, salidas Delta, caché Ivy local, temporales Spark, metadatos de construcción
y logs se excluyen de Git. Los fixtures pequeños identificados como sintéticos y las
pruebas y documentación sí se versionan. Los informes JSON/XML y logs de ejecución
se guardan localmente bajo data/checks o data/lakehouse/<kind>/_runs y están ignorados.
No se han creado commits, pushes ni cambios de autoría.

Pendientes: selección conjunta de edificios, interpretación y validación analítica
de los originales BDG2, metadatos/unidades/zonas verificadas por fuente, Silver y su
modelo largo, reglas de calidad, integración meteorológica justificada, Gold,
explotación mínima y evaluación de rendimiento.
No hay streaming, predicción, cloud ni control operativo de instalaciones.
