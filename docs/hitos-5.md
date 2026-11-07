# Hito 5 — Silver de electricidad y controles de calidad

**Estado:** H05 formal pendiente; H05.1 preliminar implementado, con verificación
sintética registrada al final de esta guía. **Responsabilidad:** Carlos implementa el flujo;
Irantzu define y valida las reglas analíticas. **Objetivos:** OE3, OE4.
**Requisito previo para H05 formal:** contratos aceptados en H04.

## Finalidad

Implementar un flujo mínimo desde una **publicación Bronze coherente** hasta
electricidad Silver en formato largo, con trazabilidad y anotaciones de
calidad explícitas. Se comenzará por una transformación acotada; las reglas
adicionales se introducirán mediante incrementos verificables.

## Actividades incrementales

1. Definir la configuración de Silver y consumir las versiones Bronze
   seleccionadas mediante `read_bronze_snapshot(...)`.
2. Transformar a formato largo únicamente las columnas eléctricas aprobadas,
   manteniendo identificadores, granularidad y multiplicidad justificadas.
3. Interpretar fechas y números conforme a H04, preservando texto original,
   fallos de conversión, procedencia y ambigüedades horarias.
4. Aplicar indicadores de ausencia, cero, negativo, duplicidad o valor
   extremo según las reglas aprobadas, sin confundir incidencias con errores.
5. Persistir y releer Delta Silver con pruebas de funcionamiento,
   reejecución, fallo y conciliación de recuentos.

## Entregables previstos

- Módulos de procesamiento y calidad únicamente cuando exista código
  probado que alojar, sin estructuras vacías anticipadas.
- Salidas Delta Silver versionadas y separadas de `raw` y Bronze.
- Pruebas sintéticas de casos válidos, ausentes, malformados, duplicados y
  situaciones temporales límite que correspondan al contrato.

## Criterios de aceptación

- [ ] Las filas resultantes respetan granularidad, claves, tipos y
      procedencia; Bronze y los CSV originales permanecen inalterados.
- [ ] La conciliación explica multiplicidad, exclusiones y registros
      rechazados o apartados, sin pérdidas silenciosas.
- [ ] Los indicadores de calidad y sus casos límite están probados.
- [ ] Las reejecuciones y escrituras parciales tienen un comportamiento
      documentado; no se atribuye atomicidad entre tablas.
- [ ] Se han ejecutado pruebas pertinentes y se diferencian las realizadas
      con datos sintéticos de cualquier validación real autorizada.

## Issues de GitHub propuestos

- `[H05] Leer publicaciones Bronze coherentes para Silver`
- `[H05] Implementar transformación de electricidad a formato largo`
- `[H05] Interpretar fechas y valores con trazabilidad`
- `[H05] Añadir reglas de calidad y pruebas de casos límite`
- `[H05] Comprobar persistencia, reejecución y recuperación Silver`

## Límites y condición de avance

No incorporar otros combustibles, imputaciones no aprobadas, meteorología,
predicciones ni indicadores Gold. H06 requiere que el flujo eléctrico
mínimo cumpla su contrato y haya superado una revisión técnica.

## H05.1 — Transformación técnica preliminar

Incremento autorizado por separado el 01/11/2026, exclusivamente con datos
sintéticos. No aprueba H04 ni completa H05: la selección definitiva y los
contratos analíticos siguen pendientes. La lista de columnas es un parámetro
técnico, no un manifiesto académico de edificios seleccionados. La actualización
del plan general quedó fuera de aquel incremento; su estado se consulta en el
[registro de hitos](milestones.md).

### Interfaces y esquema provisional

El módulo `src/tfm_lakehouse/silver_electricity.py` ofrece:

```python
electricity_to_long(bronze, electricity_columns) -> DataFrame
read_electricity_long(spark, config, electricity_columns) -> (selection, DataFrame)
```

La primera función acepta un DataFrame con la columna exacta `timestamp`, las
cuatro columnas de linaje del [contrato Bronze](hitos-1-2.md) y las columnas
eléctricas solicitadas. Todos los campos de entrada deben ser `StringType`.
La selección debe ser una secuencia explícita no vacía, sin nombres duplicados
y con coincidencia exacta con las columnas recibidas. Se rechazan nombres de
entrada vacíos o ambiguos incluso sin distinguir mayúsculas, campos obligatorios
ausentes, tipos incompatibles y columnas técnicas `_bronze_` desconocidas. Una
evolución del esquema técnico requerirá revisar este contrato; no se descarta
linaje nuevo silenciosamente. `timestamp` y cualquier nombre con el prefijo
reservado `_bronze_` no pueden solicitarse como edificios.

La salida básica tiene estas columnas:

| Campo | Tipo y significado |
| --- | --- |
| `building_id` | Cadena: nombre original de la columna eléctrica solicitada |
| `timestamp_original` | Cadena original recibida en `timestamp`, incluido null |
| `electricity_value_original` | Cadena o null original de la celda eléctrica |
| `_bronze_source_uri` | Procedencia original recibida, sin reinterpretación |
| `_bronze_source_sha256` | Hash recibido de Bronze, sin recalcularlo a partir de celdas |
| `_bronze_data_kind` | Declaración original `real`/`synthetic` |
| `_bronze_dataset_reference` | Referencia original del dataset |

El grano es **una aparición de fila Bronze por columna eléctrica solicitada**.
No se establece clave primaria. Dos apariciones idénticas siguen siendo dos
filas idénticas: Bronze no aporta un ordinal original que permita asignarles
una identidad estable. No se generan índices, hashes de fila ni identificadores
dependientes del particionado para fingir esa identidad.

La función básica conserva el linaje recibido, pero no autentica un DataFrame
arbitrario como publicación Bronze. La segunda interfaz utiliza exclusivamente
`read_bronze_snapshot(...)`, que verifica todas las fuentes configuradas y lee
las versiones publicadas mediante `versionAsOf`. Devuelve su contexto original
`selection` y añade a cada fila eléctrica:

| Campo adicional | Procedencia |
| --- | --- |
| `_bronze_source_name` | Literal `electricity` |
| `_bronze_snapshot_run_id` | `selection["run_id"]` |
| `_bronze_delta_version` | Versión Delta de electricidad seleccionada por el lector, como entero |
| `_bronze_table_path` | Ruta de la tabla eléctrica registrada en la publicación |

La versión se refiere a la tabla eléctrica, no a una versión única compartida
entre tablas. El contexto conserva también las versiones de las demás fuentes,
la referencia del informe y la indicación de recuperación de una publicación
anterior cuando corresponda. El lector existente efectúa sus comprobaciones de
conteo; la transformación posterior sigue siendo una proyección diferida.
No se atribuye atomicidad entre Bronze y una futura capa Silver. Las rutas del
linaje son información privada si describen una ejecución real; la herramienta
no publica informes ni extrae datos reales para Git.

### Conservación, multiplicidad y ejecución

Se construye por fila un array de estructuras `(building_id, valor)` y se aplica
`explode`. Cada estructura existe incluso cuando su valor es null. Por tanto:

```text
filas largas = apariciones Bronze × columnas eléctricas solicitadas
```

La igualdad se comprueba con datos sintéticos, incluidos nulos y duplicados.
La función básica no ejecuta conteos adicionales ni materializa el conjunto en
el driver. No impone orden físico. Las pruebas comparan multiconjuntos, no
secuencias ordenadas. No hay filtros, agregaciones, joins ni deduplicaciones.

Los nombres se pasan como literales para `building_id` y como referencias
completas de columna entre backticks, escapando los backticks internos. No se
concatenan expresiones SQL ejecutables. Se prueban puntos, espacios, comillas,
backticks, Unicode y texto con apariencia de SQL; algunos de esos nombres no
son admitidos por el lector CSV Bronze actual para persistir Delta, pero sí
sirven para probar la transformación genérica sobre DataFrames sintéticos.

Se conservan null, `""`, `"NaN"`, códigos especiales, ceros, negativos, extremos
y espacios sin conversión numérica. Una cadena vacía ya interpretada como null
por el lector CSV Bronze no puede reconstruirse: la garantía empieza en el
DataFrame recibido, no en la representación byte a byte del CSV.

La etiqueta temporal permanece como texto, incluidas fechas inválidas, etiquetas
repetidas y ejemplos asociados a cambios DST. No hay conversión UTC, deducción
de zona, resolución de ambigüedades ni asignación de `fold`. No se interpreta
la etiqueta como instante físico ni se atribuyen unidades al valor eléctrico.

El plan utiliza expresiones Spark, sin UDF de Python, `collect`, `toPandas`,
persistencia ni caché. La expansión produce necesariamente N×K filas y un array
de K estructuras por aparición de entrada; el plan crece con la selección de
columnas. No se han medido rendimiento, escalabilidad ni umbrales de anchura.

### Verificación sintética y límites

`tests/test_silver_electricity.py` separa validaciones `unit` sin JVM de pruebas
`integration` con Spark/Delta y datos inventados. La integración crea una
publicación Bronze temporal válida y después una versión eléctrica física
divergente sin publicación completa. Verifica que la interfaz consume la versión
publicada, conserva el linaje y concilia 4×2=8 filas, incluidos los duplicados.
Solo se escriben tablas Bronze sintéticas temporales; no se persiste Silver.

Selección focal: `tests/test_silver_electricity.py`; regresión general:
`-m "not real_data and not performance"`, con `TFM_REAL_CONFIG` ausente. Para
evitar descargas y cambios en Ivy se utiliza el [perfil de JAR locales](hitos-1-2.md#perfil-temporal-sin-resolución-maven),
con copias en un directorio temporal. En este incremento también se asignaron
destinos temporales a Ivy, al almacén Spark y a `SPARK_LOCAL_DIRS`, desactivando
la caché de pytest y la escritura de bytecode. El gestor Spark y Delta son los
reales; solo se sustituye el helper de resolución Maven en el proceso de prueba.

Verificación del 01/11/2026 sobre `main`, base
`ea71de4ab848dcb5b9fa58aba80dd410e9c60aaa`, con estos tres entregables locales.
Entorno sin cambios: Python 3.11.17, PySpark 3.5.7 y Delta 3.3.3.

| Comprobación ejecutada | Resultado observado |
| --- | --- |
| Primer intento focal dentro del sandbox | 39 pruebas unitarias superadas; siete errores de preparación por bloqueo del socket local Py4J, antes de ejecutar sus pruebas Spark |
| Focal con sockets locales permitidos y perfil temporal | 46 superadas: 39 `unit` y siete `integration`; 15,81 s |
| Suite general con el mismo perfil, en proceso nuevo | 498 superadas, una excluida por selección (`real_data`), cero fallos; 47,30 s |
| Selección de rendimiento | Excluida expresamente; no hay pruebas `performance` implementadas |
| Revisión independiente de código y documentación | Dos revisiones de solo lectura, sin defectos bloqueantes identificados |
| Integridad SHA-256 antes/después | Sin cambios en los cuatro archivos de originales/procedencia, 59 de Bronze, 39 de evidencias privadas y 23 de Ivy |

Los errores del primer intento corresponden a `java.net.SocketException:
Operation not permitted`, no a aserciones de la transformación. No motivaron
cambios en el código de Spark. Las duraciones describen estas ejecuciones de
pruebas, no constituyen un benchmark. La suite general incluye las regresiones
de Bronze, Spark, H03.2 y H03.3. No se ejecutaron ingestas ni caracterizaciones
BDG2 reales, ni se modificaron dependencias o los archivos de Git.

El procedimiento siguiente reproduce el perfil utilizado. Ejecutarlo primero
tal como aparece para la selección focal y, en un proceso nuevo, omitir el
argumento `"tests/test_silver_electricity.py"` para la suite general. Requiere
los JAR ya disponibles y un entorno que permita los sockets locales de Spark;
si falta un artefacto, termina sin descargarlo.

```bash
env -u TFM_REAL_CONFIG .venv/bin/python -B - <<'PY'
import hashlib
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import pytest
from tfm_lakehouse import spark as manager

os.environ["GIT_OPTIONAL_LOCKS"] = "0"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
names = (
    "io.delta_delta-spark_2.12-3.3.3.jar",
    "io.delta_delta-storage-3.3.3.jar",
    "org.antlr_antlr4-runtime-4.9.3.jar",
)
cache = Path.home() / ".ivy2" / "jars"
for name in names:
    source = cache / name
    if source.is_symlink() or not source.is_file():
        raise RuntimeError(f"Required local JAR unavailable: {name}")

with TemporaryDirectory(prefix="tfm-h051-tests-") as temporary:
    root = Path(temporary)
    os.environ["SPARK_LOCAL_DIRS"] = str(root / "spark-local")
    copies = []
    for name in names:
        source, target = cache / name, root / name
        shutil.copyfile(source, target)
        with source.open("rb") as handle:
            before = hashlib.file_digest(handle, "sha256").digest()
        with target.open("rb") as handle:
            assert before == hashlib.file_digest(handle, "sha256").digest()
        copies.append(str(target))

    def local_jars(builder):
        return (builder.config("spark.jars", ",".join(copies))
                .config("spark.jars.packages", "")
                .config("spark.jars.ivy", str(root / "ivy"))
                .config("spark.sql.warehouse.dir", (root / "warehouse").as_uri()))

    manager.configure_spark_with_delta_pip = local_jars
    status = pytest.main([
        "-q", "-p", "no:cacheprovider",
        "tests/test_silver_electricity.py",
        "-m", "not real_data and not performance",
        "--basetemp", str(root / "pytest"),
    ])
    raise SystemExit(status)
PY
```

Quedan fuera: CLI y configuración Silver, selección de edificios reales,
persistencia Silver, reglas de calidad de negocio, interpretación numérica o
temporal, unidades verificadas por fuente, integración meteorológica,
imputación e indicadores Gold. Su aprobación corresponde a H04 y a incrementos
posteriores expresamente autorizados. Este componente no se ha validado con
BDG2 real ni modifica el riesgo residual JVM documentado en AUD-09.

## H05.2 — Interpretación sintética incremental

Las etapas `interpret_electricity_numeric` e `interpret_electricity_temporal`
añaden respectivamente cinco campos numéricos y tres temporales nominales al
formato largo. La segunda conserva el esquema y los valores recibidos, sin
asignar zona horaria ni interpretar las etiquetas como instantes físicos.

La verificación sintética de H05.2.2 y las regresiones seleccionadas de H05.2.1
y H05.1 sumaron 126 pruebas superadas; se excluyó la prueba del lector que
escribe Delta. Las pruebas temporales cubren calendario, precedencia, esquema,
duplicados y composición con ambas etapas anteriores, con distintas opciones
ANSI, zonas horarias y políticas de análisis de fechas. No se han procesado
datos BDG2 reales ni publicado Silver. En aquel momento quedaba pendiente la
integración obligatoria de 10.000 filas largas sintéticas.

En la verificación posterior de H05.2.3, 5.000 filas Bronze sintéticas y dos
columnas de edificio produjeron 10.000 filas largas. Se comprobaron las tres
etapas, los multiconjuntos, duplicados, nulos, linaje, metadatos y esquemas,
así como el resultado tras reordenar y reparticionar la entrada. La selección
de integración y regresión H05 ejecutó 127 pruebas con éxito y excluyó la del
lector que escribe Delta. Otras 65 pruebas sintéticas de fuentes y configuración
pasaron. La selección H05 cubre opciones ANSI, zonas UTC y con DST, y políticas
de análisis temporal. La prueba condicional de 100.000 filas no se ha ejecutado.
La revisión independiente detectó que faltaba materializar y comparar las
etapas intermedias; se añadieron esas comprobaciones y se repitió la selección.
Con estos resultados queda verificado el contrato sintético H05.2. Esto no
autoriza procesar BDG2 real ni publicar tablas Silver.
