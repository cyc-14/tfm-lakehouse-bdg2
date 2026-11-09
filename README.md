# Arquitectura Lakehouse para la gestión y análisis energético multisede en el sector hotelero

Trabajo Fin de Máster — UNIR. Autores: Carlos e Irantzu.

Prototipo local con Apache Spark y Delta Lake para integrar datos históricos de
Building Data Genome Project 2 (BDG2). La organización hotelera es ficticia;
BDG2 no representa una cadena hotelera y sus edificios no se reclasifican como hoteles.

## Estado

H01 verifica escritura y lectura Delta; H02 implementa Bronze para electricidad
en formato ancho, metadatos y meteorología opcional. H03 dispone de herramientas
de caracterización y ejecuciones reales históricas contrastadas. Su evolución
eléctrica v2 solo se ha probado con datos sintéticos; el cierre académico global
de H03 sigue pendiente.

[H04](docs/hitos-4.md) documenta los 16 edificios aprobados conjuntamente;
siguen pendientes la reproducción de la selección y su procedencia.
[H05](docs/hitos-5.md) implementa transformación eléctrica, interpretación numérica
y temporal nominal, vínculo Bronze, publicación Silver, diagnóstico de calidad,
motor IQR y publicación IQR versionada, con verificación sintética documentada.
La orquestación Silver → IQR y el cierre formal de H05 están pendientes.
La integración meteorológica y Gold quedan fuera del alcance implementado.

El [registro de hitos](docs/milestones.md) es la referencia del estado actual.
Las pruebas habituales usan datos sintéticos. No se ha autorizado validar ni
publicar Silver/IQR con BDG2 real. Los originales y sus salidas permanecen
locales, fuera de Git; una carga Bronze no demuestra resultados energéticos.

## Organización del código

El código se agrupa por responsabilidad en `src/tfm_lakehouse/ingestion/`,
`selection/`, `comparison/` y `silver/`. La configuración, Spark y las
herramientas compartidas permanecen en la raíz del paquete. Las pruebas siguen
la misma organización, con los fixtures comunes en `tests/fixtures/`.

## Entorno verificado

Ubuntu 26.04.1 LTS en WSL2, Python 3.11.17, OpenJDK 17.0.20.1,
PySpark 3.5.7, delta-spark 3.3.3 y uv 0.13.0.
Las versiones originales de Spark y Delta se han mantenido.

Ejecutar estos comandos en **Ubuntu/WSL**, desde la raíz del repositorio.
Si uv no está en PATH: `export PATH="$HOME/.local/bin:$PATH"`.

```bash
# Solo si todavía no existe .venv:
uv venv --python 3.11.17

# Dependencias exactas verificadas y paquete local:
uv pip install --python .venv/bin/python -r requirements.lock -e .
uv pip check --python .venv/bin/python

.venv/bin/python -m tfm_lakehouse check-environment --report data/checks/environment/check-001.json
env -u TFM_REAL_CONFIG .venv/bin/python -m pytest -q -m "not real_data and not performance"
```

La primera sesión Spark descarga los JAR de Delta desde Maven; se necesita Java 17
y acceso de red o la caché Ivy preparada. La configuración usa el mismo intérprete
para driver y workers sin modificar la configuración global del sistema.
La suite sintética incluye los dos casos Git corregidos en AUD-06; no inicializa
repositorios. Para comprobar Spark/Delta sin descargas ni cambios en la caché
histórica, utilizar el [perfil temporal con JAR locales](docs/hitos-1-2.md).

`--report` solo admite un JSON nuevo directamente en `data/checks/environment/`,
desde la raíz del proyecto. Se rechazan archivos existentes y enlaces; para repetir
la comprobación, elegir otro nombre. El antiguo `data/checks/environment.json` y
las evidencias previas se conservan intactos: no se trasladan ni se sobrescriben.
Sin `--report`, la comprobación mantiene su salida por consola.

## Demostración sintética de Bronze

```bash
.venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.synthetic.toml
.venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.synthetic.toml
.venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.synthetic.toml
```

Cada carga conserva **4 filas de electricidad, 2 de metadatos y 3 de meteorología**,
incluidos los duplicados presentes en los fixtures. La segunda sustituye el snapshot
completo de cada fuente. Salidas: `data/lakehouse/synthetic/bronze/`.
Estos conteos corresponden a pruebas de software con datos inventados.

Cada ejecución satisfactoria publica las versiones Delta exactas de sus tablas.
`inspect-bronze` consulta ese conjunto completo; si una recarga falla, recupera
la ejecución compatible anterior e indica esta circunstancia. Las escrituras
Delta siguen siendo transaccionales por tabla, no por el conjunto.

## Uso del prototipo con datos sintéticos

1. Prepara el [entorno local](#entorno-verificado) y comprueba Java, Spark y
   Delta con `check-environment`; elige un nombre de informe nuevo.
2. Ejecuta los comandos de ingesta e inspección anteriores con
   `config/bronze.synthetic.toml`. El informe identifica el `run_id` Bronze y
   las versiones Delta publicadas.
3. La CLI ofrece `check-environment`, `ingest-bronze` e `inspect-bronze`.
   Silver e IQR se publican mediante llamadas Python separadas; no existe un
   orquestador Silver → IQR. La guía [H05](docs/hitos-5.md#uso-de-la-api-de-publicación)
   muestra las llamadas y sus requisitos. El fixture público tiene dos columnas
   eléctricas y no cumple el requisito Silver de 16 edificios; por eso no se
   presenta como un ejemplo ejecutable de Silver/IQR.
4. Al leer una pareja compatible, `pending` significa que aún no hay IQR;
   `not_accepted` indica que se publicó, pero algún edificio no superó la
   aceptación por población o cobertura. No equivale a un fallo de publicación.
   Consulta las coberturas y
   señales por edificio en la guía H05. Ninguna anotación cambia las mediciones.

## Incorporar BDG2

Los datos originales no se incluyen ni se descargan automáticamente. Consultar la
[guía técnica](docs/hitos-1-2.md) y adaptar
[config/bronze.example.toml](config/bronze.example.toml) a la procedencia real.
Los comandos siguientes son plantillas: una ejecución real requiere autorización
expresa y revisión previa de las rutas de entrada, salida y sus efectos.
La configuración espera electricidad, metadatos y meteorología en su disposición
original bajo `data/raw/bdg2/data/`. Para usar solo las dos primeras fuentes, omitir
`sources.weather` en el TOML.

Desde **PowerShell**, sustituir la ruta de ejemplo por la ubicación del repositorio
dentro de Ubuntu/WSL y ejecutar cada comando `wsl` en una sola línea:

```powershell
$RepositoryWslPath = '/ruta/al/repositorio/tfm-lakehouse'
wsl -d Ubuntu --cd $RepositoryWslPath -- .venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.example.toml
wsl -d Ubuntu --cd $RepositoryWslPath -- .venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.example.toml
```

Fuentes: [repositorio oficial](https://github.com/buds-lab/building-data-genome-project-2),
[publicación v1.0 en Zenodo](https://zenodo.org/records/3887306) y
[artículo de referencia](https://doi.org/10.1038/s41597-020-00712-x).

## Documentación

- [Arquitectura, contratos, configuración y comandos](docs/hitos-1-2.md).
- [Plan de hitos técnicos y *milestones* de GitHub](docs/milestones.md).
- [Caracterización eléctrica y metadatos — H03](docs/h03-2-characterization.md).
- [Meteorología y diagnóstico temporal — H03](docs/h03-3-weather-timezones.md).
- [Electricidad Silver, calidad e IQR versionado — H05](docs/hitos-5.md).
- [Definiciones preparadas para los 12 *milestones*](docs/github-milestones.json).
- [Fixtures sintéticos](tests/fixtures/README.md).
