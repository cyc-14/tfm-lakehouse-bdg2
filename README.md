# Arquitectura Lakehouse para la gestión y análisis energético multisede en el sector hotelero

Trabajo Fin de Máster — UNIR. Autores: Carlos e Irantzu.

Prototipo local con Apache Spark y Delta Lake para integrar datos históricos de
Building Data Genome Project 2 (BDG2). La organización hotelera es ficticia;
BDG2 no representa una cadena hotelera y sus edificios no se reclasifican como hoteles.

## Estado

Hito 1 verificado mediante escritura y lectura real de Delta. Hito 2 implementa
Bronze para electricidad en formato ancho, metadatos y meteorología opcional.
Las pruebas de software usan fixtures **exclusivamente sintéticos**; los originales
BDG2 y sus salidas se conservan localmente, fuera de Git.
Silver, Gold, integración analítica, visualización y evaluación de rendimiento
siguen pendientes. Una carga Bronze no demuestra calidad ni resultados energéticos.

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

.venv/bin/python -m tfm_lakehouse check-environment --report data/checks/environment.json
.venv/bin/python -m pytest -q
```

La primera sesión Spark descarga los JAR de Delta desde Maven; se necesita Java 17
y acceso de red o la caché Ivy preparada. La configuración usa el mismo intérprete
para driver y workers sin modificar la configuración global del sistema.

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

## Incorporar BDG2

Los datos originales no se incluyen ni se descargan automáticamente. Consultar la
[guía técnica](docs/hitos-1-2.md) y adaptar
[config/bronze.example.toml](config/bronze.example.toml) a la procedencia real.
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
- [Definiciones preparadas para los 12 *milestones*](docs/github-milestones.json).
- [Fixtures sintéticos](tests/fixtures/README.md).
