# Arquitectura Lakehouse para la gestión y análisis energético multisede en el sector hotelero

Trabajo Fin de Máster — UNIR. Autores: Carlos e Irantzu.

Prototipo local con Apache Spark y Delta Lake para integrar datos históricos de
Building Data Genome Project 2 (BDG2). La organización hotelera es ficticia;
BDG2 no representa una cadena hotelera y sus edificios no se reclasifican como hoteles.

## Estado

Hito 1 verificado mediante escritura y lectura real de Delta. Hito 2 implementado
y probado con fixtures **exclusivamente sintéticos**: electricidad en formato ancho
y metadatos. No hay datos originales BDG2 locales ni resultados experimentales reales.
Silver, Gold, meteorología, visualización y rendimiento siguen pendientes.

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

Cada carga conserva **4 filas de electricidad y 2 de metadatos**, incluidos los
duplicados presentes en el fixture. La segunda sustituye el snapshot completo;
no añade otras cuatro o dos filas. Salidas: `data/lakehouse/synthetic/bronze/`.
Estos conteos corresponden a pruebas de software con datos inventados.

## Incorporar BDG2

Los datos originales no se incluyen ni se descargan automáticamente. Consultar la
[guía técnica](docs/hitos-1-2.md#incorporación-de-bdg2) y adaptar
[config/bronze.example.toml](config/bronze.example.toml) a la procedencia real.
La configuración espera la disposición original bajo `data/raw/bdg2/data/`.

Fuentes: [repositorio oficial](https://github.com/buds-lab/building-data-genome-project-2),
[publicación v1.0 en Zenodo](https://zenodo.org/records/3887306) y
[artículo de referencia](https://doi.org/10.1038/s41597-020-00712-x).

## Documentación

- [Arquitectura, contratos, configuración y comandos](docs/hitos-1-2.md).
- [Evidencias técnicas y limitaciones](docs/evidencias-hitos-1-2.md).
- [Fixtures sintéticos](tests/fixtures/README.md).
