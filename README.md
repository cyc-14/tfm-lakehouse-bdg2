# Arquitectura Lakehouse para la gestión y análisis energético multisede en el sector hotelero

Trabajo Fin de Estudios - UNIR

## Objetivo

Desarrollar un prototipo Lakehouse basado en Apache Spark
y Delta Lake para integrar, transformar y analizar datos
energéticos del Building Data Genome Project 2 (BDG2).

El caso hotelero se plantea como una cadena simulada.
Los datos energéticos proceden del conjunto original BDG2.

## Tecnologías

- Python 3.11
- Apache Spark / PySpark 3.5.7
- Delta Lake 3.3.3
- Java 17
- Ubuntu mediante WSL2

## Arquitectura prevista

Datos originales -> Bronze -> Silver -> Gold -> Análisis

## Estado actual

Entorno de desarrollo preparado.
Pruebas funcionales de Spark y Delta Lake pendientes.

## Instalación de dependencias

```bash
uv venv --python 3.11
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Datos

Los conjuntos de datos originales no se incluyen en
este repositorio. Fuente: https://zenodo.org/records/3887306
