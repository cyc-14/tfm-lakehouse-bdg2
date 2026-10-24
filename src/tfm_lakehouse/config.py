"""TOML configuration with paths relative to the file, never the working directory."""
from dataclasses import dataclass
from pathlib import Path
import tomllib


class ConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class BronzeConfig:
    raw_root: Path
    lakehouse_root: Path
    electricity: Path
    metadata: Path
    data_kind: str
    dataset_reference: str
    master: str = "local[2]"
    ivy_dir: Path | None = None
    weather: Path | None = None

    @property
    def namespace(self) -> Path:
        return self.lakehouse_root / self.data_kind

    @property
    def sources(self) -> dict[str, Path]:
        sources = {"electricity": self.electricity, "metadata": self.metadata}
        if self.weather is not None:
            sources["weather"] = self.weather
        return sources

    def table_path(self, name: str) -> Path:
        if name not in self.sources:
            raise ConfigurationError(f"Fuente no configurada: {name}")
        return self.namespace / "bronze" / name

    def validate(self) -> None:
        if self.data_kind not in {"real", "synthetic"}:
            raise ConfigurationError("dataset.kind debe ser real o synthetic.")
        if not self.dataset_reference.strip():
            raise ConfigurationError("Falta dataset.reference (versión/procedencia declarada).")
        raw, output = self.raw_root.resolve(), self.lakehouse_root.resolve()
        if raw == output or raw in output.parents or output in raw.parents:
            raise ConfigurationError("raw_root y lakehouse_root deben estar separados.")
        namespace = output / self.data_kind
        # Canonical destinations prevent aliases between tables and real/synthetic namespaces.
        for relative in (Path("."), Path("_runs"), Path("_ingestion.lock"), Path("bronze"),
                         *(Path("bronze") / name for name in self.sources)):
            if (self.namespace / relative).resolve() != namespace / relative:
                raise ConfigurationError(
                    "Un destino enlazado cambia la ubicación del namespace o de una tabla."
                )
        resolved_sources = [path.resolve() for path in self.sources.values()]
        for source in resolved_sources:
            if not source.is_relative_to(raw):
                raise ConfigurationError("Las fuentes deben estar dentro de raw_root.")
        if len(set(resolved_sources)) != len(resolved_sources):
            raise ConfigurationError("Las fuentes deben ser archivos distintos.")


def load_config(path: Path) -> BronzeConfig:
    path = path.expanduser().resolve()
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
        base = path.parent

        def resolve(value: str) -> Path:
            return (base / Path(value).expanduser()).resolve()

        raw = resolve(data["paths"]["raw_root"])
        spark = data.get("spark", {})
        sources = data["sources"]
        result = BronzeConfig(
            raw_root=raw, lakehouse_root=resolve(data["paths"]["lakehouse_root"]),
            electricity=(raw / sources["electricity"]).resolve(),
            metadata=(raw / sources["metadata"]).resolve(),
            data_kind=data["dataset"]["kind"],
            dataset_reference=data["dataset"]["reference"],
            master=spark.get("master", "local[2]"),
            ivy_dir=resolve(spark["ivy_dir"]) if "ivy_dir" in spark else None,
            weather=(raw / sources["weather"]).resolve() if "weather" in sources else None,
        )
        result.validate()
        return result
    except (OSError, KeyError, TypeError, AttributeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigurationError(f"Configuración inválida {path}: {exc}") from exc
