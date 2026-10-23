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

    @property
    def namespace(self) -> Path:
        return self.lakehouse_root / self.data_kind

    def validate(self) -> None:
        if self.data_kind not in {"real", "synthetic"}:
            raise ConfigurationError("dataset.kind debe ser real o synthetic.")
        if not self.dataset_reference.strip():
            raise ConfigurationError("Falta dataset.reference (versión/procedencia declarada).")
        raw, output = self.raw_root.resolve(), self.lakehouse_root.resolve()
        if raw == output or raw in output.parents or output in raw.parents:
            raise ConfigurationError("raw_root y lakehouse_root deben estar separados.")
        # Nested symlinks can escape the output root even when the configured roots are separate.
        for destination in (
            self.namespace, self.namespace / "_runs",
            self.namespace / "bronze/electricity", self.namespace / "bronze/metadata",
        ):
            if not destination.resolve().is_relative_to(output):
                raise ConfigurationError("Un destino enlazado sale de lakehouse_root.")
        for source in (self.electricity, self.metadata):
            if not source.resolve().is_relative_to(raw):
                raise ConfigurationError("Las fuentes deben estar dentro de raw_root.")
        if self.electricity.resolve() == self.metadata.resolve():
            raise ConfigurationError("Electricidad y metadatos deben ser archivos distintos.")


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
        result = BronzeConfig(
            raw_root=raw, lakehouse_root=resolve(data["paths"]["lakehouse_root"]),
            electricity=(raw / data["sources"]["electricity"]).resolve(),
            metadata=(raw / data["sources"]["metadata"]).resolve(),
            data_kind=data["dataset"]["kind"],
            dataset_reference=data["dataset"]["reference"],
            master=spark.get("master", "local[2]"),
            ivy_dir=resolve(spark["ivy_dir"]) if "ivy_dir" in spark else None,
        )
        result.validate()
        return result
    except (OSError, KeyError, TypeError, AttributeError, tomllib.TOMLDecodeError) as exc:
        raise ConfigurationError(f"Configuración inválida {path}: {exc}") from exc
