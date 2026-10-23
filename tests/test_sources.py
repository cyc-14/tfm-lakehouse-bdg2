from pathlib import Path
import pytest

from tfm_lakehouse.ingestion.csv_source import SourceError, inspect_source

pytestmark = pytest.mark.unit


def test_missing_source(tmp_path):
    with pytest.raises(SourceError, match="No existe"):
        inspect_source(tmp_path / "absent.csv", "electricity")


@pytest.mark.parametrize("content,kind,message", [
    ("", "electricity", "vacío"),
    ("date,b\n2016,1\n", "electricity", "obligatorias"),
    ("site_id,sqm\nx,10\n", "metadata", "obligatorias"),
    ("timestamp\n2016\n", "electricity", "al menos"),
    ("timestamp,a,A\n2016,1,2\n", "electricity", "duplicadas"),
    ("timestamp,\n2016,1\n", "electricity", "vacías"),
    ("timestamp,_bronze_source_uri\n2016,x\n", "electricity", "reservado"),
    ("timestamp,bad name\n2016,1\n", "electricity", "compatible"),
    ("version https://git-lfs.github.com/spec/v1\noid sha256:000\nsize 123\n",
     "metadata", "Git LFS"),
])
def test_invalid_header(tmp_path, content, kind, message):
    source = tmp_path / "source.csv"
    source.write_text(content)
    with pytest.raises(SourceError, match=message):
        inspect_source(source, kind)


def test_source_fingerprint_and_exact_header(tmp_path):
    source = tmp_path / "source.csv"
    source.write_text("timestamp,SYNTHETIC_A\n2016,001.00\n")
    info = inspect_source(source, "electricity")
    assert info.columns == ("timestamp", "SYNTHETIC_A")
    assert info.size_bytes == source.stat().st_size
    assert len(info.sha256) == 64
    assert info.path == source.resolve()
