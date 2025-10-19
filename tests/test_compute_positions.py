# tests/test_compute_positions.py
from pathlib import Path
import pandas as pd
import pytest

from src.data_utils import read_tle_file, parse_tles
from src.compute_positions import process_tle_file

ROOT = Path.cwd()
RAW_DIR = ROOT / "data" / "raw"

def _exists_and_nonempty(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 0

def test_read_and_parse_sample_tle():
    sample = RAW_DIR / "INTELSAT.txt"
    assert sample.exists()
    entries = read_tle_file(str(sample))
    assert isinstance(entries, list)
    assert len(entries) > 0
    sats = parse_tles(entries[:3])
    assert isinstance(sats, list)
    assert len(sats) > 0

@pytest.mark.slow
def test_generate_small_geo_sample(tmp_path):
    tle = str(RAW_DIR / "INTELSAT.txt")
    out = tmp_path / "geo_sample.csv.gz"
    process_tle_file(
        tle_file=tle,
        orbit_class="GEO",
        start="2025-10-01T00:00:00Z",
        end="2025-10-01T00:30:00Z",
        freq="00:15:00",
        out=str(out),
        observer=None,
        chunk_size=50,
    )
    assert out.exists()
    df = pd.read_csv(out, nrows=10)
    assert "satellite_name" in df.columns
    assert "temex" in df.columns
    assert df.shape[0] > 0

@pytest.mark.slow
def test_generate_small_leo_sample(tmp_path):
    tle = str(RAW_DIR / "STARLINK.txt")
    out = tmp_path / "leo_sample.csv.gz"
    process_tle_file(
        tle_file=tle,
        orbit_class="LEO",
        start="2025-10-01T00:00:00Z",
        end="2025-10-01T00:10:00Z",
        freq="00:01:00",
        out=str(out),
        observer=None,
        chunk_size=200,
    )
    assert out.exists()
    df = pd.read_csv(out, nrows=20)
    assert "satellite_name" in df.columns
    assert "temex" in df.columns
    assert df.shape[0] > 0
