# src/validate_positions.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Optional
import json
import logging

import pandas as pd

from src import schema as schema_mod
from src.io_utils import write_validation_report, read_manifest

logger = logging.getLogger("validate_positions")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(handler)


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    parse_dates = [c for c in schema_mod.get_column_names() if c in ("timestamp_utc", "tle_epoch", "last_updated_tle")]
    return pd.read_csv(path, parse_dates=parse_dates, infer_datetime_format=True)

def validate_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    schema_cols = schema_mod.get_column_names()
    missing = [c for c in schema_cols if c not in df.columns]
    report["missing_columns"] = missing
    if missing:
        return report
    type_report = {}
    null_counts = {c: int(df[c].isna().sum()) for c in schema_cols}
    numeric_summary = {}
    numeric_cols = [c for c in schema_cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]
    for c in numeric_cols:
        try:
            numeric_summary[c] = {"min": float(df[c].min()), "max": float(df[c].max()), "mean": float(df[c].mean())}
        except Exception:
            numeric_summary[c] = {"min": None, "max": None, "mean": None}
    duplicates = int(df.duplicated(subset=["satellite_id", "timestamp_utc"]).sum()) if {"satellite_id", "timestamp_utc"}.issubset(df.columns) else 0
    try:
        alt_min = float(df["alt_deg"].min()) if "alt_deg" in df.columns else None
        alt_max = float(df["alt_deg"].max()) if "alt_deg" in df.columns else None
    except Exception:
        alt_min = alt_max = None
    report.update({"type_report": type_report, "null_counts": null_counts, "numeric_summary": numeric_summary, "duplicate_rows": duplicates, "alt_deg_range": {"min": alt_min, "max": alt_max}})
    return report

def validate_file(path: str, out_report: Optional[str] = None) -> Dict[str, Any]:
    p = Path(path)
    df = _load_csv(p)
    report = validate_dataframe(df)
    report["rows"] = int(len(df))
    report["validated_at"] = pd.Timestamp.utcnow().isoformat()
    if out_report:
        write_validation_report(report, Path(out_report))
    else:
        report_path = p.with_suffix(".validation.json")
        write_validation_report(report, report_path)
    logger.info(f"validation complete for {p}, rows={report['rows']}")
    return report

def validate_against_manifest(csv_path: str, manifest_path: str) -> Dict[str, Any]:
    p = Path(csv_path)
    manifest = read_manifest(Path(manifest_path))
    if not manifest:
        return {"status": "no_manifest"}
    entry = next((m for m in manifest if m.get("partition_path") == str(p)), None)
    if not entry:
        return {"status": "not_in_manifest"}
    report = validate_file(str(p), out_report=entry.get("validation_report"))
    report["manifest_entry"] = entry
    return report

def cli() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--out", required=False)
    parser.add_argument("--manifest", required=False)
    args = parser.parse_args()
    if args.manifest:
        r = validate_against_manifest(args.csv, args.manifest)
    else:
        r = validate_file(args.csv, args.out)
    print(json.dumps(r, indent=2))
if __name__ == "__main__":
    cli()
