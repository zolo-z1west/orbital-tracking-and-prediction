from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
import math
import pandas as pd

@dataclass(frozen=True)
class ColumnDef:
    name: str
    dtype: Union[type, str]
    unit: Optional[str]
    source: Optional[str]

SCHEMA: Tuple[ColumnDef, ...] = (
    ColumnDef("satellite_id", str, None, "CelesTrak TLE"),
    ColumnDef("satellite_name", str, None, "CelesTrak TLE"),
    ColumnDef("group", str, None, "CelesTrak TLE"),
    ColumnDef("tle_epoch", "datetime", "ISO8601", "CelesTrak TLE"),
    ColumnDef("last_updated_tle", "datetime", "ISO8601", "CelesTrak TLE"),
    ColumnDef("timestamp_utc", "datetime", "ISO8601", "Skyfield"),
    ColumnDef("tle_age_hours", float, "hours", "computed"),
    ColumnDef("temex", float, "km", "Skyfield"),
    ColumnDef("temey", float, "km", "Skyfield"),
    ColumnDef("temez", float, "km", "Skyfield"),
    ColumnDef("temevx", float, "km/s", "Skyfield"),
    ColumnDef("temevy", float, "km/s", "Skyfield"),
    ColumnDef("temevz", float, "km/s", "Skyfield"),
    ColumnDef("alt_deg", float, "degrees", "Skyfield"),
    ColumnDef("az_deg", float, "degrees", "Skyfield"),
    ColumnDef("range_km", float, "km", "Skyfield"),
    ColumnDef("inclination_deg", float, "degrees", "Skyfield"),
    ColumnDef("eccentricity", float, None, "Skyfield"),
    ColumnDef("raan_deg", float, "degrees", "Skyfield"),
    ColumnDef("perigee_km", float, "km", "derived"),
    ColumnDef("apogee_km", float, "km", "derived"),
    ColumnDef("orbital_period_min", float, "minutes", "derived"),
    ColumnDef("mean_anomaly_deg", float, "degrees", "Skyfield"),
    ColumnDef("velocity_mag_kms", float, "km/s", "derived"),
    ColumnDef("subpoint_lat_deg", float, "degrees", "derived"),
    ColumnDef("subpoint_lon_deg", float, "degrees", "derived"),
    ColumnDef("phase_angle_deg", float, "degrees", "derived"),
    ColumnDef("angular_size_deg", float, "degrees", "derived"),
    ColumnDef("cyclical_time_sin", float, None, "computed"),
    ColumnDef("cyclical_time_cos", float, None, "computed"),
    ColumnDef("orbit_class", str, None, "derived"),
    ColumnDef("verified_stellarium", bool, None, "manual"),
    ColumnDef("estimated_error_km", float, "km", "manual"),
    ColumnDef("notes", str, None, "manual"),
    ColumnDef("altitude_km", float, "km", "derived"),
    ColumnDef("speed_to_alt_ratio", float, None, "derived"),
    ColumnDef("local_time_sin", float, None, "derived"),
    ColumnDef("local_time_cos", float, None, "derived"),
    ColumnDef("subpoint_valid", bool, None, "derived"),
    ColumnDef("is_starlink", bool, None, "derived"),
)

def get_column_names() -> List[str]:
    return [c.name for c in SCHEMA]

def get_schema() -> List[Dict[str, Any]]:
    return [asdict(c) for c in SCHEMA]

def _is_float_like(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, (float, int)):
            return True
        s = str(v)
        if s == "":
            return True
        fv = float(s)
        if math.isfinite(fv) or math.isnan(fv):
            return True
    except Exception:
        return False
    return False

def _is_int_like(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, int) and not isinstance(v, bool):
            return True
        if isinstance(v, float) and v.is_integer():
            return True
        s = str(v)
        if s == "":
            return True
        int(s)
        return True
    except Exception:
        return False

def _is_str_like(v: Any) -> bool:
    if v is None:
        return True
    return isinstance(v, str)

def _is_bool_like(v: Any) -> bool:
    if v is None:
        return True
    return isinstance(v, bool)

def _is_datetime_like(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, datetime):
        return True
    if isinstance(v, str) and v != "":
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return True
        except Exception:
            return False
    return False

TYPE_CHECKERS = {
    float: _is_float_like,
    int: _is_int_like,
    str: _is_str_like,
    bool: _is_bool_like,
    "datetime": _is_datetime_like,
}

def validate_row(row: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    for col in SCHEMA:
        name = col.name
        if name not in row:
            errors.append(f"missing:{name}")
            continue
        val = row[name]
        checker = TYPE_CHECKERS.get(col.dtype, lambda v: True)
        ok = checker(val)
        if not ok:
            errors.append(f"type:{name}:{type(val).__name__}")
    return errors

def validate_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    report: Dict[str, Any] = {}
    missing = [c for c in get_column_names() if c not in df.columns]
    report["missing_columns"] = missing
    if missing:
        return report
    null_counts = {c: int(df[c].isna().sum()) for c in df.columns}
    report["null_counts"] = null_counts
    sample = {}
    numeric_cols = [c.name for c in SCHEMA if c.dtype == float or c.dtype == int]
    for c in numeric_cols:
        if c in df.columns:
            try:
                sample[c] = {"min": float(df[c].min()), "max": float(df[c].max()), "mean": float(df[c].mean())}
            except Exception:
                sample[c] = {"min": None, "max": None, "mean": None}
    report["numeric_summary"] = sample
    dup = int(df.duplicated(subset=["satellite_id", "timestamp_utc"]).sum()) if {"satellite_id", "timestamp_utc"}.issubset(df.columns) else 0
    report["duplicate_rows"] = dup
    return report
