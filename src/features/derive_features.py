from __future__ import annotations
from typing import Dict, Any
import math
from pathlib import Path
from datetime import datetime
from skyfield.api import load, wgs84

EARTH_RADIUS_KM = 6371.0
_ts = load.timescale()

def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")

def derive_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    temex = row.get("temex")
    temey = row.get("temey")
    temez = row.get("temez")
    vx = row.get("temevx", None)
    vy = row.get("temevy", None)
    vz = row.get("temevz", None)
    result: Dict[str, Any] = {}
    x = _safe_float(temex)
    y = _safe_float(temey)
    z = _safe_float(temez)
    try:
        r = math.sqrt(x * x + y * y + z * z)
        altitude = r - EARTH_RADIUS_KM
    except Exception:
        altitude = float("nan")
    result["altitude_km"] = altitude
    try:
        if vx is None or vy is None or vz is None:
            vm = _safe_float(row.get("velocity_mag_kms", float("nan")))
        else:
            vm = math.sqrt(float(vx) ** 2 + float(vy) ** 2 + float(vz) ** 2)
    except Exception:
        vm = float("nan")
    result["velocity_mag_kms"] = vm
    try:
        result["speed_to_alt_ratio"] = vm / altitude if altitude and altitude > 0 and not math.isnan(vm) else float("nan")
    except Exception:
        result["speed_to_alt_ratio"] = float("nan")
    try:
        perigee = row.get("perigee_km")
        apogee = row.get("apogee_km")
        per = _safe_float(perigee)
        apo = _safe_float(apogee)
        if not math.isnan(per) and not math.isnan(apo):
            result["orbit_class"] = "LEO" if per < 2000 else ("GEO" if per > 20000 else "MEO")
        else:
            result["orbit_class"] = "LEO" if altitude < 2000 else ("GEO" if altitude > 20000 else "MEO")
    except Exception:
        result["orbit_class"] = "UNKNOWN"
    try:
        ts_val = row.get("timestamp_utc")
        if isinstance(ts_val, str):
            dt = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
        elif isinstance(ts_val, datetime):
            dt = ts_val
        else:
            dt = None
    except Exception:
        dt = None
    if dt is not None:                
        try:
            sec_of_day = dt.hour * 3600 + dt.minute * 60 + dt.second
            result["local_time_sin"] = math.sin(2 * math.pi * sec_of_day / 86400.0)
            result["local_time_cos"] = math.cos(2 * math.pi * sec_of_day / 86400.0)
        except Exception:
            result["local_time_sin"] = float("nan")
            result["local_time_cos"] = float("nan")
    else:
        result["local_time_sin"] = float("nan")
        result["local_time_cos"] = float("nan")
    try:
        sublat = row.get("subpoint_lat_deg")
        sublon = row.get("subpoint_lon_deg")
        if sublat is None or sublon is None or math.isnan(_safe_float(sublat)) or math.isnan(_safe_float(sublon)):
            result["subpoint_valid"] = False
        else:
            result["subpoint_valid"] = True
    
    
    except Exception:
        result["subpoint_valid"] = False
    try:
        if isinstance(row.get("satellite_name"), str) and row.get("satellite_name").upper().find("STARLINK") >= 0:
            result["is_starlink"] = True
        else:
            result["is_starlink"] = False
    except Exception:
        result["is_starlink"] = True 
    return result
