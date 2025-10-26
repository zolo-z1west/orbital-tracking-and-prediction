from __future__ import annotations
import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from skyfield.api import EarthSatellite, load, wgs84
from tqdm import tqdm

from src import data_utils
from src import schema as schema_mod
from src.features import derive_features

import logging

logger = logging.getLogger("compute_positions")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(handler)


def _parse_iso8601(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1]
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_time_grid(start: datetime, end: datetime, freq: str) -> List[datetime]:
    h, m, sec = [int(x) for x in freq.split(":")]
    step = timedelta(hours=h, minutes=m, seconds=sec)
    if step.total_seconds() <= 0:
        raise ValueError("freq must be positive")
    cur = start
    out: List[datetime] = []
    while cur <= end:
        out.append(cur)
        cur = cur + step
    return out


def _teme_pv(sat: EarthSatellite, ts, dt: datetime) -> Tuple[Tuple[float, float, float], Tuple[float, float, float]]:
    t = ts.utc(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond / 1e6)
    g = sat.at(t)
    pos = g.position.km
    vel = g.velocity.km_per_s
    return (float(pos[0]), float(pos[1]), float(pos[2])), (float(vel[0]), float(vel[1]), float(vel[2]))


def _topo_altaz_range(sat: EarthSatellite, ts, dt: datetime, observer: Optional[Tuple[float, float, float]] = None) -> Dict[str, Optional[float]]:
    if observer is None:
        return {"alt_deg": float("nan"), "az_deg": float("nan"), "range_km": float("nan")}
    lat, lon, elev = observer
    t = ts.utc(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond / 1e6)
    obs = wgs84.latlon(latitude_degrees=lat, longitude_degrees=lon, elevation_m=elev)
    diff = sat - obs
    top = diff.at(t)
    alt, az, dist = top.altaz()
    return {"alt_deg": float(alt.degrees), "az_deg": float(az.degrees), "range_km": float(dist.km)}


def _tle_epoch(sat: EarthSatellite) -> Optional[datetime]:
    try:
        return sat.epoch.utc_datetime().replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _extract_model_fields(sat: EarthSatellite) -> Dict[str, float]:
    try:
        m = sat.model
        return {
            "inclination_deg": float(getattr(m, "inclo", float("nan"))),
            "eccentricity": float(getattr(m, "ecco", float("nan"))),
            "raan_deg": float(getattr(m, "nodeo", float("nan"))),
            "mean_anomaly_deg": float(getattr(m, "mo", float("nan"))),
            "mean_motion": float(getattr(m, "no_kozai", float("nan"))),
            "arg_perigee_deg": float(getattr(m, "argpo", float("nan"))),
        }
    except Exception:
        return {
            "inclination_deg": float("nan"),
            "eccentricity": float("nan"),
            "raan_deg": float("nan"),
            "mean_anomaly_deg": float("nan"),
            "mean_motion": float("nan"),
            "arg_perigee_deg": float("nan"),
        }


def _compose_row(sat: EarthSatellite, sat_id: Any, orbit_class: str, dt: datetime, observer: Optional[Tuple[float, float, float]], ts) -> Dict[str, Any]:
    (x, y, z), (vx, vy, vz) = _teme_pv(sat, ts, dt)
    topo = _topo_altaz_range(sat, ts, dt, observer)
    model_fields = _extract_model_fields(sat)
    vel_mag = math.sqrt(vx * vx + vy * vy + vz * vz)
    epoch = _tle_epoch(sat)
    tle_age = (dt - epoch).total_seconds() / 3600.0 if epoch is not None else float("nan")
    try:
        t_sf = ts.utc(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond / 1e6)
        sub = wgs84.subpoint(sat.at(t_sf))
        sub_lat = float(sub.latitude.degrees)
        sub_lon = float(sub.longitude.degrees)
    except Exception:
        sub_lat = float("nan")
        sub_lon = float("nan")
    cyc_sin = math.sin(2 * math.pi * (dt.hour * 3600 + dt.minute * 60 + dt.second) / 86400.0)
    cyc_cos = math.cos(2 * math.pi * (dt.hour * 3600 + dt.minute * 60 + dt.second) / 86400.0)
    row: Dict[str, Any] = {
        "satellite_id": str(sat_id),
        "satellite_name": sat.name,
        "group": orbit_class,
        "tle_epoch": epoch.isoformat() if epoch else "",
        "last_updated_tle": epoch.isoformat() if epoch else "",
        "timestamp_utc": dt.isoformat(),
        "tle_age_hours": float(tle_age),
        "temex": x,
        "temey": y,
        "temez": z,
        "alt_deg": topo["alt_deg"],
        "az_deg": topo["az_deg"],
        "range_km": topo["range_km"],
        "inclination_deg": model_fields["inclination_deg"],
        "eccentricity": model_fields["eccentricity"],
        "raan_deg": model_fields["raan_deg"],
        "perigee_km": float("nan"),
        "apogee_km": float("nan"),
        "orbital_period_min": float("nan"),
        "mean_anomaly_deg": model_fields["mean_anomaly_deg"],
        "velocity_mag_kms": vel_mag,
        "subpoint_lat_deg": sub_lat,
        "subpoint_lon_deg": sub_lon,
        "phase_angle_deg": float("nan"),
        "angular_size_deg": float("nan"),
        "cyclical_time_sin": cyc_sin,
        "cyclical_time_cos": cyc_cos,
        "orbit_class": orbit_class,
        "verified_stellarium": False,
        "estimated_error_km": float("nan"),
        "notes": ""
    }
    try:
        extras = derive_features.derive_from_row(row)
        row.update(extras)
    except Exception:
        pass
    for c in schema_mod.get_column_names():
        if c not in row:
            row[c] = float("nan") if any(k in c for k in ("deg", "km", "velocity", "_hours")) else ""
    return row


def process_tle_file(tle_file: str, orbit_class: str, start: str, end: str, freq: str, out: str, observer: Optional[Tuple[float, float, float]] = None, chunk_size: int = 5000) -> None:
    tle_path = Path(tle_file)
    out_path = Path(out)
    ensure_parent_dir(out_path)
    start_dt = _parse_iso8601(start)
    end_dt = _parse_iso8601(end)
    grid = _build_time_grid(start_dt, end_dt, freq)
    ts = load.timescale()
    tle_entries = data_utils.read_tle_file(str(tle_path))
    sats = data_utils.parse_tles(tle_entries)
    col_order = schema_mod.get_column_names()
    buffer: List[Dict[str, Any]] = []
    rows_written = 0
    obs_tuple = observer if observer is not None else None
    for sat in tqdm(sats, desc="satellites"):
        sid = getattr(sat.model, "satnum", None) if hasattr(sat, "model") else None
        if sid is None:
            sid = sat.name
        for dt in grid:
            row = _compose_row(sat, sid, orbit_class, dt, obs_tuple, ts)
            buffer.append(row)
            if len(buffer) >= chunk_size:
                append_rows_to_csv(buffer, col_order, out_path)
                rows_written += len(buffer)
                buffer = []
    if buffer:
        append_rows_to_csv(buffer, col_order, out_path)
        rows_written += len(buffer)
    report = {
        "tle_file": str(tle_path),
        "orbit_class": orbit_class,
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "freq": freq,
        "sat_count": len(sats),
        "rows_written": rows_written,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
    report_path = out_path.with_suffix(".validation.json")
    with report_path.open("w") as fh:
        json.dump(report, fh, indent=2)
    logger.info(f"wrote {rows_written} rows to {out_path}")
    logger.info(f"validation report: {report_path}")


def cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tle-file", required=True)
    p.add_argument("--orbit-class", required=True, choices=["LEO", "GEO"])
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--freq", default="00:01:00")
    p.add_argument("--out", required=True)
    p.add_argument("--observer-lat", type=float, default=None)
    p.add_argument("--observer-lon", type=float, default=None)
    p.add_argument("--observer-elev", type=float, default=0.0)
    p.add_argument("--chunk-size", type=int, default=5000)
    args = p.parse_args()
    observer = None
    if args.observer_lat is not None and args.observer_lon is not None:
        observer = (args.observer_lat, args.observer_lon, args.observer_elev)
    process_tle_file(args.tle_file, args.orbit_class, args.start, args.end, args.freq, args.out, observer, args.chunk_size)


if __name__ == "__main__":
    cli()
