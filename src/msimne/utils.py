from __future__ import annotations

import datetime as dt
import time
from functools import wraps

from dateutil.relativedelta import relativedelta


VALID_CODES = {f"R{i:02d}" for i in range(1, 15)}


def parse_date(value: str) -> dt.datetime:
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError("Formato data non valido. Usa YYYY-MM-DD oppure YYYY-MM.")


def monthly_windows(start_dt: dt.datetime, end_dt: dt.datetime) -> list[tuple[dt.datetime, dt.datetime]]:
    windows: list[tuple[dt.datetime, dt.datetime]] = []
    cur_start = start_dt
    cur = start_dt.replace(day=1)
    while True:
        next_month = (cur + relativedelta(months=1)).replace(day=1)
        windows.append((cur_start, min(end_dt, next_month)))
        if windows[-1][1] >= end_dt:
            return windows
        cur_start = next_month
        cur = next_month


def get_utm_crs_from_lat_lon(lat: float, lon: float) -> str:
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return f"EPSG:{epsg}"


def get_tile_id(geom) -> str:
    x_min, y_min, _, _ = geom.bounds
    lat = y_min + 0.2
    lat_prefix = "N" if lat >= 0 else "S"
    lon_prefix = "E" if x_min >= 0 else "W"
    lat_str = f"{lat_prefix}{abs(lat):06.2f}".replace(".", "_")
    lon_str = f"{lon_prefix}{abs(x_min):06.2f}".replace(".", "_")
    return f"{lat_str}-{lon_str}"


def retry(times: int, delay_seconds: int):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import random

            last_err = None
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # pragma: no cover
                    last_err = exc
                    wait = delay_seconds * (2 ** attempt) + random.uniform(0, 1.5)
                    time.sleep(wait)
            if last_err:
                raise last_err
            return None

        return wrapper

    return deco
