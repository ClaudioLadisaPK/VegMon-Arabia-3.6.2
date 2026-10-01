from __future__ import annotations

import datetime as dt
import logging
import time
from functools import wraps

from dateutil.relativedelta import relativedelta

LOGGER = logging.getLogger(__name__)

VALID_CODES = {f"R{i:02d}" for i in range(1, 14)}


class NonRetryableError(Exception):
    pass


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


def previous_month_window(today: dt.date | None = None) -> tuple[dt.datetime, dt.datetime]:
    today = today or dt.date.today()
    first_this_month = dt.datetime(today.year, today.month, 1)
    first_previous_month = first_this_month - relativedelta(months=1)
    return first_previous_month, first_this_month


def get_tile_id(geom) -> str:
    x_min, y_min, _, _ = geom.bounds
    lat = y_min + 0.2
    lat_prefix = "N" if lat >= 0 else "S"
    lon_prefix = "E" if x_min >= 0 else "W"
    lat_str = f"{lat_prefix}{abs(lat):06.2f}".replace(".", "_")
    lon_str = f"{lon_prefix}{abs(x_min):06.2f}".replace(".", "_")
    return f"{lat_str}-{lon_str}"


_NETWORK_ERROR_TYPES = {
    "ConnectionError",
    "ConnectTimeout",
    "ReadTimeout",
    "Timeout",
    "MaxRetryError",
    "NameResolutionError",
    "NewConnectionError",
    "ProtocolError",
    "ChunkedEncodingError",
    "TimeoutError",
}
_NETWORK_ERROR_TEXT = (
    "name resolution",
    "temporary failure",
    "timed out",
    "connection",
    "curl error",
    "http response code",
    "429",
    "502",
    "503",
    "504",
)


def is_network_error(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if type(current).__name__ in _NETWORK_ERROR_TYPES:
            return True
        text = str(current).lower()
        if any(token in text for token in _NETWORK_ERROR_TEXT):
            return True
        current = current.__cause__ or current.__context__
    return False


def wait_for_network(url: str, max_wait_seconds: int, poll_seconds: int = 30) -> bool:
    """Attende che l'endpoint risponda; True se raggiungibile entro max_wait_seconds."""
    import requests

    deadline = time.monotonic() + max(0, max_wait_seconds)
    first = True
    while True:
        try:
            response = requests.get(url, timeout=15)
            if response.status_code < 500 and response.status_code != 429:
                if not first:
                    LOGGER.info("Rete di nuovo raggiungibile: %s", url)
                return True
            reason = f"HTTP {response.status_code}"
        except Exception as exc:
            reason = str(exc).splitlines()[0][:200]
        if time.monotonic() >= deadline:
            LOGGER.error("Rete non raggiungibile dopo %ss (%s): %s", max_wait_seconds, url, reason)
            return False
        if first:
            LOGGER.warning("Rete non raggiungibile (%s), attendo fino a %ss: %s", url, max_wait_seconds, reason)
            first = False
        time.sleep(poll_seconds)


def call_with_retries(
    func,
    *args,
    attempts: int,
    delay_seconds: int,
    label: str,
    network_url: str | None = None,
    network_wait_seconds: int = 0,
    **kwargs,
):
    """Un solo livello di tentativi; sugli errori di rete prima aspetta che la rete torni."""
    import random

    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except NonRetryableError:
            raise
        except Exception as exc:
            if attempt >= attempts:
                raise
            network = is_network_error(exc)
            LOGGER.warning(
                "Tentativo %s/%s fallito per %s%s: %s",
                attempt,
                attempts,
                label,
                " (errore di rete)" if network else "",
                str(exc).splitlines()[0][:300] if str(exc) else type(exc).__name__,
            )
            if network and network_url and network_wait_seconds > 0:
                wait_for_network(network_url, network_wait_seconds)
            wait = min(delay_seconds * (2 ** (attempt - 1)), 300) + random.uniform(0, 1.5)
            time.sleep(wait)
    return None


def retry(times: int, delay_seconds: int):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import random

            last_err = None
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except NonRetryableError:
                    raise
                except Exception as exc:  # pragma: no cover
                    last_err = exc
                    if attempt >= times - 1:
                        break
                    LOGGER.warning(
                        "Tentativo %s/%s fallito in %s: %s",
                        attempt + 1,
                        times,
                        func.__name__,
                        exc,
                    )
                    wait = delay_seconds * (2 ** attempt) + random.uniform(0, 1.5)
                    time.sleep(wait)
            if last_err:
                raise last_err
            return None

        return wrapper

    return deco
