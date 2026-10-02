from __future__ import annotations

import logging
from collections.abc import Sequence
from threading import Lock

import numpy as np
import odc.stac
import shapely
from shapely.geometry import shape
import planetary_computer
import rasterio
import xarray as xr
from pystac_client import Client as StacClient
from pystac_client.stac_api_io import StacApiIO
from urllib3.util.retry import Retry

from .config import Settings
from .utils import NonRetryableError

LOGGER = logging.getLogger(__name__)


class InsufficientSclCoverage(NonRetryableError):
    def __init__(self, ratio: float) -> None:
        super().__init__(f"Copertura SCL insufficiente: {ratio:.2%}")
        self.ratio = ratio


def bbox_intersects(left: Sequence[float], right: Sequence[float]) -> bool:
    return left[0] <= right[2] and left[2] >= right[0] and left[1] <= right[3] and left[3] >= right[1]


class StacItemCache:
    def __init__(self, catalog: StacClient, settings: Settings, bounds_wgs84: Sequence[float]) -> None:
        self.catalog = catalog
        self.settings = settings
        self.bounds_wgs84 = tuple(float(value) for value in bounds_wgs84)
        self._items_by_range: dict[tuple[str, str], list] = {}
        self._lock = Lock()

    def _range_items(self, current_range: tuple[str, str]) -> list:
        with self._lock:
            cached = self._items_by_range.get(current_range)
            if cached is not None:
                return cached

            search = self.catalog.search(
                collections=["sentinel-2-l2a"],
                bbox=self.bounds_wgs84,
                datetime=current_range,
                query={"eo:cloud_cover": {"lt": self.settings.cloud_cover_lt}},
            )
            items = list(search.items())
            self._items_by_range[current_range] = items
        LOGGER.info(
            "STAC cache %s->%s scene=%s bbox=%s",
            current_range[0],
            current_range[1],
            len(items),
            ",".join(f"{value:.5f}" for value in self.bounds_wgs84),
        )
        return items

    def search_items(self, geom_wgs84, ranges: list[tuple[str, str]]) -> tuple[list, list[str]]:
        tile_bounds = tuple(float(value) for value in geom_wgs84.bounds)
        items = []
        found_by_range = []
        seen: set[str] = set()
        for current_range in ranges:
            range_items = [
                item
                for item in self._range_items(current_range)
                if getattr(item, "bbox", None) is None or bbox_intersects(item.bbox, tile_bounds)
            ]
            found_by_range.append(f"{current_range[0]}->{current_range[1]}:{len(range_items)}")
            for item in range_items:
                item_key = getattr(item, "id", None) or repr(item)
                if item_key in seen:
                    continue
                seen.add(item_key)
                items.append(item)
        return items, found_by_range

    def sign_items(self, items: list) -> list:
        return [planetary_computer.sign(item) for item in items]


COVERAGE_GRID_SIZE = 40
MAX_SCENES_FACTOR = 3
# Scene con nuvolosita' nella stessa fascia (es. 0-5%) sono equivalenti: le nuvole le toglie l'SCL.
# Dentro la fascia si preferiscono le scene che coprono piu' tile, cosi' le strisce parziali
# servono solo dove mancano scene complete (meno scene per tile = run piu' veloce).
CLOUD_BUCKET_PCT = 5.0


def _coverage_points(geom_wgs84, size: int = COVERAGE_GRID_SIZE) -> np.ndarray:
    minx, miny, maxx, maxy = geom_wgs84.bounds
    xs = np.linspace(minx, maxx, size + 2)[1:-1]
    ys = np.linspace(miny, maxy, size + 2)[1:-1]
    grid_x, grid_y = np.meshgrid(xs, ys)
    inside = shapely.contains_xy(geom_wgs84, grid_x.ravel(), grid_y.ravel())
    return np.column_stack([grid_x.ravel()[inside], grid_y.ravel()[inside]])


def _item_footprint(item):
    geometry = getattr(item, "geometry", None)
    if not geometry:
        return None
    try:
        footprint = shape(geometry)
    except Exception:
        return None
    return footprint if not footprint.is_empty else None


def select_items_for_coverage(items: list, geom_wgs84, depth: int) -> tuple[list, float]:
    """Sceglie le scene meno nuvolose garantendo che ogni zona della tile ne abbia almeno `depth`.

    Le scene sono gia' ordinate per nuvolosita'. Una scena viene presa solo se copre almeno una
    zona della tile che ha ancora meno di `depth` scene: cosi' le scene parziali (bordo di un
    riquadro MGRS o di un'orbita), spesso con nuvolosita' quasi nulla, non possono piu' occupare
    tutti i posti lasciando scoperto il resto della tile. Il limite MAX_SCENES_FACTOR * depth
    tiene il volume di dati simile a prima (le scene parziali vengono lette solo sulla loro parte).
    Restituisce le scene scelte e la quota della tile coperta da almeno una di esse.
    """
    points = _coverage_points(geom_wgs84)
    if len(points) == 0 or depth <= 0:
        return items[:depth], 1.0
    counts = np.zeros(len(points), dtype=np.int32)
    selected = []
    max_scenes = max(depth, depth * MAX_SCENES_FACTOR)
    for item in items:
        footprint = _item_footprint(item)
        if footprint is None:
            covers = np.ones(len(points), dtype=bool)
        else:
            covers = shapely.contains_xy(footprint, points[:, 0], points[:, 1])
        if not covers.any() or not (counts[covers] < depth).any():
            continue
        selected.append(item)
        counts[covers] += 1
        if (counts >= depth).all() or len(selected) >= max_scenes:
            break
    return selected, float(np.count_nonzero(counts > 0)) / len(points)


def open_catalog(settings: Settings) -> StacClient:
    http_retry = Retry(
        total=settings.http_max_retry,
        backoff_factor=settings.http_retry_delay,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=None,
        respect_retry_after_header=True,
    )
    stac_io = StacApiIO(timeout=settings.http_timeout, max_retries=http_retry)
    return StacClient.open(settings.stac_url, stac_io=stac_io)


def gdal_http_options(settings: Settings) -> dict[str, str]:
    return {
        "GDAL_HTTP_MAX_RETRY": str(settings.http_max_retry),
        "GDAL_HTTP_RETRY_DELAY": str(settings.http_retry_delay),
        "GDAL_HTTP_TIMEOUT": str(settings.http_timeout),
        "GDAL_HTTP_CONNECTTIMEOUT": str(min(settings.http_timeout, 30)),
    }


def compute_valid_ratio(mask_bool: xr.DataArray) -> float:
    return float(mask_bool.mean().compute().item())


def _normalize_ranges(rng: tuple[str, str] | Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    if isinstance(rng, tuple) and len(rng) == 2 and isinstance(rng[0], str):
        return [rng]
    return list(rng)


def load_s2_median(
    catalog: StacClient,
    settings: Settings,
    geom_wgs84,
    rng: tuple[str, str] | Sequence[tuple[str, str]],
    target_crs: str,
    use_scl: bool = True,
    max_attempts: int = 5,
    item_cache: StacItemCache | None = None,
) -> xr.Dataset | None:
    ranges = _normalize_ranges(rng)
    if item_cache is not None:
        items, found_by_range = item_cache.search_items(geom_wgs84, ranges)
    else:
        items = []
        found_by_range = []
        for current_range in ranges:
            search = catalog.search(
                collections=["sentinel-2-l2a"],
                bbox=geom_wgs84.bounds,
                datetime=current_range,
                query={"eo:cloud_cover": {"lt": settings.cloud_cover_lt}},
            )
            current_items = list(search.items())
            found_by_range.append(f"{current_range[0]}->{current_range[1]}:{len(current_items)}")
            items.extend(current_items)

    if not items:
        LOGGER.warning("Nessuna scena trovata per %s", ", ".join(found_by_range) or ranges)
        return None

    found_items = len(items)
    tile_area = geom_wgs84.area or 1.0

    def footprint_fraction(item) -> float:
        footprint = _item_footprint(item)
        return 1.0 if footprint is None else footprint.intersection(geom_wgs84).area / tile_area

    def sort_key(item):
        cloud = item.properties.get("eo:cloud_cover", 100)
        return (int(cloud // CLOUD_BUCKET_PCT), -round(footprint_fraction(item), 2), cloud)

    items.sort(key=sort_key)
    candidate_counts = [settings.max_items]
    if use_scl and settings.initial_max_items < settings.max_items:
        candidate_counts.insert(0, settings.initial_max_items)
    item_counts = []
    for candidate_count in candidate_counts:
        item_count = min(candidate_count, found_items)
        if item_count > 0 and item_count not in item_counts:
            item_counts.append(item_count)

    last_low_ratio = None
    for item_count in item_counts:
        selected_items, footprint_coverage = select_items_for_coverage(items, geom_wgs84, item_count)
        LOGGER.info(
            "Scene Sentinel-2 trovate=%s usate=%s profondita=%s copertura_footprint=%.0f%% max_items=%s finestre=%s",
            found_items,
            len(selected_items),
            item_count,
            footprint_coverage * 100,
            settings.max_items,
            ", ".join(found_by_range),
        )
        signed_items = item_cache.sign_items(selected_items) if item_cache is not None else [
            planetary_computer.sign(item) for item in selected_items
        ]

        bands = ["B04", "B03", "B02", "B08"]
        if use_scl:
            bands.append("SCL")

        load_kwargs = {}
        if use_scl:
            load_kwargs["resampling"] = {"SCL": "nearest"}

        with rasterio.Env(
            GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
            CPL_VSIL_CURL_NON_CACHED="1",
            CPL_VSIL_CURL_CACHE_SIZE="67108864",
            **gdal_http_options(settings),
        ):
            ds = odc.stac.load(
                signed_items,
                bands=bands,
                dtype="float32",
                crs=target_crs,
                geopolygon=geom_wgs84,
                resolution=settings.resolution,
                chunks={"x": 2048, "y": 2048, "time": -1},
                groupby="solar_day",
                fail_on_error=False,
                skip_broken_datasets=True,
                **load_kwargs,
            )

        if getattr(ds, "time", None) is None or ds.time.size == 0:
            continue

        if not use_scl:
            return ds.median(dim="time", skipna=True)

        mask_valid = ds.SCL.isin(list(settings.valid_scl_classes))
        ratio = compute_valid_ratio(mask_valid.any(dim="time"))
        LOGGER.info("Copertura SCL con %s scene: ratio valid=%0.3f", len(selected_items), ratio)
        if ratio < settings.min_valid_ratio:
            last_low_ratio = ratio
            continue

        ds = ds.where(mask_valid).drop_vars("SCL")
        return ds.median(dim="time", skipna=True)

    if use_scl and last_low_ratio is not None:
        raise InsufficientSclCoverage(last_low_ratio)
    return None
