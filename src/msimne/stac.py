from __future__ import annotations

import logging
from collections.abc import Sequence
from threading import Lock

import numpy as np
import odc.stac
import planetary_computer
import rasterio
import xarray as xr
from pystac_client import Client as StacClient

from .config import Settings
from .utils import NonRetryableError, retry

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


def open_catalog(settings: Settings) -> StacClient:
    return StacClient.open(settings.stac_url)


def compute_valid_ratio(mask_bool: xr.DataArray) -> float:
    return float(mask_bool.mean().compute().item())


def _normalize_ranges(rng: tuple[str, str] | Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    if isinstance(rng, tuple) and len(rng) == 2 and isinstance(rng[0], str):
        return [rng]
    return list(rng)


@retry(6, 30)
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
    items.sort(key=lambda item: item.properties.get("eo:cloud_cover", 100))
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
        selected_items = items[:item_count]
        LOGGER.info(
            "Scene Sentinel-2 trovate=%s usate=%s max_items=%s finestre=%s",
            found_items,
            len(selected_items),
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
            GDAL_HTTP_MAX_RETRY="5",
            GDAL_HTTP_RETRY_DELAY="1",
            CPL_VSIL_CURL_NON_CACHED="1",
            CPL_VSIL_CURL_CACHE_SIZE="67108864",
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
