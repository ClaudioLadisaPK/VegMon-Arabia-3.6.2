from __future__ import annotations

import logging

import numpy as np
import odc.stac
import planetary_computer
import rasterio
import xarray as xr
from pystac_client import Client as StacClient

from .config import Settings
from .utils import retry

LOGGER = logging.getLogger(__name__)


def open_catalog(settings: Settings) -> StacClient:
    return StacClient.open(settings.stac_url)


def compute_valid_ratio(mask_bool: xr.DataArray) -> float:
    return float(mask_bool.mean().compute().item())


@retry(6, 30)
def load_s2_median(
    catalog: StacClient,
    settings: Settings,
    geom_wgs84,
    rng: tuple[str, str],
    target_crs: str,
    use_scl: bool = True,
    max_attempts: int = 5,
) -> xr.Dataset | None:
    import random

    last_ds = None
    for attempt in range(1, max_attempts + 1):
        search = catalog.search(
            collections=["sentinel-2-l2a"],
            bbox=geom_wgs84.bounds,
            datetime=rng,
            query={"eo:cloud_cover": {"lt": settings.cloud_cover_lt}},
        )
        items = list(search.items())
        if not items:
            LOGGER.warning("Nessuna scena trovata per %s", rng)
            return None

        found_items = len(items)
        items.sort(key=lambda item: item.properties.get("eo:cloud_cover", 100))
        items = items[: settings.max_items]
        LOGGER.info("Scene Sentinel-2 trovate=%s usate=%s max_items=%s", found_items, len(items), settings.max_items)
        items = [planetary_computer.sign(item) for item in items]
        random.shuffle(items)

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
                items,
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
            last_ds = ds
            continue

        if not use_scl:
            return ds.median(dim="time", skipna=True)

        mask_valid = ds.SCL.isin(list(settings.valid_scl_classes))
        ratio = compute_valid_ratio(mask_valid.any(dim="time"))
        LOGGER.info("Tentativo %s/%s ratio valid=%0.3f", attempt, max_attempts, ratio)
        if ratio < settings.min_valid_ratio:
            last_ds = ds
            continue

        ds = ds.where(mask_valid).drop_vars("SCL")
        return ds.median(dim="time", skipna=True)

    LOGGER.warning("Copertura sotto soglia dopo %s tentativi", max_attempts)
    if last_ds is None:
        return None
    if use_scl and "SCL" in last_ds:
        mask_valid = last_ds.SCL.isin(list(settings.valid_scl_classes))
        return last_ds.where(mask_valid).drop_vars("SCL").median(dim="time", skipna=True)
    return last_ds.median(dim="time", skipna=True)
