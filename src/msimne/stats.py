from __future__ import annotations

import math
import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio

from .config import Settings


LOGGER = logging.getLogger(__name__)
WEB_MERCATOR_RADIUS = 6378137.0


def mask_is_all_valid(mask) -> bool:
    if mask is False:
        return True
    if isinstance(mask, np.ndarray):
        return not mask.any()
    return False


def _web_mercator_row_area_ha(transform, rows: np.ndarray) -> np.ndarray:
    y = transform.f + (rows.astype(np.float64) + 0.5) * transform.e
    lat_rad = np.arctan(np.sinh(y / WEB_MERCATOR_RADIUS))
    projected_area_ha = abs(transform.a * transform.e - transform.b * transform.d) / 10000.0
    return projected_area_ha * np.cos(lat_rad) ** 2


def classify_stats(aoi: gpd.GeoDataFrame, ndvi_filename: str, settings: Settings, out_csv_name: str) -> None:
    ndvi_fp = settings.ndvi_dir / ndvi_filename
    LOGGER.info("Calcolo statistiche NDVI %s", ndvi_filename)
    with rasterio.open(ndvi_fp) as src:
        scale = np.float32(src.scales[0] if getattr(src, "scales", None) else 1.0)
        pixel_area_ha = abs(src.transform.a * src.transform.e - src.transform.b * src.transform.d) / 10000.0
        use_web_mercator_area = src.crs and src.crs.to_epsg() == 3857
        n = 0
        s = 0.0
        ss = 0.0
        cur_min = np.inf
        cur_max = -np.inf
        veg_pixels = 0
        area_veg = 0.0
        for _, window in src.block_windows(1):
            block = src.read(1, window=window, masked=True)
            data = block.data.astype(np.float32, copy=False) * scale
            if mask_is_all_valid(block.mask):
                vals = data.ravel()
                veg_mask = data >= settings.ndvi_threshold
            else:
                valid_mask = ~np.ma.getmaskarray(block)
                vals = data[valid_mask]
                veg_mask = valid_mask & (data >= settings.ndvi_threshold)
            if vals.size == 0:
                continue
            n += vals.size
            s += float(vals.sum())
            ss += float((vals * vals).sum())
            cur_min = min(cur_min, float(vals.min()))
            cur_max = max(cur_max, float(vals.max()))
            if use_web_mercator_area:
                veg_rows = np.nonzero(veg_mask)[0] + int(window.row_off)
                if veg_rows.size:
                    rows, counts = np.unique(veg_rows, return_counts=True)
                    area_veg += float(np.sum(counts * _web_mercator_row_area_ha(src.transform, rows)))
            else:
                veg_pixels += int(np.count_nonzero(veg_mask))
        if n == 0:
            raise ValueError(f"Nessun pixel NDVI valido in {ndvi_fp}")

    mean_ndvi = s / n
    var = max(ss / n - mean_ndvi * mean_ndvi, 0.0)
    std_ndvi = math.sqrt(var)
    if not use_web_mercator_area:
        area_veg = veg_pixels * pixel_area_ha
    pd.DataFrame(
        {
            "Min_NDVI": [cur_min],
            "Max_NDVI": [cur_max],
            "Mean_NDVI": [mean_ndvi],
            "Std_NDVI": [std_ndvi],
            "Area_vegetata_ha": [area_veg],
        }
    ).to_csv(settings.stats_dir / out_csv_name, sep=";", decimal=",", index=False)
    LOGGER.info("Statistiche NDVI create %s", out_csv_name)
