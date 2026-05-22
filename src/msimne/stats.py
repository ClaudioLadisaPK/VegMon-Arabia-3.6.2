from __future__ import annotations

import math

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.vrt import WarpedVRT
from rasterio.warp import Resampling

from .config import Settings
from .utils import get_utm_crs_from_lat_lon


def mask_is_all_valid(mask) -> bool:
    if mask is False:
        return True
    if isinstance(mask, np.ndarray):
        return not mask.any()
    return False


def classify_stats(aoi: gpd.GeoDataFrame, ndvi_filename: str, settings: Settings, out_csv_name: str) -> None:
    ndvi_fp = settings.ndvi_dir / ndvi_filename
    with rasterio.open(ndvi_fp) as src:
        scale = np.float32(src.scales[0] if getattr(src, "scales", None) else 1.0)
        n = 0
        s = 0.0
        ss = 0.0
        cur_min = np.inf
        cur_max = -np.inf
        for _, window in src.block_windows(1):
            block = src.read(1, window=window, masked=True)
            vals = block.data.astype(np.float32, copy=False) * scale if mask_is_all_valid(block.mask) else block.compressed().astype(np.float32, copy=False) * scale
            if vals.size == 0:
                continue
            n += vals.size
            s += float(vals.sum())
            ss += float((vals * vals).sum())
            cur_min = min(cur_min, float(vals.min()))
            cur_max = max(cur_max, float(vals.max()))
        if n == 0:
            raise ValueError(f"Nessun pixel NDVI valido in {ndvi_fp}")

    mean_ndvi = s / n
    var = max(ss / n - mean_ndvi * mean_ndvi, 0.0)
    std_ndvi = math.sqrt(var)
    centroid_wgs84 = aoi.to_crs("EPSG:4326").geometry.centroid.iloc[0]
    utm_crs = get_utm_crs_from_lat_lon(float(centroid_wgs84.y), float(centroid_wgs84.x))

    with rasterio.open(ndvi_fp) as src_ndvi:
        scale = np.float32(src_ndvi.scales[0] if getattr(src_ndvi, "scales", None) else 1.0)
        with WarpedVRT(src_ndvi, crs=utm_crs, resampling=Resampling.nearest) as vrt:
            res_x, res_y = vrt.res
            pixel_area_ha = (abs(res_x) * abs(res_y)) / 10000.0
            veg_pixels = 0
            for _, window in vrt.block_windows(1):
                block = vrt.read(1, window=window, masked=True)
                vals = block.data.astype(np.float32, copy=False) * scale if mask_is_all_valid(block.mask) else block.data[~block.mask].astype(np.float32, copy=False) * scale
                veg_pixels += int(np.sum(vals >= settings.ndvi_threshold))
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
