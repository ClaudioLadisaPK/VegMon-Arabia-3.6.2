from __future__ import annotations

import os
import subprocess
from pathlib import Path

import numpy as np
import rasterio
import rioxarray  # noqa: F401 - registers the .rio accessor on xarray objects
import xarray as xr

from .config import Settings


def set_gdal_env() -> None:
    os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    os.environ.setdefault("GDAL_HTTP_MAX_RETRY", "5")
    os.environ.setdefault("GDAL_HTTP_RETRY_DELAY", "1")
    os.environ.setdefault("CPL_VSIL_CURL_NON_CACHED", "1")
    os.environ.setdefault("CPL_VSIL_CURL_CACHE_SIZE", "67108864")
    os.environ.setdefault("GDAL_CACHEMAX", "32768")


def set_scale_offset(path: Path, scale: float, offset: float = 0.0) -> None:
    with rasterio.open(path, "r+") as dst:
        dst.scales = tuple([scale] * dst.count)
        dst.offsets = tuple([offset] * dst.count)


def save_stack_int16(xarr: xr.Dataset, bands: list[str], path: Path, nodata: int, crs=None) -> None:
    stack = xr.concat([xarr[b].astype("float32") for b in bands], dim="band").transpose("band", "y", "x")
    stack = stack.where(np.isfinite(stack)).clip(0, 10000)
    stack_rounded = xr.apply_ufunc(np.rint, stack, dask="parallelized", output_dtypes=[np.float32])
    stack_i16 = stack_rounded.fillna(nodata).astype(np.int16)
    if crs is not None:
        stack_i16.rio.write_crs(crs, inplace=True)
    stack_i16.rio.write_nodata(nodata, encoded=True, inplace=True)
    stack_i16.rio.to_raster(path, compress="DEFLATE", predictor=2, dtype="int16")


def convert_to_cog(path: Path, settings: Settings, nodata: int, dtype: str = "Int16", crs: str | None = None) -> None:
    temp = path.with_suffix(".temp.tif")
    with rasterio.open(path) as src:
        crs_str = crs or (src.crs.to_string() if src.crs else None)
    cmd = ["gdal_translate", "-of", "COG", "-ot", dtype]
    cmd += [
        "-co",
        "BIGTIFF=IF_SAFER",
        "-stats",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "PREDICTOR=2",
        "-co",
        f"NUM_THREADS={settings.gdal_threads}",
        "-a_nodata",
        str(nodata),
    ]
    if crs_str:
        cmd += ["-a_srs", crs_str]
    cmd += [str(path), str(temp)]
    subprocess.run(cmd, check=True, timeout=settings.gdal_timeout)
    os.replace(str(temp), str(path))
