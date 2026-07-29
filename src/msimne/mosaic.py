from __future__ import annotations

import os
import subprocess
from glob import glob
from pathlib import Path
from typing import Iterable

import geopandas as gpd

from .config import Settings
from .io import set_scale_offset


def mosaic_export(
    pattern: str,
    output_dir: Path,
    output_name: str,
    settings: Settings,
    dtype: str = "Int16",
    nodata: int | None = None,
    aoi: gpd.GeoDataFrame | None = None,
    scale_forced: float | None = None,
    source_files: Iterable[Path] | None = None,
) -> None:
    nodata = settings.ndvi_nodata if nodata is None else nodata
    files = [str(path) for path in source_files] if source_files is not None else glob(str(settings.work_dir / pattern))
    if not files:
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(output_name).stem
    out_fp = output_dir / output_name
    tmp_fp = settings.work_dir / f"_tmp_{stem}.tif"
    vrt_fp = settings.work_dir / f"_tmp_{stem}.vrt"
    list_fp = settings.work_dir / f"_tmp_{stem}_list.txt"

    with open(list_fp, "w", encoding="utf-8") as stream:
        for fp in files:
            stream.write(Path(fp).resolve().as_posix() + "\n")

    run_gdal(
        [
            "gdalbuildvrt",
            "-input_file_list",
            list_fp.as_posix(),
            "-srcnodata",
            str(nodata),
            "-vrtnodata",
            str(nodata),
            "-allow_projection_difference",
            vrt_fp.as_posix(),
        ],
        settings,
    )

    warp_cmd = [
        "gdalwarp",
        "-t_srs",
        settings.final_mosaic_crs,
        "-r",
        "near",
        "-srcnodata",
        str(nodata),
        "-dstnodata",
        str(nodata),
        "-multi",
        "--config",
        "GDAL_NUM_THREADS",
        settings.gdal_threads,
        "-wm",
        str(settings.gdal_warp_memory_mb),
        "-co",
        "TILED=YES",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "PREDICTOR=2",
        "-co",
        "BIGTIFF=IF_SAFER",
        "-tap",
        "-tr",
        str(settings.resolution),
        str(settings.resolution),
        "-overwrite",
        "-ot",
        dtype,
    ]

    temp_aoi = None
    if aoi is not None:
        aoi_out = aoi.to_crs(settings.final_mosaic_crs) if aoi.crs else aoi.set_crs(settings.final_mosaic_crs)
        temp_aoi = settings.work_dir / f"_tmp_{stem}_aoi.gpkg"
        aoi_out.to_file(temp_aoi, driver="GPKG", layer="aoi")
        warp_cmd += ["-cutline", temp_aoi.as_posix(), "-cl", "aoi", "-crop_to_cutline"]

    warp_cmd += [vrt_fp.as_posix(), tmp_fp.as_posix()]
    run_gdal(warp_cmd, settings)

    if scale_forced is not None:
        set_scale_offset(tmp_fp, scale=scale_forced, offset=0.0)

    run_gdal(
        [
            "gdal_translate",
            "-of",
            "COG",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "PREDICTOR=2",
            "-co",
            f"NUM_THREADS={settings.gdal_threads}",
            "-co",
            "BIGTIFF=IF_SAFER",
            "-a_nodata",
            str(nodata),
            "-ot",
            dtype,
            tmp_fp.as_posix(),
            out_fp.as_posix(),
        ],
        settings,
    )

    for path in (list_fp, vrt_fp, tmp_fp):
        if path.exists():
            os.remove(path)
    if temp_aoi and temp_aoi.exists():
        os.remove(temp_aoi)


def run_gdal(command: list[str], settings: Settings) -> None:
    result = subprocess.run(command, capture_output=True, text=True, timeout=settings.gdal_timeout)
    if result.returncode == 0:
        return

    details = "\n".join(
        part.strip()
        for part in (
            f"Command: {' '.join(command)}",
            f"stdout:\n{result.stdout}" if result.stdout else "",
            f"stderr:\n{result.stderr}" if result.stderr else "",
        )
        if part.strip()
    )
    raise RuntimeError(details)
