from __future__ import annotations

import os
import subprocess
import sys
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
    # Il COG viene scritto con un nome provvisorio e rinominato solo a fine conversione:
    # un file finale presente e' quindi sempre completo (serve alla ripresa della fase finale).
    partial_fp = output_dir / f"{stem}.partial.tif"
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
        settings.final_gdal_threads,
        "-wm",
        f"{settings.gdal_warp_memory_mb}MB",
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
            "-stats",
            "-of",
            "COG",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "PREDICTOR=2",
            "-co",
            f"NUM_THREADS={settings.final_gdal_threads}",
            "-co",
            "OVERVIEWS=AUTO",
            "-co",
            "STATISTICS=YES",
            "-co",
            "BIGTIFF=IF_SAFER",
            "-a_nodata",
            str(nodata),
            "-ot",
            dtype,
            tmp_fp.as_posix(),
            partial_fp.as_posix(),
        ],
        settings,
    )
    os.replace(partial_fp, out_fp)

    for path in (list_fp, vrt_fp, tmp_fp):
        if path.exists():
            os.remove(path)
    if temp_aoi and temp_aoi.exists():
        os.remove(temp_aoi)


def run_gdal(command: list[str], settings: Settings) -> None:
    env = gdal_subprocess_env(settings)
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=settings.gdal_timeout, env=env)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"Command timed out after {settings.gdal_timeout} seconds: {' '.join(command)}"
        ) from exc
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


def gdal_subprocess_env(settings: Settings) -> dict[str, str]:
    env = os.environ.copy()
    env["GDAL_CACHEMAX"] = str(settings.final_gdal_cache_mb)
    prefix = Path(sys.prefix)
    candidates = {
        "GDAL_DATA": (prefix / "share" / "gdal", prefix / "Library" / "share" / "gdal"),
        "PROJ_DATA": (prefix / "share" / "proj", prefix / "Library" / "share" / "proj"),
        "PROJ_LIB": (prefix / "share" / "proj", prefix / "Library" / "share" / "proj"),
    }
    for name, paths in candidates.items():
        if env.get(name):
            continue
        for path in paths:
            if path.exists():
                env[name] = str(path)
                break
    return env
