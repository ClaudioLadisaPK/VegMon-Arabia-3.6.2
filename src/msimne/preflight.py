from __future__ import annotations

import shutil

from .config import Settings


def validate_runtime(settings: Settings, region_code: str) -> None:
    missing_commands = [cmd for cmd in ("gdal_translate", "gdalwarp", "gdalbuildvrt") if shutil.which(cmd) is None]
    if missing_commands:
        raise RuntimeError(
            "Comandi GDAL mancanti nel PATH: " + ", ".join(missing_commands)
        )

    aoi_file = settings.resolve_aoi_file(region_code)
    if not aoi_file.exists():
        raise FileNotFoundError(f"AOI non trovato per {region_code}: {aoi_file}")

    grid_file = settings.resolve_grid_file()
    if not grid_file.exists():
        raise FileNotFoundError(f"Griglia input non trovata: {grid_file}")
