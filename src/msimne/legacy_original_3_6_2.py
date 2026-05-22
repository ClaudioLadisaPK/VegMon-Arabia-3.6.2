# Import librerie di sistema e utility
import os
import time
import subprocess
from pathlib import Path
from glob import glob
import datetime
from datetime import date

from dateutil.relativedelta import relativedelta

# Dask per calcolo parallelo
import dask
from dask.distributed import Client

# Librerie geospaziali
import geopandas as gpd
import xarray as xr
import rioxarray
import numpy as np
import odc.stac
from pystac_client import Client as StacClient
import planetary_computer

# Elaborazione raster
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import rasterio.mask
from rasterio.features import geometry_mask   # <--- NUOVO per la copertura celle
from rasterio.vrt import WarpedVRT

# Gestione dati tabellari e pulizia directory
import pandas as pd
import shutil
from shapely.geometry import Polygon
from pyproj import Transformer

# --- Robustezza HTTP per GDAL/Rasterio su COG ---
os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
os.environ.setdefault("GDAL_HTTP_MAX_RETRY", "5")
os.environ.setdefault("GDAL_HTTP_RETRY_DELAY", "1")
os.environ.setdefault("CPL_VSIL_CURL_NON_CACHED", "1")
os.environ.setdefault("CPL_VSIL_CURL_CACHE_SIZE", "67108864")  # 64 MB

# ----------------------------------------
# File di configurazione e costanti
# ----------------------------------------
FINAL_MOSAIC_CRS = 'EPSG:3857'
AOI_DIR = Path('.')
GRID_FILE = 'ARAB_GRIGLIA.geojson'

# nuove cartelle
WORK_DIR  = Path('WORKING_S2')
STACK_DIR = Path('S2') / 'STACK'
NDVI_DIR  = Path('S2') / 'NDVI'
STATS_DIR = Path('S2') / 'STATS'

# creazione solo se mancanti
for _d in (WORK_DIR, STACK_DIR, NDVI_DIR, STATS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

NDVI_NODATA = -32768
GDAL_TIMEOUT = 1800  # 30 minuti

# ----------------------------------------
# Funzioni di utilità
# ----------------------------------------
VALID_CODES = {f"R{i:02d}" for i in range(1, 15)}

def get_utm_crs_from_lat_lon(lat: float, lon: float) -> str:
    zone = int((lon + 180) / 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone
    return f"EPSG:{epsg}"

def get_tile_id(geom) -> str:
    x_min, y_min, _, _ = geom.bounds
    lat = y_min + 0.2
    lat_prefix = 'N' if lat >= 0 else 'S'
    lon_prefix = 'E' if x_min >= 0 else 'W'
    lat_str = f"{lat_prefix}{abs(lat):06.2f}".replace('.', '_')
    lon_str = f"{lon_prefix}{abs(x_min):06.2f}".replace('.', '_')
    return f"{lat_str}-{lon_str}"

def retry(times: int, delay_seconds: int):
    import random
    def deco(func):
        def wrapper(*args, **kwargs):
            last_err = None
            for i in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_err = e
                    wait = delay_seconds * (2 ** i) + random.uniform(0, 1.5)
                    print(f"[Tentativo {i+1}/{times}] Errore in {func.__name__}: {e} → retry tra {wait:.1f}s")
                    if i < times - 1:
                        time.sleep(wait)
            print(f"🚫 {func.__name__} fallito dopo {times} tentativi: {last_err}")
            return None
        return wrapper
    return deco

def set_scale_offset(path: Path, scale: float, offset: float = 0.0):
    with rasterio.open(path, "r+") as dst:
        dst.scales  = tuple([scale]  * dst.count)
        dst.offsets = tuple([offset] * dst.count)

def save_stack_int16(xarr: xr.Dataset, bands, path: Path, nodata: int = -32768, crs=None):
    stack = xr.concat([xarr[b].astype('float32') for b in bands], dim='band').transpose('band', 'y', 'x')
    stack = stack.where(np.isfinite(stack)).clip(0, 10000)
    stack_rounded = xr.apply_ufunc(np.rint, stack, dask="parallelized", output_dtypes=[np.float32])
    stack_i16 = stack_rounded.fillna(nodata).astype(np.int16)
    if crs is not None:
        stack_i16.rio.write_crs(crs, inplace=True)
    stack_i16.rio.write_nodata(nodata, encoded=True, inplace=True)
    stack_i16.rio.to_raster(path, compress='DEFLATE', predictor=2, dtype='int16')

@retry(3, 60)
def convert_to_cog(path: Path, nodata: int = -32768, dtype: str | None = "Int16", crs: str | None = None):
    temp = path.with_suffix('.temp.tif')
    with rasterio.open(path) as src:
        crs_str = crs or (src.crs.to_string() if src.crs else None)
    cmd = ['gdal_translate', '-of', 'COG']
    if dtype is not None:
        cmd += ['-ot', dtype]
    cmd += [
        '-co', 'BIGTIFF=IF_SAFER', '-stats',
        '-co', 'COMPRESS=DEFLATE', '-co', 'PREDICTOR=2',
        '-co', 'NUM_THREADS=ALL_CPUS',
        '-a_nodata', str(nodata)
    ]
    if crs_str:
        cmd += ['-a_srs', crs_str]
    cmd += [str(path), str(temp)]
    subprocess.run(cmd, check=True, timeout=GDAL_TIMEOUT)
    os.replace(str(temp), str(path))

def clip_to_aoi_array(fp: Path, aoi: gpd.GeoDataFrame) -> np.ma.MaskedArray:
    with rasterio.open(fp) as src:
        data, _ = rasterio.mask.mask(
            src, aoi.geometry,
            crop=True,
            nodata=src.nodata,
            filled=False
        )
    return data[0]

# ----------------------------------------
# Compositing mensile Sentinel-2
# ----------------------------------------
def q16_signed(a, scale=10000.0, nodata=-999):
    out = np.clip(a, -1.0, 1.0) * scale
    out = np.rint(out)
    out[~np.isfinite(a)] = nodata
    return out.astype(np.int16)

def _compute_valid_ratio_mask(mask_bool: xr.DataArray) -> float:
    ratio = mask_bool.mean().compute().item()
    return float(ratio)

@retry(6, 30)
def _load_s2_stack_with_coverage_retry(
    catalog: StacClient,
    geom_wgs84, rng, target_crs,
    min_valid_ratio: float = 0.95,
    max_attempts: int = 5,
    max_items: int = 20
) -> xr.Dataset | None:
    import random as _random
    last_ds = None
    for att in range(1, max_attempts + 1):
        search = catalog.search(
            collections=['sentinel-2-l2a'],
            bbox=geom_wgs84.bounds,
            datetime=rng,
            query={'eo:cloud_cover': {'lt': 80}}
        )
        items = list(search.items())
        if not items:
            print(f"[try {att}/{max_attempts}] Nessuna scena trovata.")
            return None
        items.sort(key=lambda it: it.properties.get('eo:cloud_cover', 100))
        items = items[:max_items]
        items = [planetary_computer.sign(it) for it in items]
        _random.shuffle(items)
        with rasterio.Env(
            GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
            GDAL_HTTP_MAX_RETRY="5",
            GDAL_HTTP_RETRY_DELAY="1",
            CPL_VSIL_CURL_NON_CACHED="1",
            CPL_VSIL_CURL_CACHE_SIZE="67108864"
        ):
            ds = odc.stac.load(
                items,
                bands=['B04', 'B03', 'B02', 'B08', 'SCL'],
                dtype='float32',
                crs=target_crs,
                geopolygon=geom_wgs84,
                resolution=10,
                resampling={'SCL': 'nearest'},
                chunks={'x': 2048, 'y': 2048, 'time': -1},
                groupby='solar_day',
                fail_on_error=False,
                skip_broken_datasets=True
            )
        if getattr(ds, "time", None) is None or ds.time.size == 0:
            print(f"[try {att}/{max_attempts}] Load riuscito ma nessun frame valido.")
            last_ds = ds
            continue
        mask_valid = ds.SCL.isin([2, 4, 5, 6, 7])
        ratio_fast = _compute_valid_ratio_mask(mask_valid.any(dim='time'))
        print(f"[try {att}/{max_attempts}] ratio_scl_any={ratio_fast:.3f} (target {min_valid_ratio:.3f})")
        if ratio_fast < min_valid_ratio:
            last_ds = ds
            time.sleep(1.0 + _random.random() * 2.0)
            continue
        ds = ds.where(mask_valid).drop_vars('SCL')
        med = ds.median(dim='time', skipna=True)
        return med
    print(f"⚠️ Copertura < soglia dopo {max_attempts} tentativi. Proseguo con ultimo dataset.")
    return last_ds

# --- NUOVO: loader senza SCL, usato solo per riempire i buchi
@retry(6, 30)
def _load_s2_stack_no_scl(
    catalog: StacClient,
    geom_wgs84, rng, target_crs,
    max_attempts: int = 5,
    max_items: int = 20
) -> xr.Dataset | None:
    # Carica stack Sentinel-2 SENZA applicare la maschera SCL.
    import random as _random

    med_last = None

    for att in range(1, max_attempts + 1):
        search = catalog.search(
            collections=['sentinel-2-l2a'],
            bbox=geom_wgs84.bounds,
            datetime=rng,
            query={'eo:cloud_cover': {'lt': 80}}
        )
        items = list(search.items())
        if not items:
            print(f"[noSCL try {att}/{max_attempts}] Nessuna scena trovata.")
            return None

        items.sort(key=lambda it: it.properties.get('eo:cloud_cover', 100))
        items = items[:max_items]
        items = [planetary_computer.sign(it) for it in items]
        _random.shuffle(items)

        with rasterio.Env(
            GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
            GDAL_HTTP_MAX_RETRY="5",
            GDAL_HTTP_RETRY_DELAY="1",
            CPL_VSIL_CURL_NON_CACHED="1",
            CPL_VSIL_CURL_CACHE_SIZE="67108864"
        ):
            ds = odc.stac.load(
                items,
                bands=['B04', 'B03', 'B02', 'B08'],
                dtype='float32',
                crs=target_crs,
                geopolygon=geom_wgs84,
                resolution=10,
                chunks={'x': 2048, 'y': 2048, 'time': -1},
                groupby='solar_day',
                fail_on_error=False,
                skip_broken_datasets=True
            )

        if getattr(ds, 'time', None) is None or ds.time.size == 0:
            print(f"[noSCL try {att}/{max_attempts}] Load riuscito ma nessun frame valido.")
            continue

        med = ds.median(dim='time', skipna=True)
        med_last = med

        try:
            ratio = float(np.isfinite(med['B04']).mean().compute().item())
            print(f"[noSCL try {att}/{max_attempts}] valid_ratio_med={ratio:.3f}")
        except Exception:
            pass

        return med

    print("⚠️ _load_s2_stack_no_scl: nessun dataset valido dopo i tentativi.")
    return med_last

def _validate_raster_coverage(path: Path, nodata_val: int, min_valid_ratio: float) -> bool:
    try:
        with rasterio.open(path) as src:
            scale = max(int(max(src.width, src.height) / 2000), 1)
            out_h = max(src.height // scale, 1)
            out_w = max(src.width // scale, 1)
            arr = src.read(1, out_shape=(out_h, out_w), resampling=Resampling.nearest)
            ratio = float(np.count_nonzero(arr != nodata_val)) / arr.size
            print(f"[check] {path.name} valid_ratio(quick)={ratio:.3f} (target {min_valid_ratio:.3f})")
            return ratio >= min_valid_ratio
    except Exception as e:
        print(f"[check] errore validazione {path}: {e}")
        return False

@retry(6, 30)
def compute_sentinel2_composite(geom, tile_id: str, start: date, end: date):
    print(f"📦 Tessera {tile_id}: {start} - {end}")

    # out_dir di lavoro per-tile
    out_dir = WORK_DIR / tile_id
    out_dir.mkdir(parents=True, exist_ok=True)

    base = f"{start:%Y%m%d}_{end:%Y%m%d}"
    paths = {
        'stack': out_dir / f"stack_{base}.tif",
        'ndvi':  out_dir / f"ndvi_{base}.tif",
        'bin':   out_dir / f"ndvi_bin_{base}.tif"  # temporaneo
    }

    if all(p.exists() for p in paths.values()):
        print(f"⏩ Tessera {tile_id} già elaborata per {base}. Salto...")
        return

    rng = (start.isoformat(), end.isoformat())
    geom_wgs84 = gpd.GeoSeries([geom], crs=FINAL_MOSAIC_CRS).to_crs('EPSG:4326').iloc[0]
    target_crs = FINAL_MOSAIC_CRS

    catalog = StacClient.open(
        'https://planetarycomputer.microsoft.com/api/stac/v1',
        modifier=planetary_computer.sign_inplace
    )

    med = _load_s2_stack_with_coverage_retry(
        catalog=catalog,
        geom_wgs84=geom_wgs84,
        rng=rng,
        target_crs=target_crs,
        min_valid_ratio=0.95,
        max_attempts=5,
        max_items=20
    )
    if med is None:
        print(f"❌ Nessun dataset valido per {tile_id} nel periodo {base}.")
        return

    med = med.persist()

    try:
        crs_to_use = med.rio.crs
        crs_str = rasterio.crs.CRS.from_user_input(crs_to_use).to_string()
    except Exception:
        crs_to_use = target_crs
        crs_str = rasterio.crs.CRS.from_user_input(target_crs).to_string()

    # STACK (per-tile, temporaneo)
    save_stack_int16(med, ['B04', 'B03', 'B02', 'B08'], paths['stack'], nodata=-32768, crs=crs_to_use)
    convert_to_cog(paths['stack'], nodata=-32768, dtype="Int16", crs=crs_str)

    # NDVI (per-tile, temporaneo)
    red = med['B04'].astype('float32')
    nir = med['B08'].astype('float32')
    den = nir + red
    ndvi = xr.where(den > 0, (nir - red) / den, np.nan).clip(-1.0, 1.0)

    ndvi_q = xr.apply_ufunc(
        q16_signed, ndvi,
        dask="parallelized",
        kwargs=dict(scale=10000.0, nodata=NDVI_NODATA),
        output_dtypes=[np.int16]
    )
    ndvi_q.rio.write_nodata(NDVI_NODATA, encoded=True, inplace=True)
    ndvi_q.rio.write_crs(crs_to_use, inplace=True)
    ndvi_q.rio.to_raster(paths['ndvi'], compress='DEFLATE', predictor=2, dtype='int16')
    set_scale_offset(paths['ndvi'], scale=1/10000.0, offset=0.0)
    convert_to_cog(paths['ndvi'], nodata=NDVI_NODATA, dtype="Int16", crs=crs_str)

    # Post-check rapido + eventuale re-run
    if not _validate_raster_coverage(paths['ndvi'], NDVI_NODATA, min_valid_ratio=0.95):
        print(f"🔁 Copertura NDVI insufficiente in {paths['ndvi'].name}. Ritento una volta la generazione del tile...")
        try:
            for p in paths.values():
                if p.exists():
                    p.unlink()
        except Exception:
            pass

        med2 = _load_s2_stack_with_coverage_retry(
            catalog=catalog,
            geom_wgs84=geom_wgs84,
            rng=rng,
            target_crs=target_crs,
            min_valid_ratio=0.95,
            max_attempts=5,
            max_items=20
        )
        if med2 is None:
            print(f"❌ Secondo tentativo fallito per {tile_id} nel periodo {base}.")
            return

        med2 = med2.persist()

        try:
            crs_to_use2 = med2.rio.crs
            crs_str2 = rasterio.crs.CRS.from_user_input(crs_to_use2).to_string()
        except Exception:
            crs_to_use2 = target_crs
            crs_str2 = rasterio.crs.CRS.from_user_input(target_crs).to_string()

        save_stack_int16(med2, ['B04', 'B03', 'B02', 'B08'], paths['stack'], nodata=-32768, crs=crs_to_use2)
        convert_to_cog(paths['stack'], nodata=-32768, dtype="Int16", crs=crs_str2)

        red2 = med2['B04'].astype('float32')
        nir2 = med2['B08'].astype('float32')
        den2 = nir2 + red2
        ndvi2 = xr.where(den2 > 0, (nir2 - red2) / den2, np.nan).clip(-1.0, 1.0)

        ndvi_q2 = xr.apply_ufunc(
            q16_signed, ndvi2,
            dask="parallelized",
            kwargs=dict(scale=10000.0, nodata=NDVI_NODATA),
            output_dtypes=[np.int16]
        )
        ndvi_q2.rio.write_nodata(NDVI_NODATA, encoded=True, inplace=True)
        ndvi_q2.rio.write_crs(crs_to_use2, inplace=True)
        ndvi_q2.rio.to_raster(paths['ndvi'], compress='DEFLATE', predictor=2, dtype='int16')
        set_scale_offset(paths['ndvi'], scale=1/10000.0, offset=0.0)
        convert_to_cog(paths['ndvi'], nodata=NDVI_NODATA, dtype="Int16", crs=crs_str2)

        if not _validate_raster_coverage(paths['ndvi'], NDVI_NODATA, min_valid_ratio=0.95):
            print(f"⚠️ Copertura NDVI ancora insufficiente dopo il re-run su {tile_id}. Continuo ma segnalo il tile.")
        else:
            print(f"✅ Copertura NDVI ok dopo re-run: {paths['ndvi'].name}")

    # BIN per-tile (solo temporaneo per eventuali debug; non verrà mosaicato/esportato)
    veg = xr.where(np.isfinite(ndvi), xr.where(ndvi >= 0.2, 1, 0), NDVI_NODATA).astype(np.int16)
    veg.rio.write_nodata(NDVI_NODATA, encoded=True, inplace=True)
    veg.rio.write_crs(crs_to_use, inplace=True)
    veg.rio.to_raster(paths['bin'], compress='DEFLATE', predictor=2, dtype='int16')
    convert_to_cog(paths['bin'], nodata=NDVI_NODATA, dtype="Int16", crs=crs_str)

# --- NUOVO: versione senza SCL usata nel riempimento buchi -------------------
@retry(6, 30)
def compute_sentinel2_composite_no_scl(geom, tile_id: str, start: date, end: date):
    # Versione alternativa che NON applica le maschere SCL, usata per riempire i buchi.
    print(f"📦 Tessera {tile_id}: {start} - {end} (no SCL)")

    out_dir = WORK_DIR / tile_id
    out_dir.mkdir(parents=True, exist_ok=True)

    base = f"{start:%Y%m%d}_{end:%Y%m%d}"
    paths = {
        'stack': out_dir / f"stack_{base}.tif",
        'ndvi':  out_dir / f"ndvi_{base}.tif",
        'bin':   out_dir / f"ndvi_bin_{base}.tif",
    }

    rng = (start.isoformat(), end.isoformat())
    geom_wgs84 = gpd.GeoSeries([geom], crs=FINAL_MOSAIC_CRS).to_crs('EPSG:4326').iloc[0]
    target_crs = FINAL_MOSAIC_CRS

    catalog = StacClient.open(
        'https://planetarycomputer.microsoft.com/api/stac/v1',
        modifier=planetary_computer.sign_inplace
    )

    med = _load_s2_stack_no_scl(
        catalog=catalog,
        geom_wgs84=geom_wgs84,
        rng=rng,
        target_crs=target_crs,
        max_attempts=5,
        max_items=20,
    )
    if med is None:
        print(f"❌ Nessun dataset valido (no SCL) per {tile_id} nel periodo {base}.")
        return

    med = med.persist()

    try:
        crs_to_use = med.rio.crs
        crs_str = rasterio.crs.CRS.from_user_input(crs_to_use).to_string()
    except Exception:
        crs_to_use = target_crs
        crs_str = rasterio.crs.CRS.from_user_input(target_crs).to_string()

    save_stack_int16(med, ['B04', 'B03', 'B02', 'B08'], paths['stack'], nodata=-32768, crs=crs_to_use)
    convert_to_cog(paths['stack'], nodata=-32768, dtype="Int16", crs=crs_str)

    red = med['B04'].astype('float32')
    nir = med['B08'].astype('float32')
    den = nir + red
    ndvi = xr.where(den > 0, (nir - red) / den, np.nan).clip(-1.0, 1.0)

    ndvi_q = xr.apply_ufunc(
        q16_signed, ndvi,
        dask="parallelized",
        kwargs=dict(scale=10000.0, nodata=NDVI_NODATA),
        output_dtypes=[np.int16]
    )
    ndvi_q.rio.write_nodata(NDVI_NODATA, encoded=True, inplace=True)
    ndvi_q.rio.write_crs(crs_to_use, inplace=True)
    ndvi_q.rio.to_raster(paths['ndvi'], compress='DEFLATE', predictor=2, dtype='int16')
    set_scale_offset(paths['ndvi'], scale=1/10000.0, offset=0.0)
    convert_to_cog(paths['ndvi'], nodata=NDVI_NODATA, dtype="Int16", crs=crs_str)

    veg = xr.where(np.isfinite(ndvi), xr.where(ndvi >= 0.2, 1, 0), NDVI_NODATA).astype(np.int16)
    veg.rio.write_nodata(NDVI_NODATA, encoded=True, inplace=True)
    veg.rio.write_crs(crs_to_use, inplace=True)
    veg.rio.to_raster(paths['bin'], compress='DEFLATE', predictor=2, dtype='int16')
    convert_to_cog(paths['bin'], nodata=NDVI_NODATA, dtype="Int16", crs=crs_str)

# --- NUOVO: funzioni per controllo copertura celle griglia -------------------
def _tile_coverage_in_aoi(ndvi_fp: Path, tile_geom, aoi_union, nodata_val: int) -> float:
    # Calcola la frazione di pixel validi (!= nodata_val) all'interno
    # dell'intersezione tra la cella di griglia e l'AOI.
    inter = tile_geom.intersection(aoi_union)
    if inter.is_empty:
        # la cella non cade dentro l'AOI → non è un problema di buco
        return 1.0

    try:
        with rasterio.open(ndvi_fp) as src:
            arr, transform = rasterio.mask.mask(
                src,
                [inter],
                crop=True,
                filled=True,
                nodata=nodata_val,
            )
    except Exception as e:
        print(f"⚠️ Errore nel calcolo copertura per {ndvi_fp}: {e}")
        return 0.0

    data = arr[0]

    inside = geometry_mask(
        [inter],
        transform=transform,
        invert=True,
        out_shape=data.shape,
    )

    total = int(inside.sum())
    if total == 0:
        return 0.0

    valid = (data != nodata_val) & inside
    ratio = float(valid.sum()) / float(total)
    return ratio

def ensure_full_grid_coverage(
    tiles_gdf: gpd.GeoDataFrame,
    aoi: gpd.GeoDataFrame,
    wstart: datetime.datetime,
    wend: datetime.datetime,
    coverage_threshold: float = 0.99,
    max_loops: int = 5,
):
    # Prima del mosaico, controlla tutte le celle della griglia che intersecano l'AOI.
    # Ogni cella deve avere copertura NDVI >= coverage_threshold (dentro l'AOI).
    # Se una cella è sotto soglia, il suo tile viene ricalcolato SENZA maschera SCL.
    base = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"
    aoi_union = aoi.geometry.unary_union

    for loop_idx in range(1, max_loops + 1):
        missing = []

        for _, t in tiles_gdf.iterrows():
            tid = get_tile_id(t.geometry)
            ndvi_fp = WORK_DIR / tid / f"ndvi_{base}.tif"

            if not ndvi_fp.exists():
                ratio = 0.0
            else:
                ratio = _tile_coverage_in_aoi(ndvi_fp, t.geometry, aoi_union, NDVI_NODATA)

            if ratio < coverage_threshold:
                missing.append((tid, t.geometry, ratio))

        if not missing:
            print(f"✅ Tutte le celle della griglia hanno copertura ≥ {coverage_threshold:.1%} per {base}.")
            return

        print(f"🔁 Loop copertura {loop_idx}/{max_loops}: celle sotto soglia = {len(missing)}")

        if loop_idx == max_loops:
            print("⚠️ Raggiunto il numero massimo di loop; restano alcune celle con buchi:")
            for tid, _, ratio in missing:
                print(f"   - {tid}: copertura {ratio:.2%}")
            return

        for tid, geom, ratio in missing:
            print(f"   ↻ Ricalcolo tile {tid} (copertura {ratio:.2%}) senza maschera SCL...")
            tile_dir = WORK_DIR / tid
            for tag in ("stack", "ndvi", "ndvi_bin"):
                fp = tile_dir / f"{tag}_{base}.tif"
                if fp.exists():
                    try:
                        fp.unlink()
                    except Exception:
                        pass

            compute_sentinel2_composite_no_scl(geom, tid, wstart.date(), wend.date())

# ----------------------------------------
# Mosaic e statistiche
# ----------------------------------------

def reproject_list(files, dtype=None):
    tmp = []
    for f in files:
        src = rasterio.open(f)
        if src.crs.to_string() != FINAL_MOSAIC_CRS:
            out = Path(f).with_suffix('.rp.tif')
            tr, w, h = calculate_default_transform(src.crs, FINAL_MOSAIC_CRS, src.width, src.height, *src.bounds)
            prof = src.profile.copy()
            prof.update({'crs': FINAL_MOSAIC_CRS, 'transform': tr, 'width': w, 'height': h, 'compress': 'LZW'})
            if dtype:
                prof['dtype'] = dtype
            with rasterio.open(out, 'w', **prof) as dst:
                for i in range(1, src.count+1):
                    reproject(rasterio.band(src, i), rasterio.band(dst, i),
                              src_transform=src.transform, dst_transform=tr,
                              src_crs=src.crs, dst_crs=FINAL_MOSAIC_CRS,
                              resampling=Resampling.nearest)
            tmp.append(out)
            src.close()
        else:
            tmp.append(Path(f))
    return tmp

# mosaic_export come prima
def mosaic_export(
    pattern,
    output_dir: Path,
    output_name,
    dtype=None,
    nodata=-32768,
    aoi: gpd.GeoDataFrame = None,
    indexes=None,
    scale_forced: float | None = None
):
    files = glob(str(WORK_DIR / pattern))
    if not files:
        print(f"No files for pattern {pattern}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)  # crea se manca

    stem    = Path(output_name).stem
    out_fp  = (output_dir / output_name).resolve()
    tmp_fp  = (WORK_DIR / f"_tmp_{stem}.tif").resolve()
    vrt_fp  = (WORK_DIR / f"_tmp_{stem}.vrt").resolve()
    list_fp = (WORK_DIR / f"_tmp_{stem}_list.txt").resolve()

    with open(list_fp, "w", encoding="utf-8") as f:
        for fp in files:
            f.write(Path(fp).resolve().as_posix() + "\n")

    buildvrt_cmd = [
        "gdalbuildvrt",
        "-input_file_list", list_fp.as_posix(),
        "-srcnodata", str(nodata),
        "-vrtnodata", str(nodata),
        "-allow_projection_difference",
        vrt_fp.as_posix()
    ]

    warp_cmd = [
        "gdalwarp",
        "-t_srs", FINAL_MOSAIC_CRS,
        "-r", "near",
        "-srcnodata", str(nodata),
        "-dstnodata", str(nodata),
        "-multi", "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
        "-wm", "8192",
        "-co", "TILED=YES",
        "-co", "COMPRESS=DEFLATE",
        "-co", "PREDICTOR=2",
        "-co", "BIGTIFF=IF_SAFER",
        "-tap",
        "-tr", "10", "10",
        "-overwrite",
    ]
    if dtype:
        warp_cmd += ["-ot", dtype]

    temp_aoi = None
    if aoi is not None:
        _aoi_out = aoi.to_crs(FINAL_MOSAIC_CRS) if aoi.crs else aoi.set_crs(FINAL_MOSAIC_CRS)
        try:
            from shapely.validation import make_valid
            _aoi_out["geometry"] = _aoi_out.geometry.apply(make_valid)
        except Exception:
            _aoi_out["geometry"] = _aoi_out.buffer(0)
        temp_aoi = (WORK_DIR / f"_tmp_{stem}_aoi.gpkg").resolve()
        layer_name = "aoi"
        _aoi_out.to_file(temp_aoi, driver="GPKG", layer=layer_name)
        warp_cmd += ["-cutline", temp_aoi.as_posix(), "-cl", layer_name, "-crop_to_cutline"]

    warp_cmd += [vrt_fp.as_posix(), tmp_fp.as_posix()]

    translate_cmd = [
        "gdal_translate", "-of", "COG",
        "-co", "COMPRESS=DEFLATE",
        "-co", "PREDICTOR=2",
        "-co", "NUM_THREADS=ALL_CPUS",
        "-co", "BIGTIFF=IF_SAFER",
        "-a_nodata", str(nodata),
    ]
    if dtype:
        translate_cmd += ["-ot", dtype]
    translate_cmd += [tmp_fp.as_posix(), out_fp.as_posix()]

    print(f"[DEBUG] List: {list_fp.as_posix()} exists? {list_fp.exists()}  (n={len(files)})")
    try:
        subprocess.run(buildvrt_cmd, check=True, timeout=GDAL_TIMEOUT)
        subprocess.run(warp_cmd, check=True, timeout=GDAL_TIMEOUT)
        if scale_forced is not None:
            set_scale_offset(tmp_fp, scale=scale_forced, offset=0.0)
        subprocess.run(translate_cmd, check=True, timeout=GDAL_TIMEOUT)
        print(f"✅ Mosaic {out_fp.name} creato (COG) in {FINAL_MOSAIC_CRS} a 10 m → {output_dir}")
    except FileNotFoundError as e:
        raise RuntimeError("GDAL non trovato nel PATH (gdalbuildvrt/gdalwarp/gdal_translate).") from e
    finally:
        for p in [list_fp, vrt_fp, tmp_fp]:
            try:
                if p and Path(p).exists():
                    os.remove(p)
            except Exception:
                pass
        if temp_aoi and Path(temp_aoi).exists():
            try:
                os.remove(temp_aoi)
            except Exception:
                pass
            for suffix in (".gpkg-wal", ".gpkg-shm"):
                side = Path(str(temp_aoi) + suffix)
                if side.exists():
                    try:
                        os.remove(side)
                    except Exception:
                        pass

# --- classify_stats come nel tuo codice (non tocco nulla) ---
def classify_stats(aoi: gpd.GeoDataFrame, ndvi_filename: str, out_csv_name: str = 'stats.csv'):

    import math

    ndvi_fp = NDVI_DIR / ndvi_filename  # final NDVI path
    with rasterio.open(ndvi_fp) as src:
        scale = np.float32(src.scales[0] if getattr(src, "scales", None) else 1.0)

        n = 0
        s = 0.0
        ss = 0.0
        cur_min = np.inf
        cur_max = -np.inf

        for _, window in src.block_windows(1):
            block = src.read(1, window=window, masked=True)  # MaskedArray int16
            if _mask_is_all_valid(block.mask):
                vals = block.data.astype(np.float32, copy=False) * scale
            else:
                if block.count() == 0:
                    continue
                vals = block.compressed().astype(np.float32, copy=False) * scale

            if vals.size == 0:
                continue

            n += vals.size
            s += float(vals.sum())
            ss += float((vals * vals).sum())
            bmin = float(vals.min())
            bmax = float(vals.max())
            if bmin < cur_min: cur_min = bmin
            if bmax > cur_max: cur_max = bmax

        if n == 0:
            raise ValueError(f"Nessun pixel NDVI valido in {ndvi_fp}")

        mean_ndvi = s / n
        var = max(ss / n - mean_ndvi * mean_ndvi, 0.0)
        std_ndvi = math.sqrt(var)
        min_ndvi, max_ndvi = cur_min, cur_max

    centroid_wgs84 = aoi.to_crs('EPSG:4326').geometry.centroid.iloc[0]
    utm_crs = get_utm_crs_from_lat_lon(float(centroid_wgs84.y), float(centroid_wgs84.x))

    with rasterio.open(ndvi_fp) as src_ndvi:
        scale = np.float32(src_ndvi.scales[0] if getattr(src_ndvi, "scales", None) else 1.0)
        with WarpedVRT(src_ndvi, crs=utm_crs, resampling=Resampling.nearest) as vrt:
            res_x, res_y = vrt.res
            pixel_area_ha = (abs(res_x) * abs(res_y)) / 10000.0
            veg_pixels = 0
            for _, window in vrt.block_windows(1):
                blk = vrt.read(1, window=window, masked=True)  # int16 masked
                if blk.size == 0:
                    continue
                if _mask_is_all_valid(blk.mask):
                    vals = blk.data.astype(np.float32, copy=False) * scale
                    veg_pixels += int(np.sum(vals >= 0.2))
                else:
                    valid = ~blk.mask
                    if np.any(valid):
                        vals = blk.data[valid].astype(np.float32, copy=False) * scale
                        veg_pixels += int(np.sum(vals >= 0.2))
            area_veg = veg_pixels * pixel_area_ha

    df = pd.DataFrame({
        'Min_NDVI': [min_ndvi],
        'Max_NDVI': [max_ndvi],
        'Mean_NDVI': [mean_ndvi],
        'Std_NDVI': [std_ndvi],
        'Area_vegetata_ha': [area_veg]
    })
    out_csv = STATS_DIR / out_csv_name
    df.to_csv(out_csv, sep=';', decimal=',', index=False)
    print(f"📊 Statistiche salvate in {out_csv}")

def _mask_is_all_valid(m):
    if m is False:
        return True
    if isinstance(m, np.ndarray):
        return not m.any()
    try:
        return not bool(np.any(m))
    except Exception:
        return False

def _parse_date(s: str) -> datetime.datetime:
    s = s.strip()
    for fmt in ('%Y-%m-%d', '%Y-%m'):
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError("Formato data non valido. Usa YYYY-MM-DD oppure YYYY-MM.")

def _monthly_windows(start_dt: datetime.datetime, end_dt: datetime.datetime):
    wins = []
    cur_start = start_dt
    cur = start_dt.replace(day=1)
    while True:
        next_month = (cur + relativedelta(months=1)).replace(day=1)
        wstart = cur_start
        wend = min(end_dt, next_month)
        wins.append((wstart, wend))
        if wend >= end_dt:
            break
        cur_start = next_month
        cur = next_month
    return wins

def month_outputs_exist(code: str, wstart: datetime.datetime, wend: datetime.datetime, same_month: bool) -> bool:
    if same_month:
        suffix = f"{code}_{wstart:%Y-%m-%d}_{wend:%Y-%m-%d}"
    else:
        suffix = f"{code}_{wstart:%Y-%m}"
    ndvi_mosaic = NDVI_DIR / f"S2_ndvi_{suffix}.tif"
    stack_mosaic = STACK_DIR / f"S2_stack_{suffix}.tif"
    return ndvi_mosaic.exists() and stack_mosaic.exists()

REGION_NAMES = {
    "R01": "Ar Riyad", "R02": "Makkah Al Mukarramah", "R03": "Al Madinah Al Munawwarah",
    "R04": "Al Qaseem", "R05": "Eastern Region", "R06": "Aseer", "R07": "Tabuk",
    "R08": "Hail", "R09": "Northern Borders", "R10": "Jazan", "R11": "Najran",
    "R12": "Al Bahah", "R13": "Al Jawf", "R14": "PROVA"
}

def _poly_from_bounds_in_3857(src):
    crs_src = src.crs
    xmin, ymin, xmax, ymax = src.bounds
    transformer = Transformer.from_crs(crs_src, "EPSG:3857", always_xy=True)
    x0,y0 = transformer.transform(xmin, ymin)
    x1,y1 = transformer.transform(xmax, ymin)
    x2,y2 = transformer.transform(xmax, ymax)
    x3,y3 = transformer.transform(xmin, ymax)
    return Polygon([(x0,y0),(x1,y1),(x2,y2),(x3,y3),(x0,y0)])

def dump_footprints(pattern, out_gpkg):
    files = glob(str(WORK_DIR / pattern))
    rows = []
    for fp in files:
        try:
            with rasterio.open(fp) as src:
                poly = _poly_from_bounds_in_3857(src)
                rows.append({"path": Path(fp).name, "geometry": poly})
        except Exception as e:
            print("skip:", fp, e)
    if not rows:
        print("Nessun file per", pattern)
        return None
    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:3857")
    gdf.to_file(WORK_DIR / out_gpkg, driver="GPKG", layer="footprints")
    print("Footprints salvati:", WORK_DIR / out_gpkg)
    return gdf

def audit_missing_tiles(tiles_gdf: gpd.GeoDataFrame, base: str, out_gpkg: str):
    recs = []
    for _, t in tiles_gdf.iterrows():
        tid = get_tile_id(t.geometry)
        ndvi_fp = WORK_DIR / tid / f"ndvi_{base}.tif"
        recs.append({"tile_id": tid, "has_ndvi": ndvi_fp.exists(), "geometry": t.geometry})
    gdf = gpd.GeoDataFrame(recs, geometry="geometry", crs=tiles_gdf.crs)
    gdf.to_file(WORK_DIR / out_gpkg, driver="GPKG", layer="tiles")
    missing = int((~gdf["has_ndvi"]).sum())
    print("Audit tiles salvato:", WORK_DIR / out_gpkg)
    print("Tile NDVI mancanti:", missing, " / Totali:", len(gdf))
    return gdf

def reprocess_missing_ndvi(tiles_gdf, base, windows):
    missing = []
    for _, t in tiles_gdf.iterrows():
        tid = get_tile_id(t.geometry)
        if not (WORK_DIR / tid / f"ndvi_{base}.tif").exists():
            missing.append((tid, t.geometry))
    print("Tile da rielaborare:", len(missing))
    for tid, geom in missing:
        for wstart, wend in windows:
            compute_sentinel2_composite(geom, tid, wstart.date(), wend.date())

def _expected_tile_crs_from_geom(geom):
    g4326 = gpd.GeoSeries([geom], crs=FINAL_MOSAIC_CRS).to_crs("EPSG:4326").iloc[0]
    return get_utm_crs_from_lat_lon(float(g4326.centroid.y), float(g4326.centroid.x))

def _assign_crs_inplace_with_cog(path: Path, crs: str, nodata: int):
    convert_to_cog(path, nodata=nodata, dtype="Int16", crs=crs)

def _reproject_inplace(path: Path, t_srs: str, nodata: int, dtype: str = "Int16"):
    temp = path.with_suffix(".warp.tif")
    cmd = [
        "gdalwarp",
        "-t_srs", t_srs,
        "-r", "near",
        "-srcnodata", str(nodata),
        "-dstnodata", str(nodata),
        "-multi", "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
        "-wm", "8192",
        "-tap", "-tr", "10", "10",
        "-overwrite",
        str(path), str(temp)
    ]
    subprocess.run(cmd, check=True, timeout=GDAL_TIMEOUT)
    cmd2 = [
        "gdal_translate", "-of", "COG",
        "-co", "COMPRESS=DEFLATE", "-co", "PREDICTOR=2",
        "-co", "NUM_THREADS=ALL_CPUS", "-co", "BIGTIFF=IF_SAFER",
        "-a_nodata", str(nodata), "-ot", dtype,
        str(temp), str(path)
    ]
    subprocess.run(cmd2, check=True, timeout=GDAL_TIMEOUT)
    os.remove(temp)

def scan_and_fix_tile_georef(tiles_gdf: gpd.GeoDataFrame, base: str, which="ndvi"):
    patterns = {
        "ndvi":     "ndvi_{base}.tif",
        "stack":    "stack_{base}.tif",
        "ndvi_bin": "ndvi_bin_{base}.tif",
    }
    fname = patterns[which].format(base=base)
    fixed = 0
    checked = 0
    problems = []

    for _, t in tiles_gdf.iterrows():
        tid = get_tile_id(t.geometry)
        fp = WORK_DIR / tid / fname
        if not fp.exists():
            continue
        checked += 1
        try:
            with rasterio.open(fp) as src:
                crs_src = src.crs
                resx, resy = src.res
                exp_crs = _expected_tile_crs_from_geom(t.geometry)
                res_m = max(abs(resx), abs(resy))
                is_res_m10   = 8 <= res_m <= 12
                is_res_deg   = 0.00005 <= res_m <= 0.0002
                crs_str = crs_src.to_string() if crs_src else "None"

                if is_res_m10 and (crs_src is None or crs_str not in (exp_crs, rasterio.crs.CRS.from_user_input(exp_crs).to_string())):
                    print(f"➡️  {fp.name}: res≈10 m ma CRS={crs_str} (atteso {exp_crs}) → riassegno CRS")
                    _assign_crs_inplace_with_cog(fp, exp_crs, NDVI_NODATA)
                    fixed += 1
                elif is_res_deg:
                    print(f"➡️  {fp.name}: res≈gradi ({resx:.6f}) → riproietto in {exp_crs}")
                    _reproject_inplace(fp, exp_crs, NDVI_NODATA, dtype="Int16")
                    fixed += 1
                else:
                    if not is_res_m10:
                        problems.append((fp.name, crs_str, resx, resy, exp_crs))
        except Exception as e:
            print("skip:", fp.name, e)

    print(f"🔎 Controllati: {checked}  |  Corretti: {fixed}")
    if problems:
        print("⚠️ Raster con risoluzione/CRS sospetti:")
        for name, crs_str, rx, ry, exp in problems:
            print(f"  - {name}: CRS={crs_str}, res=({rx},{ry}), atteso ~10m in {exp}")

# ----------------------------------------
# MAIN
# ----------------------------------------
if __name__ == '__main__':
    start_time = time.time()

    # Dask (1)
    from multiprocessing import cpu_count
    client = Client(
        processes=True,
        n_workers=cpu_count(),
        threads_per_worker=1,
        memory_limit='auto'
    )
    dask.config.set({
        "distributed.worker.memory.target": 0.85,
        "distributed.worker.memory.spill": 0.90,
        "distributed.scheduler.worker-saturation": 1.0
    })
    print(f"🌐 Dashboard Dask: {client.dashboard_link}")
    odc.stac.configure_rio(cloud_defaults=True, client=client)

    # Info regioni
    print("📌 Codici regione disponibili:")
    for k in sorted(REGION_NAMES.keys()):
        print(f"  {k} -> {REGION_NAMES[k]}")

    # Scelta AOI
    valid = {f"R{str(i).zfill(2)}" for i in range(1, 15)}
    code = input("Seleziona regione (R01..R14): ").strip().upper()
    if code not in valid:
        raise ValueError(f"Codice '{code}' non valido. Usa uno tra: {', '.join(sorted(valid))}")

    AOI_FILE = Path(f"{code}.geojson")
    if not AOI_FILE.exists():
        AOI_FILE = Path(f"{code}.geojson")
    if not AOI_FILE.exists():
        raise FileNotFoundError(f"File AOI non trovato: {AOI_FILE}. Metti il file nella cartella di lavoro.")

    aoi = gpd.read_file(AOI_FILE)
    aoi = aoi.set_crs(FINAL_MOSAIC_CRS) if aoi.crs is None else aoi.to_crs(FINAL_MOSAIC_CRS)
    print(f"AOI: {AOI_FILE.name}")

    # Periodo
    s = input('Inizio (YYYY-MM[-DD]): ')
    e = input('Fine   (YYYY-MM[-DD]): ')
    sdt = _parse_date(s)
    edt = _parse_date(e)
    if edt <= sdt:
        raise ValueError("La data di fine deve essere successiva alla data di inizio.")

    same_month = (sdt.year == edt.year and sdt.month == edt.month)
    windows = [(sdt, edt)] if same_month else _monthly_windows(sdt, edt)

    # Carica griglia e seleziona tile
    grid = gpd.read_file(GRID_FILE)
    grid = grid.set_crs(FINAL_MOSAIC_CRS) if grid.crs is None else grid.to_crs(FINAL_MOSAIC_CRS)
    area_km2 = aoi.to_crs('EPSG:3857').geometry.area.sum() / 1e6
    print(f"Area AOI: {area_km2:.2f} km²")

    aoi_buff = gpd.GeoDataFrame(geometry=aoi.geometry.buffer(100), crs=aoi.crs)
    try:
        from shapely.validation import make_valid
        aoi_buff["geometry"] = aoi_buff.geometry.apply(make_valid)
    except Exception:
        aoi_buff["geometry"] = aoi_buff.buffer(0)

    tiles = gpd.sjoin(grid, aoi_buff, how='inner', predicate='intersects').drop(columns=['index_right'])
    print(f"Tile count: {len(tiles)}")

    # Generazione composite per-tile
    for widx, (wstart, wend) in enumerate(windows, start=1):
        if month_outputs_exist(code, wstart, wend, same_month):
            label = f"{wstart:%Y-%m-%d}→{wend:%Y-%m-%d}" if same_month else f"{wstart:%Y-%m}"
            print(f"⏭️  {code} {label} già chiuso (mosaici presenti). Skipping tiles.")
            continue

        print(f"🗓️  Finestra {widx}/{len(windows)}: {wstart:%Y-%m-%d} → {wend:%Y-%m-%d}")
        for idx, t in tiles.iterrows():
            tid = get_tile_id(t.geometry)
            print(f"  🔄 Tile {tid} ({idx + 1}/{len(tiles)})")
            compute_sentinel2_composite(t.geometry, tid, wstart.date(), wend.date())

        # 🔍 NUOVO: controllo e riempimento buchi prima del mosaico
        ensure_full_grid_coverage(
            tiles_gdf=tiles,
            aoi=aoi,
            wstart=wstart,
            wend=wend,
            coverage_threshold=0.99,
            max_loops=5,
        )

    # Chiudi Dask prima dei mosaici
    client.close()
    print("🔌 Dask client chiuso per liberare RAM prima dei mosaici.")
    print("--- Building final mosaics ---")

    # Mosaici e statistiche
    if same_month:
        suffix = f"{code}_{sdt:%Y-%m-%d}_{edt:%Y-%m-%d}"
        base   = f"{sdt:%Y%m%d}_{edt:%Y%m%d}"

        if len(glob(str(WORK_DIR / f"*/ndvi_{base}.tif"))) == 0:
            print(f"⚠️ Nessuna composite per il periodo {base}. Salto mosaici e statistiche.")
        else:
            stack_mosaic_name = f"S2_stack_{suffix}.tif"
            ndvi_mosaic_name  = f"S2_ndvi_{suffix}.tif"
            stats_name        = f"S2_stats_{suffix}.csv"

            mosaic_export(f"*/stack_{base}.tif", STACK_DIR, stack_mosaic_name, dtype='Int16', nodata=NDVI_NODATA, aoi=aoi, scale_forced=None)
            mosaic_export(f"*/ndvi_{base}.tif",  NDVI_DIR,  ndvi_mosaic_name,  dtype='Int16', nodata=NDVI_NODATA, aoi=aoi, scale_forced=1/10000.0)

            required = [(NDVI_DIR / ndvi_mosaic_name).exists(), (STACK_DIR / stack_mosaic_name).exists()]
            if all(required):
                classify_stats(aoi, ndvi_mosaic_name, out_csv_name=stats_name)
            else:
                print(f"⚠️ Mosaici mancanti per {suffix}. Salto le statistiche.")

    else:
        for wstart, wend in windows:
            month_suffix = f"{code}_{wstart:%Y-%m}"
            base         = f"{wstart:%Y%m%d}_{wend:%Y%m%d}"

            if len(glob(str(WORK_DIR / f"*/ndvi_{base}.tif"))) == 0:
                print(f"⚠️ Nessuna composite per {month_suffix} (base {base}). Salto mosaici e statistiche.")
                continue

            stack_mosaic_name = f"S2_stack_{month_suffix}.tif"
            ndvi_mosaic_name  = f"S2_ndvi_{month_suffix}.tif"
            stats_name        = f"S2_stats_{month_suffix}.csv"

            mosaic_export(f"*/stack_{base}.tif", STACK_DIR, stack_mosaic_name, dtype='Int16', nodata=NDVI_NODATA, aoi=aoi, scale_forced=None)
            mosaic_export(f"*/ndvi_{base}.tif",  NDVI_DIR,  ndvi_mosaic_name,  dtype='Int16', nodata=NDVI_NODATA, aoi=aoi, scale_forced=1/10000.0)

            required = [(NDVI_DIR / ndvi_mosaic_name).exists(), (STACK_DIR / stack_mosaic_name).exists()]
            if all(required):
                classify_stats(aoi, ndvi_mosaic_name, out_csv_name=stats_name)
            else:
                print(f"⚠️ Mosaici mancanti per {month_suffix}. Salto le statistiche.")

    # Pulizia finale SOLO della working area (per-tile + eventuali tmp rimasti)
    for d in WORK_DIR.iterdir():
        try:
            if d.is_dir():
                shutil.rmtree(d)
            else:
                d.unlink()
        except Exception:
            pass
    print("🧹 WORKING_S2 pulita.")

    elapsed = time.time() - start_time
    print(f"✅ Completed in {elapsed/60:.2f} min ({elapsed:.2f} s)")
