from pathlib import Path
from types import SimpleNamespace

import numpy as np
import rasterio
import shapely
from rasterio.transform import from_origin
from shapely.geometry import mapping

from msimne.composite import TilePaths, fill_holes_from_fallback
from msimne.stac import select_items_for_coverage

TILE = shapely.box(0, 0, 1, 1)


def _item(name, geom, cloud):
    return SimpleNamespace(id=name, geometry=mapping(geom), properties={"eo:cloud_cover": cloud})


def test_partial_scenes_do_not_take_all_slots():
    # 10 scene "a striscia" con 0% nuvole che coprono solo il 5% est, poi scene complete un po' piu' nuvolose
    strips = [_item(f"strip{i}", shapely.box(0.95, 0, 1.2, 1), 0.0) for i in range(10)]
    full = [_item(f"full{i}", shapely.box(-0.1, -0.1, 1.1, 1.1), 1.0 + i) for i in range(10)]
    items = strips + full
    old_choice = items[:6]  # comportamento precedente: solo per nuvolosita'
    assert all(i.id.startswith("strip") for i in old_choice)
    selected, coverage = select_items_for_coverage(items, TILE, 6)
    assert coverage == 1.0
    assert sum(i.id.startswith("full") for i in selected) == 6
    assert sum(i.id.startswith("strip") for i in selected) <= 6


def test_full_scenes_selected_in_cloud_order():
    full = [_item(f"full{i}", shapely.box(-1, -1, 2, 2), float(i)) for i in range(10)]
    selected, coverage = select_items_for_coverage(full, TILE, 6)
    assert [i.id for i in selected] == [f"full{i}" for i in range(6)]
    assert coverage == 1.0


def test_two_halves_cover_tile():
    west = [_item(f"w{i}", shapely.box(-1, -1, 0.5, 2), 0.0) for i in range(8)]
    east = [_item(f"e{i}", shapely.box(0.5, -1, 2, 2), 5.0) for i in range(8)]
    selected, coverage = select_items_for_coverage(west + east, TILE, 3)
    assert coverage == 1.0
    assert sum(i.id.startswith("w") for i in selected) == 3
    assert sum(i.id.startswith("e") for i in selected) == 3


def _write(path: Path, data: np.ndarray, scale=None):
    with rasterio.open(
        path, "w", driver="GTiff", width=data.shape[2], height=data.shape[1], count=data.shape[0],
        dtype="int16", crs="EPSG:3857", transform=from_origin(0, 100, 10, 10), nodata=-32768,
    ) as dst:
        dst.write(data)
        if scale:
            dst.scales = (scale,) * data.shape[0]


def test_fill_holes_keeps_current_pixels(tmp_path: Path):
    nd = -32768
    cur_ndvi = np.full((1, 10, 10), 5000, dtype=np.int16)
    cur_ndvi[0, :, :3] = nd  # buco a ovest
    fb_ndvi = np.full((1, 10, 10), 1000, dtype=np.int16)
    cur_stack = np.where(cur_ndvi == nd, nd, 3000).repeat(4, axis=0).astype(np.int16)
    fb_stack = np.full((4, 10, 10), 2000, dtype=np.int16)
    paths = TilePaths(stack=tmp_path / "stack.tif", ndvi=tmp_path / "ndvi.tif")
    fb = TilePaths(stack=tmp_path / "stack.fallback.tif", ndvi=tmp_path / "ndvi.fallback.tif")
    _write(paths.ndvi, cur_ndvi, scale=1e-4)
    _write(paths.stack, cur_stack)
    _write(fb.ndvi, fb_ndvi, scale=1e-4)
    _write(fb.stack, fb_stack)
    assert fill_holes_from_fallback(paths, fb, nd) == 1.0
    with rasterio.open(paths.ndvi) as src:
        out = src.read(1)
        assert src.scales[0] == 1e-4
    assert (out[:, 3:] == 5000).all()  # mese corrente intatto
    assert (out[:, :3] == 1000).all()  # buco riempito
    with rasterio.open(paths.stack) as src:
        stack = src.read()
    assert (stack[:, :, 3:] == 3000).all() and (stack[:, :, :3] == 2000).all()
