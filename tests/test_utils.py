from pathlib import Path

from msimne.config import Settings
from msimne.utils import get_utm_crs_from_lat_lon, monthly_windows, parse_date


def test_parse_date_month():
    dt = parse_date("2025-03")
    assert dt.year == 2025
    assert dt.month == 3


def test_monthly_windows_split():
    windows = monthly_windows(parse_date("2025-03-15"), parse_date("2025-05-01"))
    assert len(windows) == 2


def test_utm_crs_north():
    assert get_utm_crs_from_lat_lon(24.0, 45.0).startswith("EPSG:326")


def test_settings_defaults(tmp_path: Path):
    settings = Settings(project_root=tmp_path)
    assert settings.inputs_dir == tmp_path / "inputs"
    assert settings.resolve_grid_file() == tmp_path / "inputs" / "grids" / "ARAB_GRIGLIA.geojson"
