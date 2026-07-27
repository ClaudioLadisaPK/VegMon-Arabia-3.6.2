from pathlib import Path

from msimne.config import PRODUCTION_REGIONS, Settings
from msimne.state import PipelineState, RegionRunRecord, utc_now_iso, write_region_report
from msimne.utils import get_utm_crs_from_lat_lon, monthly_windows, parse_date, previous_month_window


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


def test_previous_month_window():
    start, end = previous_month_window(parse_date("2026-07-27").date())
    assert start.strftime("%Y-%m-%d") == "2026-06-01"
    assert end.strftime("%Y-%m-%d") == "2026-07-01"


def test_production_regions_exclude_r14():
    assert PRODUCTION_REGIONS[0] == "R01"
    assert PRODUCTION_REGIONS[-1] == "R13"
    assert "R14" not in PRODUCTION_REGIONS


def test_pipeline_state_and_report(tmp_path: Path):
    state = PipelineState(tmp_path / "pipeline.sqlite")
    run_id = state.create_run("2026-06")
    record = RegionRunRecord(
        run_id=run_id,
        month="2026-06",
        region="R01",
        status="done",
        started_at=utc_now_iso(),
        finished_at=utc_now_iso(),
        elapsed_seconds=1.0,
        ndvi_output="ndvi.tif",
        stack_output="stack.tif",
        stats_output="stats.csv",
        valid_ratio_before=0.98,
        valid_ratio_after=1.0,
        interpolated_ratio=0.02,
    )
    state.upsert_region(record)
    records = state.region_records(run_id)
    assert records[0].region == "R01"
    assert records[0].status == "done"

    report = tmp_path / "report.csv"
    write_region_report(records, report)
    assert "R01" in report.read_text(encoding="utf-8")
