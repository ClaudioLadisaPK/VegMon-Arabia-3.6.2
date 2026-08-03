from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta

from .config import PRODUCTION_REGIONS, REGION_NAMES, Settings
from .io import set_gdal_env
from .logging_utils import configure_logging
from .pipeline import run_pipeline
from .preflight import validate_runtime
from .state import PipelineState, RegionRunRecord, utc_now_iso, write_region_report
from .utils import parse_date, previous_month_window


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MSIMNE Sentinel-2 NDVI pipeline")
    parser.add_argument("--project-root", type=Path, default=Path.cwd(), help="Cartella progetto")
    parser.add_argument("--region", choices=sorted(PRODUCTION_REGIONS), help="Codice regione R01..R13")
    parser.add_argument("--all-regions", action="store_true", help="Elabora tutte le regioni operative R01..R13")
    parser.add_argument("--start", help="Data inizio: YYYY-MM-DD o YYYY-MM")
    parser.add_argument("--end", help="Data fine: YYYY-MM-DD o YYYY-MM")
    parser.add_argument("--month", help="Mese da elaborare: YYYY-MM")
    parser.add_argument("--previous-month", action="store_true", help="Elabora automaticamente il mese precedente")
    parser.add_argument(
        "--next-pending-month",
        action="store_true",
        help="Elabora il primo mese incompleto nella sequenza operativa",
    )
    parser.add_argument("--from-month", default="2026-03", help="Primo mese della sequenza operativa: YYYY-MM")
    parser.add_argument("--inputs-dir", type=Path, help="Cartella inputs; default <project-root>/inputs")
    parser.add_argument("--outputs-dir", type=Path, help="Cartella outputs; default <project-root>/outputs")
    parser.add_argument("--grid-file", type=Path, help="Override del file griglia")
    parser.add_argument("--workers", type=int, default=16, help="Numero worker Dask")
    parser.add_argument("--threads-per-worker", type=int, default=2, help="Thread per worker Dask")
    parser.add_argument("--memory-limit", default="12GB", help="Limite memoria per worker Dask")
    parser.add_argument("--gdal-threads", default="16", help="Thread GDAL per warp/COG")
    parser.add_argument("--gdal-warp-memory-mb", type=int, default=16384, help="Memoria gdalwarp in MB")
    parser.add_argument("--gdal-timeout", type=int, default=14400, help="Timeout comandi GDAL in secondi")
    parser.add_argument("--max-items", type=int, default=10, help="Numero massimo scene Sentinel-2 per tile/mese")
    parser.add_argument(
        "--seasonal-fallback-coverage-threshold",
        type=float,
        default=0.98,
        help="Soglia copertura tile sotto cui usare gli stessi mesi degli anni precedenti",
    )
    parser.add_argument(
        "--seasonal-fallback-years",
        type=int,
        default=2,
        help="Numero anni precedenti da usare per il fallback stagionale",
    )
    parser.add_argument("--log-file", type=Path, help="File log esplicito")
    parser.add_argument("--interactive", action="store_true", help="Richiede regione e date in modo interattivo")
    parser.add_argument("--verbose", action="store_true", help="Abilita logging verboso")
    return parser


def prompt_if_missing(args: argparse.Namespace) -> tuple[str, str, str]:
    region = args.region
    start = args.start
    end = args.end

    if args.interactive or region is None or start is None or end is None:
        print("Codici regione disponibili:")
        for code in sorted(PRODUCTION_REGIONS):
            print(f"  {code} -> {REGION_NAMES[code]}")

    while region is None:
        value = input("Seleziona regione (R01..R13): ").strip().upper()
        if value in PRODUCTION_REGIONS:
            region = value
        else:
            print("Codice non valido.")

    while start is None:
        value = input("Inizio (YYYY-MM[-DD]): ").strip()
        if value:
            start = value

    while end is None:
        value = input("Fine   (YYYY-MM[-DD]): ").strip()
        if value:
            end = value

    return region, start, end


def resolve_window(args: argparse.Namespace, settings: Settings, regions: list[str]) -> tuple[datetime, datetime]:
    if args.next_pending_month:
        state = PipelineState(settings.state_dir / "pipeline.sqlite")
        start = state.first_incomplete_month(parse_date(args.from_month), regions)
        return start, start + relativedelta(months=1)
    if args.previous_month:
        return previous_month_window()
    if args.month:
        start = parse_date(args.month)
        return start, start + relativedelta(months=1)
    if args.start and args.end:
        return parse_date(args.start), parse_date(args.end)
    raise ValueError("Specifica --month, --previous-month, --next-pending-month oppure --start e --end.")


def default_log_file(settings: Settings, start_dt: datetime, all_regions: bool, region: str | None) -> Path:
    scope = "all_regions" if all_regions else region or "interactive"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return settings.logs_dir / f"run_{start_dt:%Y-%m}_{scope}_{timestamp}.log"


def run_regions(settings: Settings, regions: list[str], start_dt: datetime, end_dt: datetime) -> int:
    month = f"{start_dt:%Y-%m}"
    state = PipelineState(settings.state_dir / "pipeline.sqlite")
    run_id = state.create_run(month)
    logger = logging.getLogger(__name__)
    logger.info("START monthly run_id=%s month=%s regions=%s", run_id, month, ",".join(regions))
    state.log_event(run_id, month, "run", "started", f"regions={','.join(regions)}")

    failed = 0
    for region in regions:
        started_at = utc_now_iso()
        tic = time.perf_counter()
        state.log_event(run_id, month, "region", "started", region=region)
        try:
            validate_runtime(settings, region)
            results = run_pipeline(settings, region, start_dt, end_dt)
            elapsed = time.perf_counter() - tic
            result = results[-1]
            status = "done_with_interpolation" if result.quality.filled else "done"
            record = RegionRunRecord(
                run_id=run_id,
                month=month,
                region=region,
                status=status,
                started_at=started_at,
                finished_at=utc_now_iso(),
                elapsed_seconds=elapsed,
                ndvi_output=result.ndvi_output,
                stack_output=result.stack_output,
                stats_output=result.stats_output,
                valid_ratio_before=result.quality.valid_ratio_before,
                valid_ratio_after=result.quality.valid_ratio_after,
                interpolated_ratio=result.quality.interpolated_ratio,
            )
            state.upsert_region(record)
            state.log_event(run_id, month, "region", status, region=region)
            logger.info("DONE %s elapsed_seconds=%.1f status=%s", region, elapsed, status)
        except Exception as exc:
            failed += 1
            elapsed = time.perf_counter() - tic
            message = str(exc)
            status = "failed_quality" if "Copertura NDVI sotto soglia" in message else "failed"
            state.upsert_region(
                RegionRunRecord(
                    run_id=run_id,
                    month=month,
                    region=region,
                    status=status,
                    started_at=started_at,
                    finished_at=utc_now_iso(),
                    elapsed_seconds=elapsed,
                    error_message=message,
                )
            )
            state.log_event(run_id, month, "region", status, message=message, region=region)
            logger.exception("FAILED %s status=%s", region, status)

    records = state.region_records(run_id)
    report_path = settings.reports_dir / f"run_{month}_{run_id}.csv"
    write_region_report(records, report_path)
    final_status = "done" if failed == 0 else "done_with_failures"
    state.finish_run(run_id, final_status, f"failed_regions={failed}; report={report_path}")
    state.log_event(run_id, month, "run", final_status, f"report={report_path}")
    logger.info("END monthly run_id=%s status=%s report=%s", run_id, final_status, report_path)
    return 0 if failed == 0 else 2


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    set_gdal_env()
    settings = Settings(
        project_root=args.project_root.resolve(),
        inputs_dir_override=args.inputs_dir.resolve() if args.inputs_dir else None,
        outputs_dir_override=args.outputs_dir.resolve() if args.outputs_dir else None,
        grid_file_override=args.grid_file.resolve() if args.grid_file else None,
        dask_workers=args.workers,
        dask_threads_per_worker=args.threads_per_worker,
        dask_memory_limit=args.memory_limit,
        gdal_threads=args.gdal_threads,
        gdal_warp_memory_mb=args.gdal_warp_memory_mb,
        gdal_timeout=args.gdal_timeout,
        max_items=args.max_items,
        seasonal_fallback_coverage_threshold=args.seasonal_fallback_coverage_threshold,
        seasonal_fallback_years=args.seasonal_fallback_years,
    )
    settings.ensure_directories()

    has_window_arg = args.month or args.previous_month or args.next_pending_month or (args.start and args.end)
    if args.interactive or (not args.all_regions and not has_window_arg):
        configure_logging(args.verbose, args.log_file)
        region, start, end = prompt_if_missing(args)
        start_dt = parse_date(start)
        end_dt = parse_date(end)
        regions = [region]
    else:
        regions = list(PRODUCTION_REGIONS) if args.all_regions else [args.region] if args.region else []
        if not regions:
            parser.error("Specifica --region oppure --all-regions.")
        start_dt, end_dt = resolve_window(args, settings, regions)
        configure_logging(args.verbose, args.log_file or default_log_file(settings, start_dt, args.all_regions, args.region))

    if end_dt <= start_dt:
        parser.error("La data di fine deve essere successiva alla data di inizio.")

    if len(regions) == 1 and not args.all_regions:
        validate_runtime(settings, regions[0])
        logging.getLogger(__name__).info("Regione %s - %s", regions[0], REGION_NAMES[regions[0]])
        run_pipeline(settings, regions[0], start_dt, end_dt)
        return 0

    return run_regions(settings, regions, start_dt, end_dt)


if __name__ == "__main__":
    raise SystemExit(main())
