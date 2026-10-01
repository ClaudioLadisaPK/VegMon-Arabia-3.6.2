from __future__ import annotations

import argparse
import logging
import os
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


def env_path(name: str) -> Path | None:
    value = os.environ.get(name)
    return Path(value) if value else None


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value else default


# Profili macchina: valori di partenza per i parametri di performance, separati per fase.
# Fase tile: download STAC + composite (Dask). Fase mosaico: GDAL dopo la chiusura di Dask.
# Ordine di precedenza: parametro esplicito > variabile d'ambiente > profilo > default storico.
BASE_PERFORMANCE = {
    # fase tile
    "workers": 16,
    "threads_per_worker": 2,
    "memory_limit": "12GB",
    "tile_parallelism": 1,
    "tile_gdal_cache_mb": None,
    # fase mosaico
    "gdal_threads": "16",
    "final_gdal_threads": None,
    "gdal_warp_memory_mb": 16384,
    "final_gdal_cache_mb": None,
    "final_parallel_mosaics": 1,
    # default comune delle due cache GDAL se non specificate per fase
    "gdal_cache_mb": 4096,
}
PROFILES = {
    # PC locale WSL ~15 GB RAM / 8 core. La fase mosaico resta sui valori che hanno
    # completato R04 2026-07 (il crash era in COG/overview con thread e cache GDAL alti).
    "wsl": {
        # Configurazione del test T08 (R10 2026-07): fase tile 2,3x piu' veloce di 3x1/par 3,
        # picco RAM di sistema 5,1 GB, output identici. Valori tenuti identici a quelli testati.
        "workers": 4,
        "threads_per_worker": 2,
        "memory_limit": "5GB",
        "tile_parallelism": 5,
        "tile_gdal_cache_mb": 1024,
        "gdal_threads": "2",
        "final_gdal_threads": "4",
        "gdal_warp_memory_mb": 512,
        "final_gdal_cache_mb": 4096,
        "final_parallel_mosaics": 1,
    },
    # VM cliente NCVC: 72 vCPU, 240 GB RAM.
    "vm": {
        "workers": 12,
        "threads_per_worker": 2,
        "memory_limit": "12GB",
        "tile_parallelism": 8,
        "tile_gdal_cache_mb": 2048,
        "gdal_threads": "2",
        "final_gdal_threads": "16",
        "gdal_warp_memory_mb": 8192,
        "final_gdal_cache_mb": 8192,
        "final_parallel_mosaics": 2,
    },
}
PERFORMANCE_ENV = {
    "workers": ("MSIMNE_WORKERS", int),
    "threads_per_worker": ("MSIMNE_THREADS_PER_WORKER", int),
    "memory_limit": ("MSIMNE_MEMORY_LIMIT", str),
    "gdal_threads": ("MSIMNE_GDAL_THREADS", str),
    "gdal_warp_memory_mb": ("MSIMNE_GDAL_WARP_MEMORY_MB", int),
    "gdal_cache_mb": ("MSIMNE_GDAL_CACHE_MB", int),
    "tile_gdal_cache_mb": ("MSIMNE_TILE_GDAL_CACHE_MB", int),
    "final_gdal_cache_mb": ("MSIMNE_FINAL_GDAL_CACHE_MB", int),
    "tile_parallelism": ("MSIMNE_TILE_PARALLELISM", int),
    "final_gdal_threads": ("MSIMNE_FINAL_GDAL_THREADS", str),
    "final_parallel_mosaics": ("MSIMNE_FINAL_PARALLEL_MOSAICS", int),
}


def resolve_performance(args: argparse.Namespace) -> None:
    profile = PROFILES.get(args.profile or "", {})
    for name, default in BASE_PERFORMANCE.items():
        if getattr(args, name) is not None:
            continue
        env_name, cast = PERFORMANCE_ENV[name]
        env_value = os.environ.get(env_name)
        if env_value:
            setattr(args, name, cast(env_value))
        else:
            setattr(args, name, profile.get(name, default))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MSIMNE Sentinel-2 NDVI pipeline")
    parser.add_argument(
        "--project-root",
        type=Path,
        default=env_path("MSIMNE_PROJECT_ROOT") or Path.cwd(),
        help="Cartella progetto; default cwd o MSIMNE_PROJECT_ROOT",
    )
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
    parser.add_argument(
        "--inputs-dir",
        type=Path,
        default=env_path("MSIMNE_INPUTS_DIR"),
        help="Cartella inputs; default <project-root>/inputs o MSIMNE_INPUTS_DIR",
    )
    parser.add_argument(
        "--outputs-dir",
        type=Path,
        default=env_path("MSIMNE_OUTPUTS_DIR"),
        help="Cartella outputs; default <project-root>/outputs o MSIMNE_OUTPUTS_DIR",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=env_path("MSIMNE_WORK_DIR"),
        help="Cartella temporanea di lavoro; default <outputs-dir>/working_s2 o MSIMNE_WORK_DIR",
    )
    parser.add_argument(
        "--grid-file",
        type=Path,
        default=env_path("MSIMNE_GRID_FILE"),
        help="Override del file griglia o MSIMNE_GRID_FILE",
    )
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default=os.environ.get("MSIMNE_PROFILE") or None,
        help="Profilo macchina (wsl = PC locale ~15 GB, vm = VM cliente); i parametri espliciti hanno precedenza",
    )
    tile = parser.add_argument_group("Fase tile (download STAC + composite con Dask)")
    tile.add_argument("--workers", type=int, default=None, help="Numero worker Dask")
    tile.add_argument("--threads-per-worker", type=int, default=None, help="Thread per worker Dask")
    tile.add_argument("--memory-limit", default=None, help="Limite memoria per worker Dask")
    tile.add_argument(
        "--tile-parallelism",
        type=int,
        default=None,
        help="Numero tile da elaborare in parallelo",
    )
    tile.add_argument(
        "--tile-gdal-cache-mb",
        type=int,
        default=None,
        help="GDAL_CACHEMAX per ciascun worker Dask in MB; default = --gdal-cache-mb",
    )
    final = parser.add_argument_group("Fase mosaico (GDAL, dopo la chiusura di Dask)")
    final.add_argument(
        "--final-gdal-threads",
        default=None,
        help="Thread GDAL per warp/COG/overview; default = --gdal-threads",
    )
    final.add_argument(
        "--gdal-warp-memory-mb",
        "--final-warp-memory-mb",
        dest="gdal_warp_memory_mb",
        type=int,
        default=None,
        help="Memoria gdalwarp in MB",
    )
    final.add_argument(
        "--final-gdal-cache-mb",
        type=int,
        default=None,
        help="GDAL_CACHEMAX dei comandi GDAL del mosaico in MB; default = --gdal-cache-mb",
    )
    final.add_argument(
        "--final-parallel-mosaics",
        type=int,
        choices=(1, 2),
        default=None,
        help="2 = mosaici STACK e NDVI in parallelo (raddoppia la RAM del mosaico)",
    )
    final.add_argument("--gdal-threads", default=None, help="Valore storico, usato come default di --final-gdal-threads")
    parser.add_argument("--gdal-cache-mb", type=int, default=None, help="Default comune delle due cache GDAL (MB)")
    parser.add_argument("--gdal-timeout", type=int, default=14400, help="Timeout comandi GDAL in secondi")
    parser.add_argument(
        "--max-items",
        type=int,
        default=env_int("MSIMNE_MAX_ITEMS", 10),
        help="Numero massimo scene Sentinel-2 per tile/mese",
    )
    parser.add_argument(
        "--initial-max-items",
        type=int,
        default=env_int("MSIMNE_INITIAL_MAX_ITEMS", 6),
        help="Scene iniziali da provare prima di salire a --max-items",
    )
    parser.add_argument(
        "--intermediate-compression",
        choices=("none", "deflate", "lzw"),
        default=os.environ.get("MSIMNE_INTERMEDIATE_COMPRESSION", "none").lower(),
        help="Compressione GeoTIFF temporanei in working_s2",
    )
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
    parser.add_argument(
        "--no-scl",
        action="store_true",
        help="Non usa la maschera nuvole SCL: mediana diretta sulle scene (consigliato --max-items 6-8)",
    )
    parser.add_argument(
        "--aoi-simplify-m",
        type=float,
        default=float(os.environ.get("MSIMNE_AOI_SIMPLIFY_M", "0")),
        help="Semplifica il poligono AOI per mosaico e controlli (metri, es. 5 = mezzo pixel S2); 0 = originale",
    )
    parser.add_argument("--tile-retries", type=int, default=4, help="Tentativi per tile prima di rimandarla al recupero finale")
    parser.add_argument(
        "--network-wait-max-seconds",
        type=int,
        default=1800,
        help="Su errore di rete, attesa massima che Planetary Computer torni raggiungibile",
    )
    parser.add_argument("--http-timeout", type=int, default=60, help="Timeout HTTP (secondi) per STAC e letture COG")
    parser.add_argument("--http-max-retry", type=int, default=8, help="Tentativi HTTP automatici (429/5xx/timeout)")
    parser.add_argument("--skip-disk-check", action="store_true", help="Non controlla lo spazio disco prima di partire")
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
    resolve_performance(args)
    set_gdal_env()
    settings = Settings(
        project_root=args.project_root.resolve(),
        inputs_dir_override=args.inputs_dir.resolve() if args.inputs_dir else None,
        outputs_dir_override=args.outputs_dir.resolve() if args.outputs_dir else None,
        work_dir_override=args.work_dir.resolve() if args.work_dir else None,
        grid_file_override=args.grid_file.resolve() if args.grid_file else None,
        dask_workers=args.workers,
        dask_threads_per_worker=args.threads_per_worker,
        dask_memory_limit=args.memory_limit,
        gdal_threads=args.gdal_threads,
        gdal_warp_memory_mb=args.gdal_warp_memory_mb,
        gdal_cache_mb=args.gdal_cache_mb,
        tile_gdal_cache_mb=args.tile_gdal_cache_mb or 0,
        final_gdal_cache_mb=args.final_gdal_cache_mb or 0,
        gdal_timeout=args.gdal_timeout,
        max_items=args.max_items,
        initial_max_items=args.initial_max_items,
        tile_parallelism=args.tile_parallelism,
        intermediate_compression=args.intermediate_compression,
        seasonal_fallback_coverage_threshold=args.seasonal_fallback_coverage_threshold,
        seasonal_fallback_years=args.seasonal_fallback_years,
        final_gdal_threads=args.final_gdal_threads or "",
        final_parallel_mosaics=args.final_parallel_mosaics,
        use_scl=not args.no_scl,
        aoi_simplify_m=args.aoi_simplify_m,
        tile_retries=args.tile_retries,
        network_wait_max_seconds=args.network_wait_max_seconds,
        http_timeout=args.http_timeout,
        http_max_retry=args.http_max_retry,
        check_disk_space=not args.skip_disk_check,
    )
    os.environ["GDAL_CACHEMAX"] = str(settings.tile_gdal_cache_mb)
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

    logging.getLogger(__name__).info(
        "Parametri: profilo=%s | fase tile: workers=%s threads/worker=%s memoria=%s tile_parallelism=%s "
        "gdal_cache_mb=%s | fase mosaico: final_gdal_threads=%s warp_mb=%s gdal_cache_mb=%s mosaici_paralleli=%s "
        "| scl=%s aoi_simplify_m=%s max_items=%s/%s",
        args.profile or "nessuno",
        settings.dask_workers,
        settings.dask_threads_per_worker,
        settings.dask_memory_limit,
        settings.tile_parallelism,
        settings.tile_gdal_cache_mb,
        settings.final_gdal_threads,
        settings.gdal_warp_memory_mb,
        settings.final_gdal_cache_mb,
        settings.final_parallel_mosaics,
        "si" if settings.use_scl else "no",
        settings.aoi_simplify_m,
        settings.initial_max_items,
        settings.max_items,
    )

    if len(regions) == 1 and not args.all_regions:
        validate_runtime(settings, regions[0])
        logging.getLogger(__name__).info("Regione %s - %s", regions[0], REGION_NAMES[regions[0]])
        run_pipeline(settings, regions[0], start_dt, end_dt)
        return 0

    return run_regions(settings, regions, start_dt, end_dt)


if __name__ == "__main__":
    raise SystemExit(main())
