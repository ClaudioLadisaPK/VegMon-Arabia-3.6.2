from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import REGION_NAMES, Settings
from .io import set_gdal_env
from .logging_utils import configure_logging
from .pipeline import run_pipeline
from .preflight import validate_runtime
from .utils import parse_date


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MSIMNE Sentinel-2 NDVI pipeline")
    parser.add_argument("--project-root", type=Path, default=Path.cwd(), help="Cartella progetto")
    parser.add_argument("--region", choices=sorted(REGION_NAMES.keys()), help="Codice regione R01..R14")
    parser.add_argument("--start", help="Data inizio: YYYY-MM-DD o YYYY-MM")
    parser.add_argument("--end", help="Data fine: YYYY-MM-DD o YYYY-MM")
    parser.add_argument("--inputs-dir", type=Path, help="Cartella inputs; default <project-root>/inputs")
    parser.add_argument("--outputs-dir", type=Path, help="Cartella outputs; default <project-root>/outputs")
    parser.add_argument("--grid-file", type=Path, help="Override del file griglia")
    parser.add_argument("--interactive", action="store_true", help="Richiede regione e date in modo interattivo")
    parser.add_argument("--verbose", action="store_true", help="Abilita logging verboso")
    return parser


def prompt_if_missing(args: argparse.Namespace) -> tuple[str, str, str]:
    region = args.region
    start = args.start
    end = args.end

    if args.interactive or region is None or start is None or end is None:
        print("Codici regione disponibili:")
        for code in sorted(REGION_NAMES):
            print(f"  {code} -> {REGION_NAMES[code]}")

    while region is None:
        value = input("Seleziona regione (R01..R14): ").strip().upper()
        if value in REGION_NAMES:
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)
    set_gdal_env()
    settings = Settings(
        project_root=args.project_root.resolve(),
        inputs_dir_override=args.inputs_dir.resolve() if args.inputs_dir else None,
        outputs_dir_override=args.outputs_dir.resolve() if args.outputs_dir else None,
        grid_file_override=args.grid_file.resolve() if args.grid_file else None,
    )
    settings.ensure_directories()
    region, start, end = prompt_if_missing(args)
    start_dt = parse_date(start)
    end_dt = parse_date(end)
    if end_dt <= start_dt:
        parser.error("La data di fine deve essere successiva alla data di inizio.")
    validate_runtime(settings, region)
    logging.getLogger(__name__).info("Regione %s - %s", region, REGION_NAMES[region])
    run_pipeline(settings, region, start_dt, end_dt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
