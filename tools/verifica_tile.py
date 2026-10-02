"""Controlla le tile intermedie in working_s2 e rimuove quelle illeggibili o troncate.

Serve prima di riprendere con la V03 una regione iniziata con una versione precedente,
che scriveva le tile direttamente col nome finale (una tile interrotta a meta' sembrava completa).

Uso:
    python tools/verifica_tile.py outputs\\working_s2            # solo controllo, non cancella nulla
    python tools/verifica_tile.py outputs\\working_s2 --rimuovi  # rimuove le cartelle tile rotte
"""
import argparse
import shutil
import sys
from pathlib import Path

import rasterio
from rasterio.windows import Window


def tile_ok(path: Path) -> str | None:
    """None se il file e' leggibile fino all'ultima riga, altrimenti il motivo."""
    try:
        with rasterio.open(path) as src:
            if src.width == 0 or src.height == 0:
                return "dimensioni nulle"
            last = Window(0, src.height - 1, src.width, 1)
            src.read(window=last)  # un file troncato fallisce sull'ultima riga
            src.read(1, window=Window(0, 0, src.width, 1))
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {str(exc).splitlines()[0][:120]}"
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("work_dir", type=Path)
    parser.add_argument("--rimuovi", action="store_true", help="rimuove le cartelle tile con file rotti")
    args = parser.parse_args()
    if not args.work_dir.is_dir():
        print(f"Cartella non trovata: {args.work_dir}")
        return 1
    tiles = sorted(p for p in args.work_dir.iterdir() if p.is_dir())
    bad = []
    for i, tile in enumerate(tiles, 1):
        problems = []
        for tif in sorted(tile.glob("*.tif")):
            if ".partial" in tif.name or ".fallback" in tif.name:
                problems.append(f"{tif.name}: file temporaneo")
                continue
            reason = tile_ok(tif)
            if reason:
                problems.append(f"{tif.name}: {reason}")
        if problems:
            bad.append((tile, problems))
        if i % 100 == 0:
            print(f"  controllate {i}/{len(tiles)}")
    print(f"Tile controllate: {len(tiles)} | rotte: {len(bad)}")
    for tile, problems in bad:
        print(f"  {tile.name}: {'; '.join(problems)}")
        if args.rimuovi:
            shutil.rmtree(tile)
    if bad and args.rimuovi:
        print(f"Rimosse {len(bad)} cartelle: verranno rifatte alla ripresa.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
