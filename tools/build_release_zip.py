"""Crea lo zip di rilascio per la VM (solo file versionati, nessun output).

Uso: python tools/build_release_zip.py V03
Produce VegMon_<versione>.zip con dentro la cartella VegMon-Arabia-3.6.2/ e VERSIONE.txt.
"""
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FOLDER = "VegMon-Arabia-3.6.2"
EXCLUDE = {"CLAUDE.md"}  # note di sviluppo locali

version = sys.argv[1] if len(sys.argv) > 1 else "V03"
git = lambda *a: subprocess.run(["git", "-C", str(ROOT), *a], capture_output=True, text=True, check=True).stdout.strip()
if git("status", "--porcelain", "--untracked-files=no"):
    sys.exit("Ci sono modifiche non committate: committare prima di creare il rilascio.")
files = [f for f in git("ls-files").splitlines() if f not in EXCLUDE]
info = (
    f"VegMon {version}\n"
    f"Commit: {git('rev-parse', '--short', 'HEAD')} ({git('rev-parse', '--abbrev-ref', 'HEAD')})\n"
    f"Data commit: {git('log', '-1', '--format=%ci')}\n"
    f"Creato: {datetime.now():%Y-%m-%d %H:%M}\n"
    f"File: {len(files)}\n"
    "Istruzioni di installazione sulla VM: docs/ISTRUZIONI_CODEX_V03.md\n"
)
out = ROOT / f"VegMon_{version}.zip"
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for f in files:
        z.write(ROOT / f, f"{FOLDER}/{f}")
    z.writestr(f"{FOLDER}/VERSIONE.txt", info)
    z.write(ROOT / "docs" / "ISTRUZIONI_CODEX_V03.md", "ISTRUZIONI_CODEX_V03.md")
print(info)
print(f"Creato {out} ({out.stat().st_size / 1e6:.1f} MB)")
