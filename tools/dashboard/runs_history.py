"""Storico delle run VegMon: indice dei log, versione del codice e curve salvate dal monitor."""
from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
HIST_DIR = HERE / "history"  # <id>.jsonl (campioni) e <id>.meta.json (versione codice)
VERSIONS_FILE = HERE / "versions.json"  # versioni note delle run passate, modificabile a mano
def _default_repo() -> Path:
    env = os.environ.get("VEGMON_REPO")
    if env:
        return Path(env)
    here_repo = HERE.parents[1] if len(HERE.parents) > 1 else HERE
    if (here_repo / "3.6.2.py").exists():  # dashboard dentro <progetto>/tools/dashboard
        return here_repo
    return Path("/home/ladisa/VegMon-Arabia-3.6.2")


REPO = _default_repo()
REGISTER = REPO / "test_runs" / "registro_test.csv"  # registro ufficiale dei test (T01, T02, ...)
ARCHIVE = REPO / "outputs_test" / "archivio"  # log archiviati per test: archivio/T03_R04/run_*.log
LOG_GLOBS = [
    (REPO, ["outputs*/logs/run_*.log", "outputs_test/*/logs/run_*.log", "outputs_test/archivio/*/run_*.log"]),
]
_BASELINE = Path("/home/ladisa/vegmon_baseline")
if _BASELINE.exists():
    LOG_GLOBS.append((_BASELINE, ["outputs*/logs/run_*.log", "outputs_test/*/logs/run_*.log"]))
LOG_NAME_RE = re.compile(r"run_(\d{4}-\d{2})_(R\d{2})_(\d{8})_(\d{6})\.log$")
NET_FAIL_RE = re.compile(r"NameResolution|Max retries exceeded|Rete non raggiungibile")


def run_id(log_path: Path) -> str:
    # solo il nome: resta valido quando il log viene spostato in archivio
    return log_path.stem


def repo_of(log_path: Path) -> Path | None:
    for repo, _globs in LOG_GLOBS:
        try:
            log_path.resolve().relative_to(repo)
            return repo
        except ValueError:
            continue
    return None


def git(repo: Path, *args: str) -> str:
    try:
        return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=10).stdout.rstrip()
    except (OSError, subprocess.SubprocessError):
        return ""


def capture_version(repo: Path, started_at: float | None) -> dict:
    """Versione del codice per una run attiva: commit, branch e modifiche locali in src/."""
    commit = git(repo, "rev-parse", "--short", "HEAD")
    branch = git(repo, "rev-parse", "--abbrev-ref", "HEAD")
    dirty = [l[3:] for l in git(repo, "status", "--porcelain", "--", "src").splitlines() if l.strip()]
    diff_hash = hashlib.sha1(git(repo, "diff", "HEAD", "--", "src").encode()).hexdigest()[:7] if dirty else ""
    # se un file di src e' cambiato dopo l'avvio, la versione catturata ora potrebbe non essere quella in uso
    newer = []
    if started_at:
        for p in (repo / "src").rglob("*.py"):
            try:
                if p.stat().st_mtime > started_at:
                    newer.append(str(p.relative_to(repo)))
            except OSError:
                pass
    label = commit + (f" + modifiche locali ({diff_hash})" if dirty else "")
    return {
        "commit": commit,
        "branch": branch,
        "dirty_files": dirty,
        "diff_hash": diff_hash,
        "label": label,
        "source": "rilevata dal monitor all'avvio" + (" (ATTENZIONE: src modificato dopo l'avvio)" if newer else ""),
        "uncertain": bool(newer),
        "captured_at": time.time(),
    }


def guess_version(repo: Path | None, start_ts: str | None) -> dict:
    if not repo or not start_ts:
        return {"label": "?", "source": "sconosciuta"}
    out = git(repo, "log", "--all", f"--before={start_ts}", "-1", "--format=%h %s")
    if not out:
        return {"label": "?", "source": "sconosciuta"}
    commit, _, subject = out.partition(" ")
    return {"label": f"≥ {commit}", "commit": commit, "source": f"stima: ultimo commit prima dell'avvio ({subject[:60]}); possibili modifiche locali"}


def load_versions() -> dict:
    try:
        return json.loads(VERSIONS_FILE.read_text())
    except (OSError, ValueError):
        return {}


def ts_epoch(ts: str | None) -> float | None:
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp() if ts else None


def file_gb(path: Path) -> float | None:
    try:
        return path.stat().st_size / 1024**3
    except OSError:
        return None


class RunsHistory:
    def __init__(self, log_state_cls):
        self.LogState = log_state_cls
        self.cache: dict[str, tuple[float, int, dict]] = {}
        HIST_DIR.mkdir(exist_ok=True)

    # ---- curve delle run seguite dal monitor
    def append_point(self, log_path: Path, point: dict):
        with open(HIST_DIR / f"{run_id(log_path)}.jsonl", "a") as fh:
            fh.write(json.dumps({k: (round(v, 3) if isinstance(v, float) else v) for k, v in point.items()}) + "\n")

    def ensure_meta(self, log_path: Path, repo: Path, started_at: float | None):
        meta_fp = HIST_DIR / f"{run_id(log_path)}.meta.json"
        if meta_fp.exists():
            return
        meta = capture_version(repo, started_at)
        meta["log"] = str(log_path)
        meta_fp.write_text(json.dumps(meta, indent=1))

    def samples(self, rid: str, max_points: int = 1500) -> list[dict]:
        fp = HIST_DIR / f"{rid}.jsonl"
        if not fp.exists():
            return []
        rows = []
        with open(fp) as fh:
            for line in fh:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
        if len(rows) <= max_points:
            return rows
        # media a blocchi per tenere la risposta leggera
        step = len(rows) / max_points
        out = []
        for i in range(max_points):
            block = rows[int(i * step) : int((i + 1) * step)] or [rows[int(i * step)]]
            avg = {k: sum(r.get(k, 0) or 0 for r in block) / len(block) for k in block[0] if k != "t"}
            avg["t"] = block[-1]["t"]
            avg["tiles"] = block[-1].get("tiles", 0)
            for k in ("cpu", "wr", "rd", "rx", "mem"):  # tieni anche il picco del blocco
                avg[k + "_max"] = max((r.get(k, 0) or 0) for r in block)
            out.append(avg)
        return out

    # ---- indice dei log
    def log_paths(self) -> list[Path]:
        paths = []
        for repo, globs in LOG_GLOBS:
            for g in globs:
                paths.extend(repo.glob(g))
        return sorted(set(paths))

    def summary(self, log_path: Path, live_log: Path | None = None, live_alive: bool = False) -> dict:
        st = log_path.stat()
        key = str(log_path)
        cached = self.cache.get(key)
        if cached and cached[0] == st.st_mtime and cached[1] == st.st_size and log_path != live_log:
            return cached[2]
        log = self.LogState(log_path)
        log.update()
        s = self._summarize(log_path, log, live_log == log_path and live_alive)
        self.cache[key] = (st.st_mtime, st.st_size, s)
        return s

    def _summarize(self, log_path: Path, log, alive: bool) -> dict:
        m = LOG_NAME_RE.search(log_path.name)
        month, code = (m.group(1), m.group(2)) if m else ("?", "?")
        rid = run_id(log_path)
        pt = log.phase_times
        start, end = ts_epoch(log.start), ts_epoch(log.last_ts)
        text_tail = "\n".join(log.lines)
        net_fail = bool(NET_FAIL_RE.search(text_tail))
        if alive:
            status = "in corso"
        elif "end" in pt.get("statistiche", {}):
            status = "ok"
        elif log.traceback or net_fail:
            status = "errore rete" if net_fail else "errore"
        else:
            status = "interrotta"
        labels = {"tile": "tile", "copertura_tile": "copertura tile", "mosaico_stack": "mosaico STACK",
                  "mosaico_ndvi": "mosaico NDVI", "controllo_copertura": "controllo copertura", "statistiche": "statistiche"}
        reached = next((k for k in reversed(list(labels)) if "start" in pt.get(k, {})), None)
        if status not in ("ok", "in corso") and reached:
            status += f" in {labels[reached]}"
        tiles_started = len(log.tile_starts)
        phases = {}
        for k, v in pt.items():
            if "start" in v and "end" in v and k != "fine":
                phases[k] = (ts_epoch(v["end"]) - ts_epoch(v["start"])) / 60
            elif "start" in v and k != "fine" and end:  # fase interrotta o ancora in corso: fino all'ultima riga
                phases[k] = (end - ts_epoch(v["start"])) / 60
        tile_end = ts_epoch(pt.get("tile", {}).get("end")) or (end if status != "ok" else None)
        tile_start = ts_epoch(pt.get("tile", {}).get("start"))
        tile_min = (tile_end - tile_start) / 60 if tile_end and tile_start else None
        rate = tiles_started / tile_min if tile_min and tile_min > 0.5 and tiles_started else None
        prof = re.search(r"profilo=(\w+)", log.params or "")
        par = re.search(r"tile_parallelism=(\d+)", log.params or "")
        workers = re.search(r"workers=(\d+)", log.params or "")
        # versione: meta del monitor > versions.json > stima da git
        meta_fp = HIST_DIR / f"{rid}.meta.json"
        version = None
        if meta_fp.exists():
            try:
                version = json.loads(meta_fp.read_text())
            except ValueError:
                version = None
        if version is None:
            known = load_versions()
            entry = known.get(rid) or known.get(log_path.name)
            if entry:
                version = {"label": entry.get("label", "?"), "source": entry.get("note", "versions.json"), **entry}
        if version is None:
            version = guess_version(repo_of(log_path), log.start)
        outputs_dir = log_path.parent.parent
        stack = file_gb(outputs_dir / "S2" / "STACK" / f"S2_stack_{code}_{month}.tif")
        ndvi = file_gb(outputs_dir / "S2" / "NDVI" / f"S2_ndvi_{code}_{month}.tif")
        short = tiles_started <= 3 and (end or 0) - (start or 0) < 300 and status != "ok"
        return {
            "id": rid,
            "log": str(log_path),
            "log_name": log_path.name,
            "outputs_dir": str(outputs_dir),
            "region": code,
            "region_name": log.region.split(" - ", 1)[-1] if log.region else "",
            "month": month,
            "start": log.start,
            "end": log.last_ts,
            "duration_min": (end - start) / 60 if start and end else None,
            "tile_count": log.tile_count,
            "tiles_todo": log.tiles_todo,
            "tiles_started": tiles_started,
            "tile_rate": rate,
            "phases": phases,
            "status": status,
            "short": short,
            "coverage": log.coverage,
            "timings": log.timings,
            "params": log.params,
            "profile": prof.group(1) if prof else "",
            "parallelism": int(par.group(1)) if par else None,
            "workers": int(workers.group(1)) if workers else None,
            "critical": log.critical_tiles,
            "warnings": len(log.events),
            "net_errors": len(log.net_errors) + (1 if net_fail and not log.net_errors else 0),
            "version": version,
            "stack_gb": stack,
            "ndvi_gb": ndvi,
            "has_samples": (HIST_DIR / f"{rid}.jsonl").exists(),
        }

    # ---- registro ufficiale
    def register(self) -> list[dict]:
        try:
            with open(REGISTER, newline="", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh, delimiter=";"))
        except OSError:
            return []
        out = []
        for row in rows:
            tid = (row.get("id") or "").strip()
            if not tid:
                continue
            folders = sorted(ARCHIVE.glob(f"{tid}_*")) if ARCHIVE.is_dir() else []
            logs = sorted(p for f in folders for p in f.glob("run_*.log"))
            notes = [n.read_text(errors="replace").strip() for f in folders for n in f.glob("*.txt") if not n.name.startswith("run_")]
            entry = {k: (v or "").strip() for k, v in row.items() if k}
            entry.update(
                {
                    "id": tid,
                    "logs": [p.stem for p in logs],
                    "archive": [str(f) for f in folders],
                    "archive_notes": notes,
                    "has_samples": any((HIST_DIR / f"{p.stem}.jsonl").exists() for p in logs),
                }
            )
            out.append(entry)
        return out

    def list(self, live_log: Path | None, alive: bool) -> dict:
        register = self.register()
        linked = {stem for e in register for stem in e["logs"]}
        others = [r for r in self.list_logs(live_log, alive) if r.get("id") not in linked]
        return {"register": register, "register_path": str(REGISTER), "others": others}

    def list_logs(self, live_log: Path | None, alive: bool) -> list[dict]:
        out = []
        for p in self.log_paths():
            try:
                out.append(self.summary(p, live_log, alive))
            except Exception as exc:  # noqa: BLE001 - un log illeggibile non deve rompere l'elenco
                out.append({"id": run_id(p), "log": str(p), "log_name": p.name, "status": "illeggibile", "error": repr(exc)})
        out.sort(key=lambda r: r.get("start") or "", reverse=True)
        return out

    def detail(self, rid: str, live_log: Path | None, alive: bool) -> dict | None:
        reg = next((e for e in self.register() if e["id"] == rid), None)
        if reg is not None:
            runs = [d for stem in reg["logs"] if (d := self.detail(stem, live_log, alive))]
            samples = [x for d in runs for x in d.get("samples", [])]
            return {"kind": "register", "entry": reg, "runs": runs, "samples": samples}
        for p in self.log_paths():
            if run_id(p) == rid:
                s = dict(self.summary(p, live_log, alive))
                log = self.LogState(p)
                log.update()
                s["phase_times"] = log.phase_times
                s["events"] = list(log.events)
                s["tail"] = list(log.lines)[-60:]
                s["samples"] = self.samples(rid)
                s["kind"] = "log"
                return s
        return None
