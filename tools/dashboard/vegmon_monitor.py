#!/usr/bin/env python3
"""Monitor web per le run VegMon Arabia.

Su Linux/WSL legge /proc (solo libreria standard); su Windows usa psutil (presente
nell'ambiente conda `msimne`, richiesto da Dask).

Uso:
    python vegmon_monitor.py                       # trova da solo la run attiva
    python vegmon_monitor.py --outputs-dir <cartella outputs della run>
    python vegmon_monitor.py --port 8765

Cartella del progetto: variabile VEGMON_REPO, altrimenti quella che contiene tools/dashboard.
Poi aprire http://localhost:8765.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import socket
import ssl
import threading
import time
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from runs_history import REPO as DEFAULT_REPO, RunsHistory, repo_of

SAMPLE_SECONDS = 2.0
HISTORY_POINTS = 5400  # 3 h a 2 s
HAS_PROC = os.path.exists("/proc/stat")
CLK_TCK = os.sysconf("SC_CLK_TCK") if HAS_PROC else 100
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if HAS_PROC else 1
# Benchmark R04 (200 tile, profilo wsl): minuti per fase, scalati sul numero di tile
BENCH_TILES = 200
PROBE_SECONDS = 15
PROBE_TARGETS = [
    ("STAC Planetary Computer", "https://planetarycomputer.microsoft.com/api/stac/v1"),
    ("Blob Sentinel-2 (Azure)", "https://sentinel2l2a01.blob.core.windows.net/"),
]
NET_ERR_RE = re.compile(r"errore di rete|Rete non raggiungibile|NameResolution|Max retries exceeded|timed out|ConnectionError|CURL error|HTTP response code", re.I)
BENCH_MIN = {"mosaico_stack": 7.0, "mosaico_ndvi": 2.0, "controllo_copertura": 0.5, "statistiche": 0.25}

PHASES = [
    ("tile", "Tile"),
    ("copertura_tile", "Copertura tile"),
    ("mosaico_stack", "Mosaico STACK"),
    ("mosaico_ndvi", "Mosaico NDVI"),
    ("controllo_copertura", "Controllo copertura"),
    ("statistiche", "Statistiche"),
    ("fine", "Fine"),
]

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| (\w+) \| ([\w.]+) \| (.*)$")
TILE_START_RE = re.compile(r"^Tile (N[\d_]+-E[\d_]+) \((\d+)/(\d+)\)$")
TILE_NAME_RE = re.compile(r"^N(\d+)_(\d+)-E(\d+)_(\d+)$")
TILE_IN_MSG_RE = re.compile(r"(N\d+_\d+-E\d+_\d+)")


# --------------------------------------------------------------------------- /proc
def read(path: str | Path, default: str = "") -> str:
    try:
        with open(path, "r") as fh:
            return fh.read()
    except OSError:
        return default


def cpu_times() -> list[tuple[int, int]]:
    """(busy, total) per CPU aggregata (indice 0) e per core."""
    out = []
    for line in read("/proc/stat").splitlines():
        if not line.startswith("cpu"):
            break
        vals = [int(v) for v in line.split()[1:]]
        idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
        total = sum(vals[:8])
        out.append((total - idle, total))
    return out


def meminfo() -> dict[str, float]:
    info = {}
    for line in read("/proc/meminfo").splitlines():
        key, _, rest = line.partition(":")
        info[key] = int(rest.split()[0]) * 1024
    gib = 1024**3
    return {
        "total": info.get("MemTotal", 0) / gib,
        "used": (info.get("MemTotal", 0) - info.get("MemAvailable", 0)) / gib,
        "cache": (info.get("Cached", 0) + info.get("Buffers", 0)) / gib,
        "swap_total": info.get("SwapTotal", 0) / gib,
        "swap_used": (info.get("SwapTotal", 0) - info.get("SwapFree", 0)) / gib,
    }


def net_bytes() -> tuple[int, int]:
    rx = tx = 0
    for line in read("/proc/net/dev").splitlines()[2:]:
        name, _, rest = line.partition(":")
        name = name.strip()
        if name == "lo" or name.startswith(("docker", "veth", "br-")):
            continue
        vals = rest.split()
        if len(vals) >= 9:
            rx += int(vals[0])
            tx += int(vals[8])
    return rx, tx


def probe(url: str, timeout: float = 10.0) -> dict:
    """DNS, connessione TCP e risposta HTTPS (time to first byte) verso un endpoint."""
    u = urlparse(url)
    host, port = u.hostname, u.port or 443
    res = {"t": time.time(), "ok": False}
    try:
        t0 = time.perf_counter()
        addr = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)[0][4]
        t1 = time.perf_counter()
        res["dns_ms"] = (t1 - t0) * 1000
        sock = socket.create_connection(addr[:2], timeout=timeout)
        t2 = time.perf_counter()
        res["tcp_ms"] = (t2 - t1) * 1000
        with ssl.create_default_context().wrap_socket(sock, server_hostname=host) as tls:
            tls.settimeout(timeout)
            path = u.path or "/"
            tls.sendall(f"HEAD {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nUser-Agent: vegmon-monitor\r\n\r\n".encode())
            first = tls.recv(64)
            t3 = time.perf_counter()
        res["http_ms"] = (t3 - t2) * 1000
        res["total_ms"] = (t3 - t0) * 1000
        status = first.split(b" ")[1].decode() if first.startswith(b"HTTP/") else "?"
        res["status"] = status
        res["ok"] = status[:1] in ("2", "3", "4")  # 4xx = server raggiungibile
    except Exception as exc:  # noqa: BLE001
        res["error"] = f"{type(exc).__name__}: {exc}"[:200]
    return res


def root_block_device() -> str | None:
    for line in read("/proc/mounts").splitlines():
        parts = line.split()
        if len(parts) > 1 and parts[1] == "/" and parts[0].startswith("/dev/"):
            return os.path.basename(parts[0])
    return None


def disk_sectors(dev: str | None) -> tuple[int, int]:
    if not dev:
        return 0, 0
    for line in read("/proc/diskstats").splitlines():
        parts = line.split()
        if len(parts) > 9 and parts[2] == dev:
            return int(parts[5]), int(parts[9])
    return 0, 0


def disk_free(path: str) -> dict | None:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    gib = 1024**3
    total = usage.total / gib
    free = usage.free / gib
    return {"path": path, "total": total, "free": free, "used_pct": 100 * (1 - free / total) if total else 0}


def all_pids() -> list[int]:
    return [int(p) for p in os.listdir("/proc") if p.isdigit()]


def proc_stat(pid: int) -> tuple[int, int, int] | None:
    """(ppid, cpu_ticks, rss_bytes)"""
    raw = read(f"/proc/{pid}/stat")
    if not raw:
        return None
    rest = raw[raw.rfind(")") + 2 :].split()
    ppid = int(rest[1])
    ticks = int(rest[11]) + int(rest[12])
    rss = int(rest[21]) * PAGE_SIZE
    return ppid, ticks, rss


def cmdline(pid: int) -> list[str]:
    return [a for a in read(f"/proc/{pid}/cmdline").split("\0") if a]


def proc_io(pid: int) -> tuple[int, int]:
    r = w = 0
    for line in read(f"/proc/{pid}/io").splitlines():
        if line.startswith("read_bytes:"):
            r = int(line.split()[1])
        elif line.startswith("write_bytes:"):
            w = int(line.split()[1])
    return r, w


def role_of(args: list[str], is_main: bool) -> str:
    joined = " ".join(args)
    if is_main:
        return "VegMon (main)"
    exe = os.path.basename(args[0]) if args else "?"
    if exe.startswith("gdal") or exe.startswith("ogr"):
        return exe
    if "resource_tracker" in joined:
        return "resource tracker"
    if "spawn_main" in joined or "dask" in joined:
        return "worker Dask"
    return exe


def proc_cwd(pid: int) -> Path | None:
    try:
        return Path(os.readlink(f"/proc/{pid}/cwd"))
    except OSError:
        return None


# Dischi mostrati: su WSL anche C: di Windows (il disco Linux e' un .vhdx su C:)
DISK_PATHS = ["/", "/mnt/c"] if HAS_PROC else sorted({DEFAULT_REPO.anchor or "C:\\"})


# --------------------------------------------------------------------------- Windows (psutil)
if not HAS_PROC:
    import psutil

    def cpu_times() -> list[tuple[int, int]]:
        out = []
        for t in [psutil.cpu_times()] + psutil.cpu_times(percpu=True):
            total = sum(t)
            out.append((int((total - t.idle) * CLK_TCK), int(total * CLK_TCK)))
        return out

    def meminfo() -> dict[str, float]:
        gib = 1024**3
        vm, sw = psutil.virtual_memory(), psutil.swap_memory()
        return {
            "total": vm.total / gib,
            "used": (vm.total - vm.available) / gib,
            "cache": 0.0,
            "swap_total": sw.total / gib,
            "swap_used": sw.used / gib,
        }

    def net_bytes() -> tuple[int, int]:
        rx = tx = 0
        for name, c in psutil.net_io_counters(pernic=True).items():
            if "loopback" in name.lower():
                continue
            rx += c.bytes_recv
            tx += c.bytes_sent
        return rx, tx

    def root_block_device() -> str | None:
        return "all"

    def disk_sectors(dev: str | None) -> tuple[int, int]:
        c = psutil.disk_io_counters()
        return (c.read_bytes // 512, c.write_bytes // 512) if c else (0, 0)

    def all_pids() -> list[int]:
        return psutil.pids()

    def proc_stat(pid: int) -> tuple[int, int, int] | None:
        try:
            p = psutil.Process(pid)
            with p.oneshot():
                t = p.cpu_times()
                return p.ppid(), int((t.user + t.system) * CLK_TCK), p.memory_info().rss
        except (psutil.Error, OSError):
            return None

    def cmdline(pid: int) -> list[str]:
        try:
            return psutil.Process(pid).cmdline()
        except (psutil.Error, OSError):
            return []

    def proc_io(pid: int) -> tuple[int, int]:
        try:
            c = psutil.Process(pid).io_counters()
            return c.read_bytes, c.write_bytes
        except (psutil.Error, OSError, AttributeError):
            return 0, 0

    def proc_cwd(pid: int) -> Path | None:
        try:
            return Path(psutil.Process(pid).cwd())
        except (psutil.Error, OSError):
            return None


# --------------------------------------------------------------------------- run discovery
def find_runs() -> list[dict]:
    runs = []
    for pid in all_pids():
        args = cmdline(pid)
        if not any(a.endswith("3.6.2.py") for a in args):
            continue
        st = proc_stat(pid)
        if st is None:
            continue
        # salta i figli (multiprocessing) di un altro processo 3.6.2.py
        parent_args = cmdline(st[0])
        if any(a.endswith("3.6.2.py") for a in parent_args):
            continue
        cwd = proc_cwd(pid) or DEFAULT_REPO
        opts = parse_args(args)
        repo = Path(next(a for a in args if a.endswith("3.6.2.py"))).parent
        repo = repo if repo.is_absolute() else (cwd / repo)
        out = Path(opts.get("--outputs-dir") or os.environ.get("MSIMNE_OUTPUTS_DIR", "") or (repo / "outputs"))
        out = out if out.is_absolute() else (cwd / out)
        work = opts.get("--work-dir")
        work = Path(work) if work else out / "working_s2"
        work = work if work.is_absolute() else (cwd / work)
        runs.append({"pid": pid, "args": args, "opts": opts, "outputs_dir": out.resolve(), "work_dir": work.resolve()})
    return runs


def parse_args(args: list[str]) -> dict[str, str]:
    opts = {}
    for i, a in enumerate(args):
        if a.startswith("--"):
            if "=" in a:
                k, v = a.split("=", 1)
                opts[k] = v
            elif i + 1 < len(args) and not args[i + 1].startswith("--"):
                opts[a] = args[i + 1]
            else:
                opts[a] = "true"
    return opts


def latest_log(outputs_dir: Path) -> Path | None:
    logs = sorted((outputs_dir / "logs").glob("run_*.log"), key=lambda p: p.stat().st_mtime)
    return logs[-1] if logs else None


def newest_outputs_dir() -> Path | None:
    candidates = list(DEFAULT_REPO.glob("outputs*/logs/run_*.log")) + list(DEFAULT_REPO.glob("outputs_test/*/logs/run_*.log"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime).parent.parent


# --------------------------------------------------------------------------- log parsing
class LogState:
    def __init__(self, path: Path):
        self.path = path
        self.offset = 0
        self.lines: deque[str] = deque(maxlen=400)
        self.events: deque[dict] = deque(maxlen=200)  # warning/error
        self.start: str | None = None
        self.region = ""
        self.params = ""
        self.area = ""
        self.tile_count = 0
        self.tiles_todo = 0
        self.tile_starts: dict[str, str] = {}
        self.tile_order: list[str] = []
        self.failed_tiles: set[str] = set()
        self.critical_tiles: dict[str, str] = {}
        self.phase_times: dict[str, dict] = {}  # name -> {start, end}
        self.coverage = ""
        self.timings = ""
        self.done_status = ""
        self.last_ts: str | None = None
        self.partial = ""
        self.new_markers = False  # log del codice branch (Avvio/Completato mosaico_*)
        self.old_mosaics = 0
        self.traceback = False
        self.net_errors: list[str] = []  # timestamp degli errori di rete
        self.net_waiting: str | None = None
        self.net_last: dict | None = None

    def _mark(self, name: str, key: str, ts: str):
        self.phase_times.setdefault(name, {})
        if key in self.phase_times[name]:
            return
        self.phase_times[name][key] = ts

    def update(self):
        try:
            size = self.path.stat().st_size
        except OSError:
            return
        if size < self.offset:
            self.__init__(self.path)
        with open(self.path, "r", errors="replace") as fh:
            fh.seek(self.offset)
            chunk = fh.read()
            self.offset = fh.tell()
        text = self.partial + chunk
        lines = text.split("\n")
        self.partial = lines.pop()
        for line in lines:
            self._parse(line)

    def _parse(self, line: str):
        self.lines.append(line)
        m = TS_RE.match(line)
        if not m:
            if line.startswith("Traceback"):
                self.traceback = True
            return
        ts, level, src, msg = m.groups()
        self.last_ts = ts
        if self.start is None:
            self.start = ts
        if NET_ERR_RE.search(msg) and level in ("WARNING", "ERROR", "CRITICAL"):
            self.net_errors.append(ts)
            self.net_last = {"ts": ts, "msg": msg[:300]}
            if msg.startswith("Rete non raggiungibile"):
                self.net_waiting = ts
        if msg.startswith("Rete di nuovo raggiungibile"):
            self.net_waiting = None
            self.net_last = {"ts": ts, "msg": msg[:300]}
        if level in ("WARNING", "ERROR", "CRITICAL"):
            self.events.append({"ts": ts, "level": level, "msg": msg[:300]})
        tm = TILE_START_RE.match(msg)
        if tm:
            tid = tm.group(1)
            if tid not in self.tile_starts:
                self.tile_order.append(tid)
            self.tile_starts[tid] = ts
            self.tile_count = self.tile_count or int(tm.group(3))
            self._mark("tile", "start", ts)
            return
        if msg.startswith("Parametri:"):
            self.params = msg[len("Parametri:"):].strip()
        elif msg.startswith("Regione "):
            self.region = msg[len("Regione "):]
        elif msg.startswith("Area AOI"):
            self.area = msg[len("Area AOI "):]
        elif msg.startswith("Tile count"):
            self.tile_count = int(msg.split()[-1])
        elif msg.startswith("Tile da elaborare"):
            self.tiles_todo = int(msg.split()[3])
            self._mark("tile", "start", ts)
        elif "fallita" in msg and msg.startswith("Tile "):
            tm2 = TILE_IN_MSG_RE.search(msg)
            if tm2:
                self.failed_tiles.add(tm2.group(1))
        elif "soglia critica" in msg or "mancante dopo retry" in msg:
            tm2 = TILE_IN_MSG_RE.search(msg)
            if tm2:
                self.critical_tiles[tm2.group(1)] = msg.rsplit(":", 1)[-1].strip()
        elif msg.startswith("Tile fallite nella fase principale") or (
            msg.startswith("Ricalcolo tile") and len(self.tile_starts) >= (self.tiles_todo or self.tile_count or 1)
        ):
            self._mark("tile", "end", ts)
            self._mark("copertura_tile", "start", ts)
        elif msg.startswith("Chiusura client Dask") and len(self.tile_starts) >= (self.tiles_todo or self.tile_count or 1):
            # su Ctrl+C la chiusura Dask viene loggata anche a tile incomplete: non e' la fine della fase
            self._mark("tile", "end", ts)
            self._mark("copertura_tile", "start", self.phase_times.get("tile", {}).get("end", ts))
            self._mark("copertura_tile", "end", ts)
        elif msg.startswith("Avvio mosaico_"):
            self.new_markers = True
            self._mark(msg.split()[1], "start", ts)
        elif msg.startswith("Completato mosaico_"):
            self._mark(msg.split()[1], "end", ts)
        elif msg.startswith("Mosaico STACK gia presente"):
            self._mark("mosaico_stack", "start", ts)
            self._mark("mosaico_stack", "end", ts)
        elif msg.startswith("Mosaico NDVI gia presente"):
            self._mark("mosaico_ndvi", "start", ts)
            self._mark("mosaico_ndvi", "end", ts)
        elif msg == "Created 1 records" and not self.new_markers:
            # codice base: il GPKG AOI temporaneo viene scritto all'avvio di ogni mosaico
            self.old_mosaics += 1
            self._mark("tile", "end", ts)
            self._mark("copertura_tile", "start", self.phase_times["tile"]["end"])
            self._mark("copertura_tile", "end", ts)
            if self.old_mosaics == 1:
                self._mark("mosaico_stack", "start", ts)
            elif self.old_mosaics == 2:
                self._mark("mosaico_stack", "end", ts)
                self._mark("mosaico_ndvi", "start", ts)
        elif msg.startswith("NDVI coverage before gap filling"):
            self.coverage = msg.rsplit(":", 1)[1].strip()
            self._mark("mosaico_ndvi", "end", ts)
            self._mark("controllo_copertura", "start", ts)
            self._mark("controllo_copertura", "end", ts)
        elif msg.startswith("Copertura NDVI S2_"):
            self.coverage = msg.split(":", 1)[1].strip()
            self._mark("controllo_copertura", "end", ts)
        elif msg.startswith("Calcolo statistiche") or msg.startswith("Statistiche mancanti"):
            self._mark("statistiche", "start", ts)
        elif msg.startswith("Statistiche NDVI create"):
            self._mark("statistiche", "end", ts)
            self._mark("fine", "start", ts)
            self.done_status = self.done_status or "ok"
        elif msg.startswith("Tempi fasi"):
            self.timings = msg.split(":", 1)[1].strip()
        elif msg.startswith("DONE "):
            self.done_status = msg.rsplit("status=", 1)[-1]
            self._mark("fine", "start", ts)
        elif msg.startswith("FAILED "):
            self.done_status = "failed:" + msg.rsplit("status=", 1)[-1]
            self._mark("fine", "start", ts)
        # inizio del controllo copertura = fine dell'ultimo mosaico
        pt = self.phase_times
        if "end" in pt.get("mosaico_ndvi", {}) and "end" in pt.get("mosaico_stack", {}) and "controllo_copertura" not in pt:
            self._mark("controllo_copertura", "start", max(pt["mosaico_ndvi"]["end"], pt["mosaico_stack"]["end"]))


# --------------------------------------------------------------------------- monitor
def parse_ts(ts: str | None) -> float | None:
    if not ts:
        return None
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp()


def dir_size(path: Path) -> int:
    total = 0
    try:
        for root, _dirs, files in os.walk(path):
            for f in files:
                try:
                    total += os.lstat(os.path.join(root, f)).st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


def vrt_size(vrt: Path) -> tuple[int, int, int] | None:
    head = read(vrt)[:4000]
    m = re.search(r'rasterXSize="(\d+)" rasterYSize="(\d+)"', head)
    if not m:
        return None
    bands = len(re.findall(r"<VRTRasterBand", read(vrt))) or 1
    return int(m.group(1)), int(m.group(2)), bands


class Monitor:
    def __init__(self, fixed_outputs: Path | None, record: bool = True):
        self.fixed_outputs = fixed_outputs
        self.record = record
        self.runs = RunsHistory(LogState)
        self.lock = threading.Lock()
        self.history: deque[dict] = deque(maxlen=HISTORY_POINTS)
        self.dev = root_block_device()
        self.prev_cpu = cpu_times()
        self.prev_disk = disk_sectors(self.dev)
        self.prev_t = time.time()
        self.prev_proc: dict[int, int] = {}
        self.prev_io: dict[int, tuple[int, int]] = {}
        self.log: LogState | None = None
        self.outputs_dir: Path | None = None
        self.work_dir: Path | None = None
        self.run: dict | None = None
        self.snapshot: dict = {}
        self.slow_cache: dict = {}
        self.slow_at = 0.0
        self.tile_done_at: dict[str, float] = {}
        self.prev_net = net_bytes()
        self.probes: dict[str, deque] = {name: deque(maxlen=720) for name, _url in PROBE_TARGETS}

    def probe_loop(self):
        while True:
            for name, url in PROBE_TARGETS:
                r = probe(url)
                with self.lock:
                    self.probes[name].append(r)
            time.sleep(PROBE_SECONDS)

    def net_status(self, now: float) -> dict:
        targets = []
        with self.lock:
            snap = {k: list(v) for k, v in self.probes.items()}
        for name, url in PROBE_TARGETS:
            hist = snap.get(name, [])
            last = hist[-1] if hist else None
            hour = [p for p in hist if now - p["t"] <= 3600]
            ok_ms = [p["total_ms"] for p in hour if p["ok"]]
            fails = [p for p in hour if not p["ok"]]
            targets.append(
                {
                    "name": name,
                    "host": urlparse(url).hostname,
                    "last": last,
                    "uptime_pct": 100 * (len(hour) - len(fails)) / len(hour) if hour else None,
                    "fails_1h": len(fails),
                    "last_fail": fails[-1] if fails else None,
                    "median_ms": sorted(ok_ms)[len(ok_ms) // 2] if ok_ms else None,
                    "series": [[p["t"], p.get("total_ms") if p["ok"] else None] for p in hist if now - p["t"] <= 3 * 3600],
                }
            )
        log = self.log
        log_info = {}
        if log:
            recent = [t for t in log.net_errors if (parse_ts(t) or 0) >= now - 3600]
            log_info = {
                "errors_total": len(log.net_errors),
                "errors_1h": len(recent),
                "waiting_since": log.net_waiting,
                "last": log.net_last,
            }
        return {"targets": targets, "log": log_info}

    # ---- campionamento
    def loop(self):
        while True:
            try:
                self.sample()
            except Exception as exc:  # il monitor non deve mai morire
                with self.lock:
                    self.snapshot["monitor_error"] = repr(exc)
            time.sleep(SAMPLE_SECONDS)

    def sample(self):
        now = time.time()
        dt = max(now - self.prev_t, 1e-3)
        cpu = cpu_times()
        per_core = []
        for (b1, t1), (b0, t0) in zip(cpu, self.prev_cpu):
            per_core.append(100.0 * (b1 - b0) / (t1 - t0) if t1 > t0 else 0.0)
        self.prev_cpu = cpu
        rd, wr = disk_sectors(self.dev)
        read_mbs = (rd - self.prev_disk[0]) * 512 / dt / 1e6
        write_mbs = (wr - self.prev_disk[1]) * 512 / dt / 1e6
        self.prev_disk = (rd, wr)
        nrx, ntx = net_bytes()
        rx_mbs = max((nrx - self.prev_net[0]) / dt / 1e6, 0)
        tx_mbs = max((ntx - self.prev_net[1]) / dt / 1e6, 0)
        self.prev_net = (nrx, ntx)
        self.prev_t = now
        mem = meminfo()

        # run attiva
        runs = find_runs()
        run = None
        if self.fixed_outputs:
            run = next((r for r in runs if r["outputs_dir"] == self.fixed_outputs), None)
            outputs, work = self.fixed_outputs, (run["work_dir"] if run else self.fixed_outputs / "working_s2")
        elif runs:
            run = max(runs, key=lambda r: r["pid"])
            outputs, work = run["outputs_dir"], run["work_dir"]
        else:
            outputs = self.outputs_dir or newest_outputs_dir()
            work = self.work_dir or (outputs / "working_s2" if outputs else None)
        if outputs != self.outputs_dir:
            self.tile_done_at = {}
            self.log = None
        self.outputs_dir, self.work_dir, self.run = outputs, work, run

        log_path = latest_log(outputs) if outputs else None
        if log_path and (self.log is None or self.log.path != log_path):
            self.log = LogState(log_path)
            self.history.clear()
        if self.log:
            self.log.update()

        procs = self.process_table(run, dt)
        tiles = self.scan_tiles(work, now)
        mosaic = self.mosaic_progress(outputs, work, procs)
        if now - self.slow_at > 15 and outputs:
            self.slow_at = now
            self.slow_cache = {
                "work_bytes": dir_size(work) if work else 0,
                "outputs_bytes": dir_size(outputs / "S2"),
            }

        done = tiles["counts"]["done"]
        point = {
            "t": now,
            "cpu": per_core[0] if per_core else 0,
            "mem": mem["used"],
            "rd": max(read_mbs, 0),
            "wr": max(write_mbs, 0),
            "tiles": done,
            "proc_cpu": sum(p["cpu"] for p in procs),
            "rx": rx_mbs,
            "tx": tx_mbs,
        }
        if dt >= 1.0:  # evita picchi falsi su campioni ravvicinati
            self.history.append(point)
            if self.record and run and log_path:
                try:
                    repo = repo_of(log_path)
                    if repo:
                        self.runs.ensure_meta(log_path, repo, parse_ts(self.log.start) if self.log else None)
                    self.runs.append_point(log_path, point)
                except OSError:
                    pass
        snap = {
            "now": now,
            "host": {
                "cpu": per_core[0] if per_core else 0,
                "cores": per_core[1:],
                "mem": mem,
                "disk_dev": self.dev,
                "read_mbs": max(read_mbs, 0),
                "write_mbs": max(write_mbs, 0),
                "disks": [d for d in (disk_free(p) for p in DISK_PATHS) if d],
                **self.slow_cache,
            },
            "run": self.run_info(run, outputs, log_path),
            "procs": procs,
            "tiles": tiles,
            "mosaic": mosaic,
            "net": {"rx_mbs": rx_mbs, "tx_mbs": tx_mbs, "rx_total_gb": nrx / 1024**3, **self.net_status(now)},
        }
        snap["progress"] = self.progress(snap)
        with self.lock:
            self.snapshot = snap

    def process_table(self, run: dict | None, dt: float) -> list[dict]:
        if not run:
            self.prev_proc = {}
            return []
        children: dict[int, list[int]] = {}
        stats = {}
        for pid in all_pids():
            st = proc_stat(pid)
            if st:
                stats[pid] = st
                children.setdefault(st[0], []).append(pid)
        tree, stack = [], [run["pid"]]
        while stack:
            pid = stack.pop()
            tree.append(pid)
            stack.extend(children.get(pid, []))
        rows, cur, cur_io = [], {}, {}
        for pid in tree:
            if pid not in stats:
                continue
            _ppid, ticks, rss = stats[pid]
            cur[pid] = ticks
            prev = self.prev_proc.get(pid)
            cpu = 100.0 * (ticks - prev) / CLK_TCK / dt if prev is not None else 0.0
            io = proc_io(pid)
            cur_io[pid] = io
            pio = self.prev_io.get(pid, io)
            args = cmdline(pid)
            rows.append(
                {
                    "pid": pid,
                    "role": role_of(args, pid == run["pid"]),
                    "cpu": max(cpu, 0.0),
                    "rss_gb": rss / 1024**3,
                    "io_r": max(io[0] - pio[0], 0) / dt / 1e6,
                    "io_w": max(io[1] - pio[1], 0) / dt / 1e6,
                    "cmd": " ".join(shlex.quote(a) for a in args)[:400],
                }
            )
        self.prev_proc, self.prev_io = cur, cur_io
        return rows

    def scan_tiles(self, work: Path | None, now: float) -> dict:
        items, counts = [], {"done": 0, "running": 0, "pending": 0, "failed": 0, "critical": 0}
        log = self.log
        if work and work.is_dir():
            for entry in os.scandir(work):
                if not entry.is_dir():
                    continue
                m = TILE_NAME_RE.match(entry.name)
                if not m:
                    continue
                try:
                    names = os.listdir(entry.path)
                except OSError:
                    names = []
                has_ndvi = any(n.startswith("ndvi_") and n.endswith(".tif") and ".partial" not in n for n in names)
                has_stack = any(n.startswith("stack_") and n.endswith(".tif") and ".partial" not in n for n in names)
                tid = entry.name
                if has_ndvi and has_stack:
                    state = "done"
                    if tid not in self.tile_done_at:
                        try:
                            self.tile_done_at[tid] = max(
                                os.stat(os.path.join(entry.path, n)).st_mtime for n in names if n.endswith(".tif")
                            )
                        except (OSError, ValueError):
                            self.tile_done_at[tid] = now
                elif log and tid in log.tile_starts and self.run:
                    state = "running"
                else:
                    state = "pending"
                if log and tid in log.failed_tiles and state != "done":
                    state = "failed"
                if log and tid in log.critical_tiles:
                    counts["critical"] += 1
                counts[state] += 1
                items.append(
                    {
                        "id": tid,
                        "x": float(f"{m.group(3)}.{m.group(4)}"),
                        "y": float(f"{m.group(1)}.{m.group(2)}"),
                        "s": state,
                        "crit": log.critical_tiles.get(tid) if log else None,
                        "started": log.tile_starts.get(tid) if log else None,
                    }
                )
        total = len(items) or (log.tile_count if log else 0)
        # completamenti per minuto (ultimi 10 minuti, da mtime dei file)
        recent = [t for t in self.tile_done_at.values() if now - t <= 600]
        rate10 = len(recent) / 10.0
        started_in_run = parse_ts(log.start) if log else None
        in_run = [t for t in self.tile_done_at.values() if started_in_run and t >= started_in_run]
        rate_run = len(in_run) / max((now - started_in_run) / 60, 1e-3) if started_in_run and in_run else 0.0
        timeline = sorted(t for t in self.tile_done_at.values() if started_in_run and t >= started_in_run - 60)
        return {
            "total": total,
            "counts": counts,
            "items": items,
            "rate10": rate10,
            "rate_run": rate_run,
            "done_this_run": len(in_run),
            "timeline": timeline[-2000:],
        }

    def mosaic_progress(self, outputs: Path | None, work: Path | None, procs: list[dict]) -> dict:
        out = {"steps": [], "gdal": []}
        if not outputs:
            return out
        for kind, folder in (("stack", "STACK"), ("ndvi", "NDVI")):
            info = {"kind": kind}
            final = sorted((outputs / "S2" / folder).glob(f"S2_{kind}_*.tif"))
            final = [p for p in final if ".partial" not in p.name]
            partial = sorted((outputs / "S2" / folder).glob(f"S2_{kind}_*.partial.tif"))
            tmp = sorted(work.glob(f"_tmp_S2_{kind}_*.tif")) if work and work.is_dir() else []
            vrt = sorted(work.glob(f"_tmp_S2_{kind}_*.vrt")) if work and work.is_dir() else []
            if final:
                info["final"] = {"name": final[-1].name, "gb": final[-1].stat().st_size / 1024**3}
            if tmp:
                info["warp_gb"] = tmp[-1].stat().st_size / 1024**3
            if partial:
                info["cog_gb"] = partial[-1].stat().st_size / 1024**3
            if vrt:
                dims = vrt_size(vrt[-1])
                if dims:
                    x, y, b = dims
                    info["dims"] = [x, y, b]
                    info["expected_warp_gb"] = x * y * b * 2 / 1024**3
            out["steps"].append(info)
        out["gdal"] = [p["role"] for p in procs if p["role"].startswith("gdal")]
        return out

    def run_info(self, run: dict | None, outputs: Path | None, log_path: Path | None) -> dict:
        log = self.log
        info = {
            "alive": bool(run),
            "pid": run["pid"] if run else None,
            "cmd": " ".join(run["args"]) if run else "",
            "region_arg": (run or {}).get("opts", {}).get("--region"),
            "month": (run or {}).get("opts", {}).get("--month"),
            "outputs_dir": str(outputs) if outputs else None,
            "log": str(log_path) if log_path else None,
        }
        if log:
            if not info["month"] and log_path:
                m = re.match(r"run_(\d{4}-\d{2})_", log_path.name)
                info["month"] = m.group(1) if m else None
            info.update(
                {
                    "region": log.region,
                    "params": log.params,
                    "area": log.area,
                    "start": log.start,
                    "last_ts": log.last_ts,
                    "phase_times": log.phase_times,
                    "coverage": log.coverage,
                    "timings": log.timings,
                    "status": log.done_status,
                    "events": list(log.events)[-60:],
                    "tail": list(log.lines)[-80:],
                    "critical": log.critical_tiles,
                    "tiles_todo": log.tiles_todo,
                    "traceback": log.traceback,
                }
            )
        return info

    def progress(self, snap: dict) -> dict:
        run, tiles = snap["run"], snap["tiles"]
        pt = run.get("phase_times", {}) or {}
        status = run.get("status", "")
        current = None
        for key, _label in reversed(PHASES):
            if "start" in pt.get(key, {}):
                current = key
                break
        if status:
            state = "failed" if status.startswith("failed") else "done"
            current = "fine"
        elif not run.get("alive"):
            state = ("failed" if run.get("traceback") else "stopped") if run.get("start") else "idle"
        else:
            state = "running"
        # ETA
        total, done = tiles["total"], tiles["counts"]["done"]
        rate = tiles["rate10"] or tiles["rate_run"]
        remaining_tiles = max(total - done, 0)
        scale = (total or BENCH_TILES) / BENCH_TILES
        eta_min = 0.0
        if current in (None, "tile"):
            eta_min += remaining_tiles / rate if rate > 0 else float("nan")
            eta_min += 1.0  # copertura tile
        for key in ("mosaico_stack", "mosaico_ndvi", "controllo_copertura", "statistiche"):
            if "end" in pt.get(key, {}):
                continue
            bench = BENCH_MIN[key] * scale
            if "start" in pt.get(key, {}):
                elapsed = (snap["now"] - parse_ts(pt[key]["start"])) / 60
                eta_min += max(bench - elapsed, 0.2)
            else:
                eta_min += bench
        if state in ("done", "failed"):
            eta_min = 0.0
        start = parse_ts(run.get("start"))
        end = parse_ts(pt.get("fine", {}).get("start")) if state in ("done", "failed") else None
        return {
            "state": state,
            "phase": current,
            "phases": PHASES,
            "elapsed_min": ((end or snap["now"]) - start) / 60 if start else None,
            "eta_min": None if eta_min != eta_min else eta_min,
            "rate": rate,
        }

    def status(self) -> dict:
        with self.lock:
            snap = dict(self.snapshot)
        return snap

    def history_since(self, since: float) -> list[dict]:
        with self.lock:
            return [p for p in self.history if p["t"] > since]


# --------------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    monitor: Monitor
    page: bytes

    def log_message(self, *_args):
        pass

    def _send(self, body: bytes, ctype: str):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/status"):
            self._send(json.dumps(self.monitor.status()).encode(), "application/json")
        elif self.path.startswith("/api/history"):
            m = re.search(r"since=([\d.]+)", self.path)
            since = float(m.group(1)) if m else 0.0
            self._send(json.dumps(self.monitor.history_since(since)).encode(), "application/json")
        elif self.path.startswith("/api/runs"):
            m = self.monitor
            live = m.log.path if m.log else None
            self._send(json.dumps(m.runs.list(live, bool(m.run))).encode(), "application/json")
        elif self.path.startswith("/api/run?"):
            m = self.monitor
            rid = re.search(r"id=([\w.-]+)", self.path)
            live = m.log.path if m.log else None
            d = m.runs.detail(rid.group(1), live, bool(m.run)) if rid else None
            if d is None:
                self.send_error(404)
            else:
                self._send(json.dumps(d).encode(), "application/json")
        elif self.path in ("/", "/index.html"):
            self._send(self.page, "text/html; charset=utf-8")
        else:
            self.send_error(404)


def main():
    ap = argparse.ArgumentParser(description="Monitor web per run VegMon")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--outputs-dir", type=Path, help="Cartella outputs da seguire (default: run attiva)")
    ap.add_argument("--no-record", action="store_true", help="Non salvare le curve in history/ (copie di prova)")
    args = ap.parse_args()

    monitor = Monitor(args.outputs_dir.resolve() if args.outputs_dir else None, record=not args.no_record)
    monitor.sample()
    threading.Thread(target=monitor.loop, daemon=True).start()
    threading.Thread(target=monitor.probe_loop, daemon=True).start()
    Handler.monitor = monitor
    Handler.page = (Path(__file__).with_name("index.html")).read_bytes()
    try:
        server = ThreadingHTTPServer((args.host, args.port), Handler)
    except OSError as exc:
        if exc.errno != 98:
            raise
        raise SystemExit(
            f"Porta {args.port} gia in uso: probabilmente il monitor e' gia attivo -> apri http://localhost:{args.port}\n"
            f"Per riavviarlo: pkill -f vegmon_monitor.py   oppure usa un'altra porta: --port {args.port + 1}"
        )
    print(f"VegMon monitor su http://localhost:{args.port}  (Ctrl+C per uscire)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
