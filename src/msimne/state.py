from __future__ import annotations

import csv
import sqlite3
import uuid
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class RegionRunRecord:
    run_id: str
    month: str
    region: str
    status: str
    started_at: str
    finished_at: str
    elapsed_seconds: float
    ndvi_output: str = ""
    stack_output: str = ""
    stats_output: str = ""
    valid_ratio_before: float | None = None
    valid_ratio_after: float | None = None
    interpolated_ratio: float | None = None
    error_message: str = ""


class PipelineState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    month TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    message TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS region_runs (
                    run_id TEXT NOT NULL,
                    month TEXT NOT NULL,
                    region TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    elapsed_seconds REAL,
                    ndvi_output TEXT,
                    stack_output TEXT,
                    stats_output TEXT,
                    valid_ratio_before REAL,
                    valid_ratio_after REAL,
                    interpolated_ratio REAL,
                    error_message TEXT,
                    PRIMARY KEY (run_id, region)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    month TEXT NOT NULL,
                    region TEXT,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )

    def create_run(self, month: str) -> str:
        run_id = f"{month}-{uuid.uuid4().hex[:10]}"
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO runs(run_id, month, status, started_at) VALUES (?, ?, ?, ?)",
                (run_id, month, "running", utc_now_iso()),
            )
        return run_id

    def finish_run(self, run_id: str, status: str, message: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ?, message = ? WHERE run_id = ?",
                (status, utc_now_iso(), message, run_id),
            )

    def log_event(
        self,
        run_id: str,
        month: str,
        stage: str,
        status: str,
        message: str = "",
        region: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO events(run_id, month, region, stage, status, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, month, region, stage, status, message, utc_now_iso()),
            )

    def upsert_region(self, record: RegionRunRecord) -> None:
        data = asdict(record)
        columns = ",".join(data)
        placeholders = ",".join("?" for _ in data)
        updates = ",".join(f"{key}=excluded.{key}" for key in data if key not in {"run_id", "region"})
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO region_runs({columns}) VALUES ({placeholders})
                ON CONFLICT(run_id, region) DO UPDATE SET {updates}
                """,
                tuple(data.values()),
            )

    def region_records(self, run_id: str) -> list[RegionRunRecord]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM region_runs WHERE run_id = ? ORDER BY region",
                (run_id,),
            ).fetchall()
        return [RegionRunRecord(**dict(row)) for row in rows]


def write_region_report(records: list[RegionRunRecord], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(records[0]).keys()) if records else [field.name for field in fields(RegionRunRecord)]
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))
