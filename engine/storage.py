from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb

from engine.config import TargetConfig

log = logging.getLogger(__name__)

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "scrape.duckdb"

FIELD_TYPE_MAP = {
    "str": "VARCHAR",
    "int": "BIGINT",
    "float": "DOUBLE",
    "url": "VARCHAR",
}


class Storage:
    def __init__(self, db_path: str | Path | None = None) -> None:
        if db_path is None:
            db_path = DEFAULT_DB
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(db_path))

    @staticmethod
    def _q(name: str) -> str:
        return f'"{name}"'

    def ensure_table(self, target: TargetConfig) -> None:
        cols = ["_hash VARCHAR PRIMARY KEY", "_scraped_at TIMESTAMP DEFAULT now()"]
        for name, field_def in target.fields.items():
            sql_type = FIELD_TYPE_MAP[field_def.type]
            cols.append(f"{name} {sql_type}")
        tbl = self._q(target.name)
        ddl = f"CREATE TABLE IF NOT EXISTS {tbl} ({', '.join(cols)})"
        self.conn.execute(ddl)

    def insert(self, target: TargetConfig, records: list[dict]) -> int:
        if not records:
            return 0
        self.ensure_table(target)
        tbl = self._q(target.name)
        cols = ["_hash"] + list(target.fields.keys())
        placeholders = ", ".join(["?"] * len(cols))
        sql = (
            f"INSERT OR IGNORE INTO {tbl} ({', '.join(cols)}) "
            f"VALUES ({placeholders})"
        )
        inserted = 0
        for record in records:
            values = [record["_hash"]] + [record.get(c) for c in cols[1:]]
            try:
                self.conn.execute(sql, values)
                inserted += 1
            except duckdb.ConstraintException:
                pass
        return inserted

    def count(self, target_name: str) -> int:
        try:
            result = self.conn.execute(
                f"SELECT count(*) FROM {self._q(target_name)}"
            ).fetchone()
            return result[0] if result else 0
        except duckdb.CatalogException:
            return 0

    @staticmethod
    def _safe_export_path(path: str | Path) -> Path:
        path = Path(path).resolve()
        data_dir = Path(__file__).resolve().parent.parent / "data"
        if not str(path).startswith(str(data_dir.resolve())):
            raise ValueError(f"Export path must be inside data/: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def export_csv(self, target_name: str, path: str | Path) -> None:
        path = self._safe_export_path(path)
        tbl = self._q(target_name)
        self.conn.execute(
            f"COPY (SELECT * FROM {tbl} ORDER BY _scraped_at) "
            f"TO '{path}' (HEADER, DELIMITER ',')"
        )
        log.info("exported %s to %s", target_name, path)

    def export_json(self, target_name: str, path: str | Path) -> None:
        path = self._safe_export_path(path)
        tbl = self._q(target_name)
        rows = self.conn.execute(
            f"SELECT * FROM {tbl} ORDER BY _scraped_at"
        ).fetchdf().to_dict(orient="records")
        with open(path, "w") as f:
            json.dump(rows, f, indent=2, default=str)
        log.info("exported %s to %s", target_name, path)

    def export_parquet(self, target_name: str, path: str | Path) -> None:
        path = self._safe_export_path(path)
        tbl = self._q(target_name)
        self.conn.execute(
            f"COPY (SELECT * FROM {tbl} ORDER BY _scraped_at) "
            f"TO '{path}' (FORMAT PARQUET)"
        )
        log.info("exported %s to %s", target_name, path)

    def close(self) -> None:
        self.conn.close()
