"""
storage/result_repository.py
Repository untuk membaca dan menulis hasil perbandingan per job.
Setiap job memiliki file DuckDB sendiri agar data besar tidak saling mengganggu.
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import duckdb

from config.constants import (
    RESULT_MATCH, RESULT_MISMATCH,
    RESULT_MISSING_LEFT, RESULT_MISSING_RIGHT, RESULT_DUPLICATE_KEY,
)

logger = logging.getLogger(__name__)

_RESULT_SCHEMA = """
CREATE TABLE IF NOT EXISTS compare_results (
    row_id          INTEGER,
    status          VARCHAR,
    key_values      VARCHAR,       -- JSON serialized composite key
    left_data       VARCHAR,       -- JSON serialized row data
    right_data      VARCHAR,       -- JSON serialized row data
    diff_columns    VARCHAR        -- JSON list of column names yang berbeda
);
CREATE INDEX IF NOT EXISTS idx_status ON compare_results(status);
CREATE INDEX IF NOT EXISTS idx_row_id ON compare_results(row_id);
"""


class ResultRepository:
    """Kelola baca/tulis hasil perbandingan per job ke file DuckDB terpisah."""

    def __init__(self, job_db_path: Path):
        self._path = str(job_db_path)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    # ------------------------------------------------------------------ lifecycle

    def open(self):
        self._conn = duckdb.connect(self._path)
        for stmt in _RESULT_SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self._conn.execute(stmt)

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------ write

    def bulk_insert(self, records: List[Dict[str, Any]]) -> None:
        """Simpan batch hasil perbandingan."""
        if not records or not self._conn:
            return
        import json
        rows = [
            (
                r["row_id"],
                r["status"],
                json.dumps(r.get("key_values", {}), ensure_ascii=False),
                json.dumps(r.get("left_data", {}), ensure_ascii=False),
                json.dumps(r.get("right_data", {}), ensure_ascii=False),
                json.dumps(r.get("diff_columns", []), ensure_ascii=False),
            )
            for r in records
        ]
        self._conn.executemany(
            "INSERT INTO compare_results VALUES (?, ?, ?, ?, ?, ?)", rows
        )

    def clear(self) -> None:
        if self._conn:
            self._conn.execute("DELETE FROM compare_results")

    # ------------------------------------------------------------------ read

    def count_by_status(self) -> Dict[str, int]:
        if not self._conn:
            return {}
        rows = self._conn.execute(
            "SELECT status, COUNT(*) FROM compare_results GROUP BY status"
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    def get_summary(self) -> Dict[str, Any]:
        counts = self.count_by_status()
        total = sum(counts.values())
        return {
            "total_rows": total,
            RESULT_MATCH: counts.get(RESULT_MATCH, 0),
            RESULT_MISMATCH: counts.get(RESULT_MISMATCH, 0),
            RESULT_MISSING_LEFT: counts.get(RESULT_MISSING_LEFT, 0),
            RESULT_MISSING_RIGHT: counts.get(RESULT_MISSING_RIGHT, 0),
            RESULT_DUPLICATE_KEY: counts.get(RESULT_DUPLICATE_KEY, 0),
        }

    def get_page(
        self,
        page: int = 1,
        page_size: int = 100,
        status_filter: Optional[str] = None,
        search_key: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Ambil satu halaman hasil perbandingan dengan filter opsional.
        Paramter search_key melakukan text search pada key_values JSON.
        Returns (rows, total_count).
        """
        if not self._conn:
            return [], 0

        where_parts: list = []
        params: list = []

        if status_filter:
            where_parts.append("status = ?")
            params.append(status_filter)
        if search_key:
            where_parts.append("key_values LIKE ?")
            params.append(f"%{search_key}%")

        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        count_row = self._conn.execute(
            f"SELECT COUNT(*) FROM compare_results {where}", params
        ).fetchone()
        total = count_row[0] if count_row else 0

        offset = (page - 1) * page_size
        rows = self._conn.execute(
            f"""SELECT row_id, status, key_values, left_data, right_data, diff_columns
                FROM compare_results {where}
                ORDER BY row_id
                LIMIT ? OFFSET ?""",
            params + [page_size, offset],
        ).fetchall()

        import json
        result = []
        for r in rows:
            result.append({
                "row_id": r[0],
                "status": r[1],
                "key_values": json.loads(r[2]) if r[2] else {},
                "left_data": json.loads(r[3]) if r[3] else {},
                "right_data": json.loads(r[4]) if r[4] else {},
                "diff_columns": json.loads(r[5]) if r[5] else [],
            })
        return result, total

    def get_mismatch_column_breakdown(self) -> List[Tuple[str, int]]:
        """
        Kembalikan daftar (nama_kolom, jumlah) mismatch per kolom, diurutkan terbanyak dulu.
        Menggunakan DuckDB JSON unnest agar tidak perlu load ke Python.
        """
        if not self._conn:
            return []
        try:
            rows = self._conn.execute("""
                SELECT col_name, COUNT(*) AS cnt
                FROM (
                    SELECT unnest(from_json(diff_columns, '["VARCHAR"]')) AS col_name
                    FROM compare_results
                    WHERE status = 'mismatch'
                      AND diff_columns IS NOT NULL
                      AND diff_columns != '[]'
                )
                WHERE col_name IS NOT NULL
                GROUP BY col_name
                ORDER BY cnt DESC
                LIMIT 15
            """).fetchall()
            return [(r[0], r[1]) for r in rows]
        except Exception as e:
            logger.warning("Gagal hitung breakdown mismatch: %s", e)
            return []

    def export_to_file(self, output_path: str, status_filter: Optional[str] = None) -> None:
        """
        Export hasil ke file CSV atau Excel secara efisien.
        CSV: gunakan DuckDB COPY langsung ke file (tanpa lewat Python memory).
        Excel: gunakan openpyxl write_only mode agar streaming ke disk.
        """
        if not self._conn:
            raise RuntimeError("Repository belum dibuka.")
        from pathlib import Path as _Path
        _Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        if output_path.lower().endswith(".csv"):
            self._export_csv(output_path, status_filter)
        else:
            self._export_excel(output_path, status_filter)

    def _export_csv(self, path: str, status_filter: Optional[str]) -> None:
        """Export ke CSV menggunakan DuckDB COPY — tanpa lewat Python memory."""
        where = ""
        if status_filter:
            # Parameterized COPY belum didukung DuckDB, escape manual (nilai dari konstanta internal)
            safe = status_filter.replace("'", "''")
            where = f"WHERE status = '{safe}'"
        self._conn.execute(
            f"COPY (SELECT * FROM compare_results {where} ORDER BY row_id) "
            f"TO '{path}' (FORMAT CSV, HEADER TRUE)"
        )
        logger.info("Export CSV selesai: %s", path)

    def _export_excel(self, path: str, status_filter: Optional[str]) -> None:
        """Export ke Excel menggunakan openpyxl write_only (streaming, hemat memori)."""
        try:
            import openpyxl
        except ImportError:
            raise RuntimeError("Library openpyxl diperlukan untuk export Excel.")
        import json

        wb = openpyxl.Workbook(write_only=True)
        ws = wb.create_sheet("Detail Hasil")
        ws.append(["row_id", "status", "key_values", "left_data", "right_data", "diff_columns"])

        batch_size = 5_000
        page = 1
        while True:
            records, total = self.get_page(page, batch_size, status_filter)
            for rec in records:
                ws.append([
                    rec["row_id"],
                    rec["status"],
                    json.dumps(rec["key_values"], ensure_ascii=False),
                    json.dumps(rec["left_data"], ensure_ascii=False),
                    json.dumps(rec["right_data"], ensure_ascii=False),
                    json.dumps(rec["diff_columns"], ensure_ascii=False),
                ])
            if page * batch_size >= total:
                break
            page += 1

        wb.save(path)
        logger.info("Export Excel selesai: %s", path)

    def export_all(self, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Export semua hasil ke list Python.
        Untuk dataset besar lebih baik pakai export_to_file().
        """
        rows, _ = self.get_page(1, 9_999_999, status_filter)
        return rows

