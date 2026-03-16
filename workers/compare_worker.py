# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
workers/compare_worker.py
Worker thread untuk proses perbandingan data di background.
Menggunakan QThread agar UI tidak freeze saat proses data besar.
Mengintegrasikan semua komponen: file reader, postgres connector,
DuckDB compare engine, dan result repository.
"""

from __future__ import annotations
import logging
import traceback
from pathlib import Path

import duckdb
from PySide6.QtCore import QThread, Signal

from config.settings import AppSettings
from config.constants import (
    JOB_STATUS_PROCESSING, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED,
    STEP_INIT, STEP_IMPORT_LEFT, STEP_IMPORT_RIGHT,
    STEP_NORMALIZE, STEP_COMPARE, STEP_SAVE_RESULT, STEP_DONE,
    JOB_TYPE_FILE_VS_FILE, JOB_TYPE_FILE_VS_PG, JOB_TYPE_DB_VS_DB,
)
from models.job import CompareJob
from models.compare_config import CompareConfig, DataSourceConfig
from core.compare_engine import CompareEngine
from storage.job_manager import JobManager
from storage.result_repository import ResultRepository

logger = logging.getLogger(__name__)

_RESULT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS compare_results (
    row_id          INTEGER,
    status          VARCHAR,
    key_values      VARCHAR,
    left_data       VARCHAR,
    right_data      VARCHAR,
    diff_columns    VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_cr_status ON compare_results(status);
"""


class CompareWorker(QThread):
    """
    Background thread untuk menjalankan proses perbandingan data.
    
    Signals:
        progress(step_name, rows_done, total_rows)  - update progress bar
        log_message(text)                           - pesan log ke UI
        job_completed(job_id, summary)              - selesai berhasil
        job_failed(job_id, error_message)           - selesai dengan error
    """

    progress = Signal(str, int, int)        # step, done, total
    log_message = Signal(str)
    job_completed = Signal(str, dict)       # job_id, summary
    job_failed = Signal(str, str)           # job_id, error_message

    def __init__(
        self,
        job: CompareJob,
        config: CompareConfig,
        settings: AppSettings,
        job_manager: JobManager,
    ):
        super().__init__()
        self._job = job
        self._config = config
        self._settings = settings
        self._job_manager = job_manager
        self._cancelled = False

    def cancel(self):
        """Tandai untuk membatalkan proses."""
        self._cancelled = True

    # ------------------------------------------------------------------ run

    def run(self):
        """Entry point thread - jalankan seluruh proses perbandingan."""
        job_id = self._job.id
        job_dir = self._settings.jobs_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        job_db_path = str(job_dir / "data.duckdb")

        try:
            # Update status -> processing
            self._job_manager.update_status(job_id, JOB_STATUS_PROCESSING)
            self._emit_progress(STEP_INIT, 0, 0)
            self._log("Memulai proses perbandingan data...")

            # Buka koneksi DuckDB khusus untuk job ini
            conn = duckdb.connect(job_db_path)

            try:
                # Buat tabel hasil
                for stmt in _RESULT_TABLE_DDL.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(stmt)

                # Import data kiri dan kanan
                if self._job.job_type == JOB_TYPE_DB_VS_DB:
                    # DB vs DB: import paralel kedua sumber
                    left_rows, right_rows = self._import_db_vs_db_parallel(conn)
                else:
                    self._emit_progress(STEP_IMPORT_LEFT, 0, 0)
                    left_rows = self._import_source(
                        conn, "src_left", self._config.left_source, "kiri"
                    )
                    if self._cancelled:
                        raise InterruptedError("Proses dibatalkan oleh user.")

                    self._emit_progress(STEP_IMPORT_RIGHT, left_rows, 0)
                    right_rows = self._import_source(
                        conn, "src_right", self._config.right_source, "kanan"
                    )
                    if self._cancelled:
                        raise InterruptedError("Proses dibatalkan oleh user.")

                total_estimate = left_rows + right_rows
                self._log(
                    f"Data kiri: {left_rows:,} baris | Data kanan: {right_rows:,} baris"
                )

                # Jalankan compare engine
                self._emit_progress(STEP_COMPARE, 0, total_estimate)

                engine = CompareEngine(
                    conn=conn,
                    config=self._config,
                    progress_cb=lambda step, done, total: self._emit_progress(
                        step, done, total
                    ),
                )
                summary = engine.run()

                if self._cancelled:
                    raise InterruptedError("Proses dibatalkan oleh user.")

                # Tambahkan jumlah baris tiap sumber ke ringkasan
                summary["_left_rows"] = left_rows
                summary["_right_rows"] = right_rows

                # Simpan ringkasan
                self._emit_progress(STEP_SAVE_RESULT, 0, 0)
                self._job_manager.update_result_summary(job_id, summary)
                self._job_manager.update_status(job_id, JOB_STATUS_COMPLETED)

                self._emit_progress(STEP_DONE, summary["total_rows"], summary["total_rows"])
                self._log(
                    f"Perbandingan selesai! Total {summary['total_rows']:,} baris diproses."
                )
                _dup_cnt = summary.get('duplicate_key', 0)
                self._log(
                    f"  Cocok: {summary.get('match', 0):,} | "
                    f"Tidak cocok: {summary.get('mismatch', 0):,} | "
                    f"Hanya di kiri: {summary.get('missing_right', 0):,} | "
                    f"Hanya di kanan: {summary.get('missing_left', 0):,}"
                    + (f" | Key Duplikat: {_dup_cnt:,} (baris ini dikecualikan dari perbandingan)" if _dup_cnt > 0 else "")
                )

                # Tandai selesai; emit dilakukan SETELAH conn.close()
                # agar file data.duckdb pasti tidak terkunci saat UI
                # membuka ResultRepository (kritis di Windows)
                _completed_summary = summary

            finally:
                conn.close()

            # Emit di sini — koneksi DuckDB sudah ditutup
            self.job_completed.emit(job_id, _completed_summary)

        except InterruptedError as e:
            self._log(f"[DIBATALKAN] {e}")
            self._job_manager.update_status(job_id, JOB_STATUS_FAILED, str(e))
            self.job_failed.emit(job_id, str(e))
        except Exception as e:
            tb = traceback.format_exc()
            msg = f"Terjadi error: {e}"
            logger.error("CompareWorker error:\n%s", tb)
            self._log(f"[ERROR] {msg}")
            self._log(tb)
            self._job_manager.update_status(job_id, JOB_STATUS_FAILED, msg)
            self.job_failed.emit(job_id, msg)

    # ------------------------------------------------------------------ import helpers

    def _import_source(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        source: DataSourceConfig,
        side_label: str,
    ) -> int:
        """Import satu sumber data ke tabel DuckDB. Returns jumlah baris."""

        def _progress(rows: int):
            self._log(f"  [{side_label}] {rows:,} baris diimpor...")
            self._emit_progress(
                f"Mengimpor data {side_label}: {rows:,} baris",
                rows, 0
            )

        chunk = self._settings.import_chunk_size

        if source.source_type in ("excel",):
            from services.file_reader import ExcelReader
            reader = ExcelReader(source.file_path)
            return reader.import_to_duckdb(
                conn, table_name,
                sheet_name=source.sheet_name or 0,
                skip_rows=source.skip_rows,
                chunk_size=chunk,
                progress_callback=_progress,
            )

        elif source.source_type == "csv":
            from services.file_reader import CSVReader
            reader = CSVReader(
                source.file_path,
                separator=source.csv_separator,
                encoding=source.csv_encoding,
            )
            return reader.import_to_duckdb(
                conn, table_name,
                chunk_size=chunk,
                progress_callback=_progress,
            )

        elif source.source_type in ("postgres", "mysql"):
            profile = self._resolve_db_profile(source)

            if source.source_type == "mysql":
                from services.mysql_connector import MySQLConnector
                connector = MySQLConnector.from_profile(profile)
            else:
                from services.postgres_connector import PostgresConnector
                connector = PostgresConnector.from_profile(profile)

            try:
                return connector.import_to_duckdb(
                    conn, table_name,
                    schema=source.schema_name,
                    table=source.table_name,
                    custom_query=source.custom_query if source.use_custom_query else "",
                    chunk_size=chunk,
                    progress_callback=_progress,
                )
            finally:
                connector.close()
        else:
            raise ValueError(f"Tipe sumber data tidak dikenal: {source.source_type}")

    # ------------------------------------------------------------------ DB profile resolver

    def _resolve_db_profile(self, source):
        """Resolve profil koneksi DB dari connection_id atau inline dict."""
        from storage.connection_store import ConnectionStore
        from storage.duckdb_storage import DuckDBStorage
        from models.connection_profile import ConnectionProfile

        if source.connection_id:
            storage = DuckDBStorage(self._settings.db_path)
            cs = ConnectionStore(storage)
            profile = cs.get_by_id(source.connection_id)
            if not profile:
                raise ValueError(f"Profil koneksi tidak ditemukan: {source.connection_id}")
            return profile
        elif source.pg_connection_inline:
            return ConnectionProfile.from_dict(source.pg_connection_inline)
        else:
            raise ValueError(
                "Tidak ada informasi koneksi database. "
                "Isi detail koneksi atau pilih saved profile."
            )

    # ------------------------------------------------------------------ DB vs DB parallel import

    def _import_db_vs_db_parallel(self, conn: duckdb.DuckDBPyConnection):
        """
        Import kedua sumber DB secara paralel menggunakan ThreadPoolExecutor.
        Returns (left_rows, right_rows).
        """
        import concurrent.futures
        import threading

        # Progress counters (thread-safe via list)
        left_count = [0]
        right_count = [0]
        lock = threading.Lock()

        def _left_progress(rows):
            with lock:
                left_count[0] = rows
            self._log(f"  [kiri] {rows:,} baris diimpor...")
            self._emit_progress(f"Mengimpor DB kiri: {rows:,} baris", rows, 0)

        def _right_progress(rows):
            with lock:
                right_count[0] = rows
            self._log(f"  [kanan] {rows:,} baris diimpor...")
            self._emit_progress(f"Mengimpor DB kanan: {rows:,} baris", rows, 0)

        chunk = self._settings.import_chunk_size
        left_source = self._config.left_source
        right_source = self._config.right_source

        # DuckDB connections are NOT thread-safe; each thread gets its own temp DuckDB,
        # then we copy both tables into the main conn after threads finish.
        import tempfile, os
        job_id = self._job.id

        self._emit_progress(STEP_IMPORT_LEFT, 0, 0)
        self._log("Memulai impor paralel DB kiri dan DB kanan...")

        left_err = [None]
        right_err = [None]
        left_rows_result = [0]
        right_rows_result = [0]

        def import_left():
            try:
                with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
                    tmp_path = f.name
                try:
                    tmp_conn = duckdb.connect(tmp_path)
                    profile = self._resolve_db_profile(left_source)
                    if left_source.source_type == "mysql":
                        from services.mysql_connector import MySQLConnector
                        connector = MySQLConnector.from_profile(profile)
                    else:
                        from services.postgres_connector import PostgresConnector
                        connector = PostgresConnector.from_profile(profile)
                    try:
                        left_rows_result[0] = connector.import_to_duckdb(
                            tmp_conn, "src_left",
                            schema=left_source.schema_name,
                            table=left_source.table_name,
                            custom_query=left_source.custom_query if left_source.use_custom_query else "",
                            chunk_size=chunk,
                            progress_callback=_left_progress,
                        )
                    finally:
                        connector.close()
                    tmp_conn.close()
                    return tmp_path
                except Exception:
                    duckdb.connect(tmp_path).close()
                    os.unlink(tmp_path)
                    raise
            except Exception as e:
                left_err[0] = e
                return None

        def import_right():
            try:
                with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
                    tmp_path = f.name
                try:
                    tmp_conn = duckdb.connect(tmp_path)
                    profile = self._resolve_db_profile(right_source)
                    if right_source.source_type == "mysql":
                        from services.mysql_connector import MySQLConnector
                        connector = MySQLConnector.from_profile(profile)
                    else:
                        from services.postgres_connector import PostgresConnector
                        connector = PostgresConnector.from_profile(profile)
                    try:
                        right_rows_result[0] = connector.import_to_duckdb(
                            tmp_conn, "src_right",
                            schema=right_source.schema_name,
                            table=right_source.table_name,
                            custom_query=right_source.custom_query if right_source.use_custom_query else "",
                            chunk_size=chunk,
                            progress_callback=_right_progress,
                        )
                    finally:
                        connector.close()
                    tmp_conn.close()
                    return tmp_path
                except Exception:
                    duckdb.connect(tmp_path).close()
                    os.unlink(tmp_path)
                    raise
            except Exception as e:
                right_err[0] = e
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fut_left = executor.submit(import_left)
            fut_right = executor.submit(import_right)
            left_tmp = fut_left.result()
            right_tmp = fut_right.result()

        if left_err[0]:
            raise left_err[0]
        if right_err[0]:
            raise right_err[0]

        # Attach temp DuckDB files and copy tables into main conn
        try:
            conn.execute(f"ATTACH '{left_tmp}' AS tmp_left (READ_ONLY)")
            conn.execute("CREATE TABLE src_left AS SELECT * FROM tmp_left.src_left")
            conn.execute("DETACH tmp_left")
        finally:
            try:
                os.unlink(left_tmp)
            except Exception:
                pass

        try:
            conn.execute(f"ATTACH '{right_tmp}' AS tmp_right (READ_ONLY)")
            conn.execute("CREATE TABLE src_right AS SELECT * FROM tmp_right.src_right")
            conn.execute("DETACH tmp_right")
        finally:
            try:
                os.unlink(right_tmp)
            except Exception:
                pass

        self._log(
            f"Impor paralel selesai: kiri {left_rows_result[0]:,} baris, "
            f"kanan {right_rows_result[0]:,} baris"
        )
        return left_rows_result[0], right_rows_result[0]

    # ------------------------------------------------------------------ helpers

    def _emit_progress(self, step: str, done: int, total: int):
        self.progress.emit(step, done, total)

    def _log(self, message: str):
        self.log_message.emit(message)
