# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
workers/compare_worker.py

Thread background buat proses komparasi data.
Pakai QThread supaya UI gak freeze — kalau dijalanin di main thread, user pasti ngamuk.
Integrasi semua komponen: file reader, konektor DB, DuckDB engine, dan result repository.
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
    Thread background buat komparasi data.
    
    Sinyal:
        progress(step_name, rows_done, total_rows)  - update progress bar
        log_message(text)                           - kirim log ke UI
        job_completed(job_id, summary)              - selesai tanpa error
        job_failed(job_id, error_message)           - selesai tapi gagal
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
        """Tandai untuk membatalkan proses — cek self._cancelled di tiap step."""
        self._cancelled = True

    # == entrypoint thread ==

    def run(self):
        """Dipanggil otomatis sama QThread — jangan panggil manual."""
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
                # optimalkan DuckDB buat dataset gede — jangan pelit resource
                import os as _os
                _cpu = max(2, (_os.cpu_count() or 4))
                conn.execute(f"PRAGMA threads={_cpu}")

                # memory limit: 70% RAM tersedia, minimal 2GB, maksimal 16GB
                # kalau pakai laptop kentang, ya sabar aja
                try:
                    import psutil as _psutil
                    _avail_mb = _psutil.virtual_memory().available // (1024 * 1024)
                except Exception:
                    _avail_mb = 4096  # default 4 GB jika psutil tidak tersedia
                _mem_mb = max(2048, min(int(_avail_mb * 0.70), 16 * 1024))
                conn.execute(f"PRAGMA memory_limit='{_mem_mb}MB'")

                # Buat tabel hasil
                for stmt in _RESULT_TABLE_DDL.strip().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(stmt)

                # import data kiri dan kanan
                if self._job.job_type == JOB_TYPE_DB_VS_DB:
                    # DB vs DB: import paralel keduanya sekaligus
                    left_rows, right_rows = self._djumboImportDBParallel(conn)
                else:
                    self._emit_progress(STEP_IMPORT_LEFT, 0, 0)
                    left_rows = self._djumboImportSumber(
                        conn, "src_left", self._config.left_source, "kiri"
                    )
                    if self._cancelled:
                        raise InterruptedError("Proses dibatalkan oleh user.")

                    self._emit_progress(STEP_IMPORT_RIGHT, left_rows, 0)
                    right_rows = self._djumboImportSumber(
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

                # Load global transform rules dari settings jika diaktifkan
                transform_rules = []
                if self._config.options.apply_global_transforms:
                    transform_rules = self._settings.get_transform_rules()
                    if transform_rules:
                        active = [r for r in transform_rules if r.enabled]
                        self._log(
                            f"Global transform rules: {len(active)} aktif dari "
                            f"{len(transform_rules)} rule terdaftar."
                        )

                # Load global group expansion rules jika diaktifkan
                group_expansion_rules = []
                if getattr(self._config.options, "apply_group_expansion", True):
                    group_expansion_rules = self._settings.get_group_expansion_rules()
                    if group_expansion_rules:
                        active_ge = [r for r in group_expansion_rules if r.enabled]
                        if active_ge:
                            descs = ", ".join(
                                f"{r.left_col}\u2192[{', '.join(r.right_cols[:2])}"
                                f"{'...' if len(r.right_cols) > 2 else ''}]"
                                f" ({r.total_mappings()} baris)"
                                for r in active_ge[:3]
                            )
                            self._log(
                                f"Group expansion rules: {len(active_ge)} aktif \u2014 {descs}"
                            )

                engine = CompareEngine(
                    conn=conn,
                    config=self._config,
                    progress_cb=lambda step, done, total: self._emit_progress(
                        step, done, total
                    ),
                    transform_rules=transform_rules,
                    group_expansion_rules=group_expansion_rules,
                )
                summary = engine.almaRun()

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

    # == helper import ==

    def _djumboImportSumber(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        source: DataSourceConfig,
        side_label: str,
    ) -> int:
        """Import satu sumber data ke tabel DuckDB. Return jumlah baris yang masuk."""

        def _progress(rows: int):
            self._log(f"  [{side_label}] {rows:,} baris diimpor...")
            self._emit_progress(
                f"Mengimpor data {side_label}: {rows:,} baris",
                rows, 0
            )

        chunk = self._settings.import_chunk_size
        # untuk njajal ukuran chunk — kalau OOM perkecil, kalau lambat perbesar
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

    # == DB profile resolver ==

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

    # == DB vs DB parallel import ==

    def _djumboImportDBParallel(self, conn: duckdb.DuckDBPyConnection):
        """
        Import dua sumber DB sekaligus secara paralel pakai ThreadPoolExecutor.
        Lebih cepet daripada satu-satu — apalagi kalau koneksi lambat.
        Return: (left_rows, right_rows).
        """
        import concurrent.futures
        import threading
        import uuid

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

        # testing thread import — semoga gak race condition, makanya pakai lock
        def import_left():
            # Use uuid path so DuckDB always creates a fresh database (NamedTemporaryFile
            # creates an empty 0-byte file that DuckDB rejects as "not a valid DuckDB db")
            tmp_path = os.path.join(tempfile.gettempdir(), f"sfa_{uuid.uuid4().hex}_L.duckdb")
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
            except Exception as e:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                left_err[0] = e
                return None

        def import_right():
            tmp_path = os.path.join(tempfile.gettempdir(), f"sfa_{uuid.uuid4().hex}_R.duckdb")
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
            except Exception as e:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
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

    # == helpers ==

    def _emit_progress(self, step: str, done: int, total: int):
        self.progress.emit(step, done, total)

    def _log(self, message: str):
        self.log_message.emit(message)
