# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
storage/duckdb_storage.py

Layer penyimpanan utama — ngandalin DuckDB buat metadata app.
Mengelola koneksi, schema (jobs, templates, connections), dan
sambungan per-job buat proses komparasi data djumbo gede.
"""

from __future__ import annotations
import logging
import threading
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)

# DDL schema untuk metadata aplikasi
_METADATA_SCHEMA = """
-- Tabel jobs
CREATE TABLE IF NOT EXISTS jobs (
    id              VARCHAR PRIMARY KEY,
    name            VARCHAR NOT NULL,
    job_type        VARCHAR NOT NULL,
    status          VARCHAR NOT NULL DEFAULT 'queued',
    config          TEXT,
    result_summary  TEXT,
    error_message   TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel templates konfigurasi
CREATE TABLE IF NOT EXISTS templates (
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    description TEXT,
    job_type    VARCHAR,
    config      TEXT,
    use_count   INTEGER DEFAULT 0,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel profil koneksi PostgreSQL/MySQL
CREATE TABLE IF NOT EXISTS connection_profiles (
    id               VARCHAR PRIMARY KEY,
    name             VARCHAR NOT NULL,
    db_type          VARCHAR DEFAULT 'postgresql',
    host             VARCHAR,
    port             INTEGER DEFAULT 5432,
    database         VARCHAR,
    username         VARCHAR,
    password         VARCHAR,
    ssl_mode         VARCHAR DEFAULT 'prefer',
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    use_ssh_tunnel   BOOLEAN DEFAULT FALSE,
    ssh_host         VARCHAR DEFAULT '',
    ssh_port         INTEGER DEFAULT 22,
    ssh_user         VARCHAR DEFAULT '',
    ssh_auth_method  VARCHAR DEFAULT 'password',
    ssh_password     VARCHAR DEFAULT '',
    ssh_key_path     VARCHAR DEFAULT ''
);
"""


class DuckDBStorage:
    """
    Pengelola koneksi DuckDB persisten buat metadata aplikasi.
    Thread-safe, pakai RLock biar gak deadlock kalau nested.
    """

    def __init__(self, db_path: Path):
        self._db_path = str(db_path)
        self._lock = threading.RLock()  # RLock biar nested acquire gak deadlock
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    # == lifecycle ==

    # Migration stmts — run ALTER TABLE ADD COLUMN IF NOT EXISTS for new columns
    _MIGRATIONS = [
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS use_ssh_tunnel  BOOLEAN DEFAULT FALSE",
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS ssh_host         VARCHAR DEFAULT ''",
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS ssh_port         INTEGER DEFAULT 22",
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS ssh_user         VARCHAR DEFAULT ''",
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS ssh_auth_method  VARCHAR DEFAULT 'password'",
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS ssh_password     VARCHAR DEFAULT ''",
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS ssh_key_path     VARCHAR DEFAULT ''",
        "ALTER TABLE connection_profiles ADD COLUMN IF NOT EXISTS db_type          VARCHAR DEFAULT 'postgresql'",
    ]

    def djumboInit(self):
        """Inisialisasi DB, buat schema kalau belum ada. Wajib dipanggil pas startup."""
        conn = self._djumboAmbilKoneksi()  # testing koneksi dulu sebelum eksekusi schema
        for stmt in _METADATA_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:  # DuckDB handles inline -- comments natively
                conn.execute(stmt)
        # Run migrations for new columns on existing DBs
        for mig in self._MIGRATIONS:
            try:
                conn.execute(mig)
            except Exception:
                pass  # kolom mungkin sudah ada di versi DB lama, skip aja
        logger.info("Database metadata siap: %s", self._db_path)

    def close(self):
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    # == koneksi ==

    def _djumboAmbilKoneksi(self) -> duckdb.DuckDBPyConnection:
        # buka koneksi kalau belum ada, thread-safe
        with self._lock:
            # kalau koneksi sudah ada pakai aja — jangan buka baru boros resource
            if self._conn is None:
                self._conn = duckdb.connect(self._db_path)
            return self._conn

    def execute(self, sql: str, params=None):
        """Jalankan SQL (INSERT/UPDATE/DELETE/DDL)."""
        with self._lock:
            conn = self._djumboAmbilKoneksi()
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    def executescript(self, sql: str):
        with self._lock:
            conn = self._djumboAmbilKoneksi()
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(stmt)

    def fetchall(self, sql: str, params=None) -> list:
        """Ambil semua baris hasil query."""
        with self._lock:
            conn = self._djumboAmbilKoneksi()
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            return result.fetchall()

    def fetchone(self, sql: str, params=None):
        """Ambil satu baris hasil query."""
        with self._lock:
            conn = self._djumboAmbilKoneksi()
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            return result.fetchone()

    def fetchdf(self, sql: str, params=None):
        """Ambil hasil query sebagai pandas DataFrame."""
        import pandas as pd
        with self._lock:
            conn = self._djumboAmbilKoneksi()
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            return result.df()

    def description(self, sql: str) -> list:
        """Ambil deskripsi kolom dari query."""
        with self._lock:
            conn = self._djumboAmbilKoneksi()
            result = conn.execute(sql)
            return result.description or []

    # == koneksi per job ==

    def djumboOpenJobDb(self, job_db_path: str) -> duckdb.DuckDBPyConnection:
        """
        Buka koneksi DuckDB terpisah khusus untuk satu job.
        Caller yang buka, caller juga yang harus tutup — jangan lupa ya.
        """
        return duckdb.connect(job_db_path)
