# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
storage/duckdb_storage.py
Layer penyimpanan utama menggunakan DuckDB.
Mengelola koneksi, schema metadata (jobs, templates, connections),
dan staging tables per job untuk proses perbandingan data besar.
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
    Pengelola koneksi DuckDB persisten untuk metadata aplikasi.
    Thread-safe dengan connection pooling sederhana.
    """

    def __init__(self, db_path: Path):
        self._db_path = str(db_path)
        self._lock = threading.RLock()  # RLock agar tidak deadlock saat nested acquire
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    # ------------------------------------------------------------------ lifecycle

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

    def initialize(self):
        """Inisialisasi database, buat schema bila belum ada."""
        conn = self._get_conn()
        for stmt in _METADATA_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:  # DuckDB handles inline -- comments natively
                conn.execute(stmt)
        # Run migrations for new columns on existing DBs
        for mig in self._MIGRATIONS:
            try:
                conn.execute(mig)
            except Exception:
                pass  # column may already exist in older DuckDB versions
        logger.info("Database metadata diinisialisasi: %s", self._db_path)

    def close(self):
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    # ------------------------------------------------------------------ connection

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            if self._conn is None:
                self._conn = duckdb.connect(self._db_path)
            return self._conn

    def execute(self, sql: str, params=None):
        """Jalankan perintah SQL (INSERT/UPDATE/DELETE/DDL)."""
        with self._lock:
            conn = self._get_conn()
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    def executescript(self, sql: str):
        with self._lock:
            conn = self._get_conn()
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:  # DuckDB handles inline -- comments natively
                    conn.execute(stmt)

    def fetchall(self, sql: str, params=None) -> list:
        """Ambil semua baris hasil query."""
        with self._lock:
            conn = self._get_conn()
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            return result.fetchall()

    def fetchone(self, sql: str, params=None):
        """Ambil satu baris hasil query."""
        with self._lock:
            conn = self._get_conn()
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            return result.fetchone()

    def fetchdf(self, sql: str, params=None):
        """Ambil hasil query sebagai pandas DataFrame."""
        import pandas as pd
        with self._lock:
            conn = self._get_conn()
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)
            return result.df()

    def description(self, sql: str) -> list:
        """Ambil deskripsi kolom dari query."""
        with self._lock:
            conn = self._get_conn()
            result = conn.execute(sql)
            return result.description or []

    # ------------------------------------------------------------------ job-scoped connection

    def open_job_db(self, job_db_path: str) -> duckdb.DuckDBPyConnection:
        """
        Buka koneksi DuckDB terpisah untuk data spesifik satu job.
        Caller bertanggung jawab menutup koneksi ini setelah selesai.
        """
        return duckdb.connect(job_db_path)
