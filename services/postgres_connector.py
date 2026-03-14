"""
services/postgres_connector.py
Service untuk koneksi dan query ke database PostgreSQL.
Mendukung list schema/table, test koneksi, dan import data ke DuckDB.
"""

from __future__ import annotations
import logging
from typing import List, Tuple, Optional, Generator

import pandas as pd
import duckdb

logger = logging.getLogger(__name__)


class PostgresConnectionError(Exception):
    """Error koneksi atau query ke PostgreSQL."""
    pass


class PostgresConnector:
    """
    Konektor PostgreSQL menggunakan SQLAlchemy + psycopg2.
    Mendukung koneksi via profil tersimpan maupun parameter langsung.
    Mendukung SSH Tunnel via sshtunnel.
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        ssl_mode: str = "prefer",
        # SSH Tunnel params
        use_ssh_tunnel: bool = False,
        ssh_host: str = "",
        ssh_port: int = 22,
        ssh_user: str = "",
        ssh_auth_method: str = "password",
        ssh_password: str = "",
        ssh_key_path: str = "",
    ):
        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password
        self._ssl_mode = ssl_mode
        self._use_ssh_tunnel = use_ssh_tunnel
        self._ssh_host = ssh_host
        self._ssh_port = ssh_port
        self._ssh_user = ssh_user
        self._ssh_auth_method = ssh_auth_method
        self._ssh_password = ssh_password
        self._ssh_key_path = ssh_key_path
        self._engine = None
        self._tunnel = None

    # ------------------------------------------------------------------ connection

    def _start_ssh_tunnel(self) -> int:
        """Mulai SSH tunnel, return local bind port."""
        try:
            from sshtunnel import SSHTunnelForwarder
        except ImportError:
            raise PostgresConnectionError(
                "Library sshtunnel tidak ditemukan. Install dengan: pip install sshtunnel"
            )
        kwargs: dict = {
            "ssh_address_or_host": (self._ssh_host, self._ssh_port),
            "ssh_username": self._ssh_user,
            "remote_bind_address": (self._host, self._port),
        }
        if self._ssh_auth_method == "key":
            kwargs["ssh_pkey"] = self._ssh_key_path
        else:
            kwargs["ssh_password"] = self._ssh_password
        self._tunnel = SSHTunnelForwarder(**kwargs)
        self._tunnel.start()
        return self._tunnel.local_bind_port

    def _get_engine(self):
        if self._engine is None:
            tunnel_started = False
            try:
                from sqlalchemy import create_engine
                from sqlalchemy.engine import URL as _URL

                if self._use_ssh_tunnel:
                    local_port = self._start_ssh_tunnel()
                    tunnel_started = True
                    db_host, db_port = "127.0.0.1", local_port
                else:
                    db_host, db_port = self._host, self._port

                # Use URL.create() so special chars in user/password are handled safely
                url = _URL.create(
                    drivername="postgresql+psycopg2",
                    username=self._username,
                    password=self._password,
                    host=db_host,
                    port=int(db_port),
                    database=self._database,
                    query={"sslmode": self._ssl_mode},
                )
                self._engine = create_engine(url, pool_pre_ping=True)
            except PostgresConnectionError:
                self._stop_tunnel_if_running()
                raise
            except ImportError:
                self._stop_tunnel_if_running()
                raise PostgresConnectionError(
                    "Library psycopg2 / SQLAlchemy tidak ditemukan. "
                    "Pastikan sudah terinstall."
                )
            except Exception as e:
                self._stop_tunnel_if_running()
                raise PostgresConnectionError(f"Gagal membuat koneksi: {e}") from e
        return self._engine

    def _stop_tunnel_if_running(self):
        if self._tunnel:
            try:
                self._tunnel.stop()
            except Exception:
                pass
            self._tunnel = None

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test koneksi ke PostgreSQL.
        Returns (sukses, pesan).
        """
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("SELECT 1"))
            return True, "Koneksi berhasil!"
        except Exception as e:
            return False, f"Koneksi gagal: {e}"

    def close(self):
        if self._engine:
            try:
                self._engine.dispose()
            except Exception:
                pass
            self._engine = None
        self._stop_tunnel_if_running()

    # ------------------------------------------------------------------ schema/table

    def list_schemas(self) -> List[str]:
        """Daftar schema yang tersedia (bukan system schema)."""
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                rows = conn.execute(text(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast') "
                    "ORDER BY schema_name"
                )).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            raise PostgresConnectionError(f"Gagal mengambil daftar schema: {e}") from e

    def list_tables(self, schema: str = "public") -> List[str]:
        """Daftar tabel dalam schema tertentu."""
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                rows = conn.execute(text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :schema AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name"
                ), {"schema": schema}).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            raise PostgresConnectionError(f"Gagal mengambil daftar tabel: {e}") from e

    def get_columns(self, schema: str, table: str) -> List[str]:
        """Daftar nama kolom dari tabel tertentu."""
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                rows = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table "
                    "ORDER BY ordinal_position"
                ), {"schema": schema, "table": table}).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            raise PostgresConnectionError(f"Gagal mengambil kolom: {e}") from e

    # ------------------------------------------------------------------ data read

    def read_chunks(
        self,
        schema: str,
        table: str,
        custom_query: str = "",
        chunk_size: int = 10_000,
    ) -> Generator[pd.DataFrame, None, None]:
        """Generator baca data PostgreSQL dalam chunks."""
        try:
            engine = self._get_engine()
            if custom_query:
                sql = f"SELECT * FROM ({custom_query}) AS _q"
            else:
                sql = f'SELECT * FROM "{schema}"."{table}"'

            for chunk in pd.read_sql(sql, engine, chunksize=chunk_size, dtype=str):
                yield chunk
        except Exception as e:
            raise PostgresConnectionError(f"Gagal membaca data: {e}") from e

    def import_to_duckdb(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        schema: str = "public",
        table: str = "",
        custom_query: str = "",
        chunk_size: int = 10_000,
        progress_callback=None,
    ) -> int:
        """Import data PostgreSQL ke tabel DuckDB. Returns jumlah baris."""
        from services.file_reader import _sanitize_col

        total_rows = 0
        first_chunk = True

        for chunk in self.read_chunks(schema, table, custom_query, chunk_size):
            if chunk.empty:
                continue
            chunk.columns = [_sanitize_col(c) for c in chunk.columns]

            if first_chunk:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM chunk WHERE 1=0"
                )
                first_chunk = False

            conn.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
            total_rows += len(chunk)

            if progress_callback:
                progress_callback(total_rows)

        logger.info("Impor PostgreSQL selesai: %d baris ke %s", total_rows, table_name)
        return total_rows

    @classmethod
    def from_profile(cls, profile) -> "PostgresConnector":
        """Buat connector dari ConnectionProfile."""
        return cls(
            host=profile.host,
            port=profile.port,
            database=profile.database,
            username=profile.username,
            password=profile.password,
            ssl_mode=profile.ssl_mode,
            use_ssh_tunnel=getattr(profile, "use_ssh_tunnel", False),
            ssh_host=getattr(profile, "ssh_host", ""),
            ssh_port=getattr(profile, "ssh_port", 22),
            ssh_user=getattr(profile, "ssh_user", ""),
            ssh_auth_method=getattr(profile, "ssh_auth_method", "password"),
            ssh_password=getattr(profile, "ssh_password", ""),
            ssh_key_path=getattr(profile, "ssh_key_path", ""),
        )
