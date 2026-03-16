# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
services/mysql_connector.py
Service untuk koneksi dan query ke database MySQL.
Mendukung list schema/table, test koneksi, dan import data ke DuckDB.
"""

from __future__ import annotations
import logging
from typing import List, Tuple, Optional, Generator

import pandas as pd
import duckdb

logger = logging.getLogger(__name__)


class MySQLConnectionError(Exception):
    """Error koneksi atau query ke MySQL."""
    pass


class MySQLConnector:
    """
    Konektor MySQL menggunakan SQLAlchemy + PyMySQL.
    Mendukung koneksi via profil tersimpan maupun parameter langsung.
    Mendukung SSH Tunnel via sshtunnel.
    SSL: Disabled / Required / Verify CA.
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        ssl_mode: str = "disabled",   # "disabled" | "required" | "verify_ca"
        ssl_ca: str = "",             # path to CA cert (only for verify_ca)
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
        self._ssl_ca = ssl_ca
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
            raise MySQLConnectionError(
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
            try:
                from sqlalchemy import create_engine
                from sqlalchemy.engine import URL as _URL

                if self._use_ssh_tunnel:
                    local_port = self._start_ssh_tunnel()
                    db_host, db_port = "127.0.0.1", local_port
                else:
                    db_host, db_port = self._host, self._port

                connect_args: dict = {}
                if self._ssl_mode == "required":
                    connect_args["ssl"] = {"ssl_disabled": False}
                elif self._ssl_mode == "verify_ca" and self._ssl_ca:
                    connect_args["ssl"] = {"ca": self._ssl_ca}

                url = _URL.create(
                    drivername="mysql+pymysql",
                    username=self._username,
                    password=self._password,
                    host=db_host,
                    port=int(db_port),
                    database=self._database,
                )
                self._engine = create_engine(
                    url,
                    pool_pre_ping=True,
                    connect_args=connect_args,
                )
            except MySQLConnectionError:
                self._stop_tunnel_if_running()
                raise
            except ImportError:
                self._stop_tunnel_if_running()
                raise MySQLConnectionError(
                    "Library PyMySQL / SQLAlchemy tidak ditemukan. "
                    "Pastikan sudah terinstall: pip install PyMySQL"
                )
            except Exception as e:
                self._stop_tunnel_if_running()
                raise MySQLConnectionError(f"Gagal membuat koneksi: {e}") from e
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
        Test koneksi ke MySQL.
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
        """Daftar database MySQL yang tersedia (bukan system database)."""
        _system_dbs = {"information_schema", "performance_schema", "mysql", "sys"}
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                rows = conn.execute(text("SHOW DATABASES")).fetchall()
            return [r[0] for r in rows if r[0].lower() not in _system_dbs]
        except Exception as e:
            raise MySQLConnectionError(f"Gagal mengambil daftar database: {e}") from e

    def list_tables(self, schema: str = "") -> List[str]:
        """Daftar tabel dalam database MySQL tertentu."""
        db = schema or self._database
        if not db:
            raise MySQLConnectionError("Nama database harus diisi.")
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                rows = conn.execute(text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :db AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name"
                ), {"db": db}).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            raise MySQLConnectionError(f"Gagal mengambil daftar tabel: {e}") from e

    def get_columns(self, schema: str, table: str) -> List[str]:
        """Daftar nama kolom dari tabel tertentu."""
        db = schema or self._database
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text
                rows = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = :db AND table_name = :table "
                    "ORDER BY ordinal_position"
                ), {"db": db, "table": table}).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            raise MySQLConnectionError(f"Gagal mengambil kolom: {e}") from e

    # ------------------------------------------------------------------ data read

    def read_chunks(
        self,
        schema: str,
        table: str,
        custom_query: str = "",
        chunk_size: int = 10_000,
    ) -> Generator[pd.DataFrame, None, None]:
        """Generator baca data MySQL dalam chunks menggunakan SSCursor (streaming)."""
        try:
            import pymysql
            import pymysql.cursors

            if self._use_ssh_tunnel and self._tunnel is None:
                # Tunnel harus sudah dimulai via _get_engine(); panggil engine dulu
                self._get_engine()

            if self._use_ssh_tunnel:
                db_host = "127.0.0.1"
                db_port = self._tunnel.local_bind_port
            else:
                db_host = self._host
                db_port = self._port

            ssl_args: dict = {}
            if self._ssl_mode == "required":
                ssl_args["ssl"] = {}
            elif self._ssl_mode == "verify_ca" and self._ssl_ca:
                ssl_args["ssl"] = {"ca": self._ssl_ca}

            raw_conn = pymysql.connect(
                host=db_host,
                port=int(db_port),
                user=self._username,
                password=self._password,
                database=schema or self._database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.SSCursor,
                **ssl_args,
            )
            try:
                with raw_conn.cursor() as cursor:
                    if custom_query:
                        sql = f"SELECT * FROM ({custom_query}) AS _q"
                    else:
                        db = schema or self._database
                        sql = f"SELECT * FROM `{db}`.`{table}`"
                    cursor.execute(sql)
                    col_names = [desc[0] for desc in cursor.description]

                    while True:
                        rows = cursor.fetchmany(chunk_size)
                        if not rows:
                            break
                        yield pd.DataFrame(rows, columns=col_names, dtype=str)
            finally:
                raw_conn.close()
        except MySQLConnectionError:
            raise
        except ImportError:
            raise MySQLConnectionError(
                "Library PyMySQL tidak ditemukan. Install: pip install PyMySQL"
            )
        except Exception as e:
            raise MySQLConnectionError(f"Gagal membaca data: {e}") from e

    def import_to_duckdb(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        schema: str = "",
        table: str = "",
        custom_query: str = "",
        chunk_size: int = 10_000,
        progress_callback=None,
    ) -> int:
        """Import data MySQL ke tabel DuckDB. Returns jumlah baris."""
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

        logger.info("Impor MySQL selesai: %d baris ke %s", total_rows, table_name)
        return total_rows

    @classmethod
    def from_profile(cls, profile) -> "MySQLConnector":
        """Buat connector dari ConnectionProfile."""
        return cls(
            host=profile.host,
            port=profile.port,
            database=profile.database,
            username=profile.username,
            password=profile.password,
            ssl_mode=getattr(profile, "ssl_mode", "disabled"),
            ssl_ca=getattr(profile, "ssl_ca", ""),
            use_ssh_tunnel=getattr(profile, "use_ssh_tunnel", False),
            ssh_host=getattr(profile, "ssh_host", ""),
            ssh_port=getattr(profile, "ssh_port", 22),
            ssh_user=getattr(profile, "ssh_user", ""),
            ssh_auth_method=getattr(profile, "ssh_auth_method", "password"),
            ssh_password=getattr(profile, "ssh_password", ""),
            ssh_key_path=getattr(profile, "ssh_key_path", ""),
        )
