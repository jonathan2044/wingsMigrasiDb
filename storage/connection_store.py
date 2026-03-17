# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
storage/connection_store.py
Kelola profil koneksi PostgreSQL/MySQL. CRUD biasa, gak ada yang spesial.
"""

from __future__ import annotations
import logging
from typing import List, Optional

from models.connection_profile import ConnectionProfile
from storage.duckdb_storage import DuckDBStorage

logger = logging.getLogger(__name__)


class ConnectionStore:
    """CRUD profil koneksi PostgreSQL/MySQL."""

    def __init__(self, storage: DuckDBStorage):
        self._storage = storage

    # == write ==

    def save(self, profile: ConnectionProfile) -> None:
        d = profile.to_dict()
        existing = self.get_by_id(profile.id)

        if existing is None:
            self._storage.execute(
                """INSERT INTO connection_profiles
                   (id, name, db_type, host, port, database, username, password, ssl_mode, created_at,
                    use_ssh_tunnel, ssh_host, ssh_port, ssh_user, ssh_auth_method, ssh_password, ssh_key_path)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [d["id"], d["name"], d.get("db_type", "postgresql"),
                 d["host"], d["port"], d["database"],
                 d["username"], d["password"], d["ssl_mode"], d["created_at"],
                 d["use_ssh_tunnel"], d["ssh_host"], d["ssh_port"], d["ssh_user"],
                 d["ssh_auth_method"], d["ssh_password"], d["ssh_key_path"]],
            )
        else:
            self._storage.execute(
                """UPDATE connection_profiles
                   SET name=?, db_type=?, host=?, port=?, database=?, username=?, password=?, ssl_mode=?,
                       use_ssh_tunnel=?, ssh_host=?, ssh_port=?, ssh_user=?,
                       ssh_auth_method=?, ssh_password=?, ssh_key_path=?
                   WHERE id=?""",
                [d["name"], d.get("db_type", "postgresql"),
                 d["host"], d["port"], d["database"],
                 d["username"], d["password"], d["ssl_mode"],
                 d["use_ssh_tunnel"], d["ssh_host"], d["ssh_port"], d["ssh_user"],
                 d["ssh_auth_method"], d["ssh_password"], d["ssh_key_path"], d["id"]],
            )

    def delete(self, profile_id: str) -> None:
        self._storage.execute(
            "DELETE FROM connection_profiles WHERE id=?", [profile_id]
        )

    # == read ==

    def get_all(self) -> List[ConnectionProfile]:
        rows = self._storage.fetchall(
            "SELECT * FROM connection_profiles ORDER BY name ASC"
        )
        return [self._row_to_profile(r) for r in rows]

    def get_by_id(self, pid: str) -> Optional[ConnectionProfile]:
        row = self._storage.fetchone(
            "SELECT * FROM connection_profiles WHERE id=?", [pid]
        )
        return self._row_to_profile(row) if row else None

    # == helper ==

    def _row_to_profile(self, row) -> ConnectionProfile:
        keys = ["id", "name", "db_type", "host", "port", "database",
                "username", "password", "ssl_mode", "created_at",
                "use_ssh_tunnel", "ssh_host", "ssh_port", "ssh_user",
                "ssh_auth_method", "ssh_password", "ssh_key_path"]
        # row may have fewer or more columns depending on DB version
        d = dict(zip(keys, row))
        d.setdefault("db_type", "postgresql")
        d.setdefault("use_ssh_tunnel", False)
        d.setdefault("ssh_host", "")
        d.setdefault("ssh_port", 22)
        d.setdefault("ssh_user", "")
        d.setdefault("ssh_auth_method", "password")
        d.setdefault("ssh_password", "")
        d.setdefault("ssh_key_path", "")
        return ConnectionProfile.from_dict(d)
