"""
models/connection_profile.py
Model data untuk profil koneksi PostgreSQL.
"""

from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional


@dataclass
class ConnectionProfile:
    """Profil koneksi ke database PostgreSQL."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""          # disimpan plain, enkripsi opsional
    ssl_mode: str = "prefer"
    created_at: datetime = field(default_factory=datetime.now)

    # SSH Tunnel fields
    use_ssh_tunnel: bool = False
    ssh_host: str = ""
    ssh_port: int = 22
    ssh_user: str = ""
    ssh_auth_method: str = "password"   # "password" | "key"
    ssh_password: str = ""
    ssh_key_path: str = ""

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.ssl_mode}"
        )

    @property
    def display_info(self) -> str:
        return f"{self.host}:{self.port}/{self.database} ({self.username})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "ssl_mode": self.ssl_mode,
            "created_at": self.created_at.isoformat(),
            "use_ssh_tunnel": self.use_ssh_tunnel,
            "ssh_host": self.ssh_host,
            "ssh_port": self.ssh_port,
            "ssh_user": self.ssh_user,
            "ssh_auth_method": self.ssh_auth_method,
            "ssh_password": self.ssh_password,
            "ssh_key_path": self.ssh_key_path,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConnectionProfile":
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            host=data.get("host", "localhost"),
            port=int(data.get("port", 5432)),
            database=data.get("database", ""),
            username=data.get("username", ""),
            password=data.get("password", ""),
            ssl_mode=data.get("ssl_mode", "prefer"),
            created_at=_parse_dt(data.get("created_at")),
            use_ssh_tunnel=bool(data.get("use_ssh_tunnel", False)),
            ssh_host=data.get("ssh_host", ""),
            ssh_port=int(data.get("ssh_port", 22)),
            ssh_user=data.get("ssh_user", ""),
            ssh_auth_method=data.get("ssh_auth_method", "password"),
            ssh_password=data.get("ssh_password", ""),
            ssh_key_path=data.get("ssh_key_path", ""),
        )


def _parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            pass
    return datetime.now()
