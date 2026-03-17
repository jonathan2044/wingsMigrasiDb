# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
models/template.py
Model data untuk Template perbandingan yang tersimpan.
"""

from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any


@dataclass
class CompareTemplate:
    """Konfigurasi perbandingan yang bisa disimpan dan dipakai ulang."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    job_type: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    use_count: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        import json
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "job_type": self.job_type,
            "config": json.dumps(self.config, ensure_ascii=False),
            "use_count": self.use_count,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompareTemplate":
        import json
        config = data.get("config")
        if isinstance(config, str):
            config = json.loads(config) if config else {}
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            description=data.get("description", ""),
            job_type=data.get("job_type", ""),
            config=config or {},
            use_count=data.get("use_count", 0),
            created_at=_parse_dt(data.get("created_at")),
            updated_at=_parse_dt(data.get("updated_at")),
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
