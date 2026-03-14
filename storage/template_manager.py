# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
storage/template_manager.py
Kelola template konfigurasi perbandingan di database.
"""

from __future__ import annotations
import json
import logging
from datetime import datetime
from typing import List, Optional

from models.template import CompareTemplate
from storage.duckdb_storage import DuckDBStorage

logger = logging.getLogger(__name__)


class TemplateManager:
    """CRUD template konfigurasi perbandingan."""

    def __init__(self, storage: DuckDBStorage):
        self._storage = storage

    # ------------------------------------------------------------------ write

    def save(self, template: CompareTemplate) -> None:
        template.updated_at = datetime.now()
        d = template.to_dict()
        existing = self.get_by_id(template.id)

        if existing is None:
            self._storage.execute(
                """INSERT INTO templates
                   (id, name, description, job_type, config, use_count, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [d["id"], d["name"], d["description"], d["job_type"],
                 d["config"], d["use_count"], d["created_at"], d["updated_at"]],
            )
        else:
            self._storage.execute(
                """UPDATE templates SET name=?, description=?, job_type=?,
                   config=?, use_count=?, updated_at=? WHERE id=?""",
                [d["name"], d["description"], d["job_type"],
                 d["config"], d["use_count"], d["updated_at"], d["id"]],
            )

    def increment_use_count(self, template_id: str) -> None:
        self._storage.execute(
            "UPDATE templates SET use_count=use_count+1 WHERE id=?", [template_id]
        )

    def delete(self, template_id: str) -> None:
        self._storage.execute("DELETE FROM templates WHERE id=?", [template_id])

    # ------------------------------------------------------------------ read

    def get_all(self) -> List[CompareTemplate]:
        rows = self._storage.fetchall(
            "SELECT * FROM templates ORDER BY use_count DESC, name ASC"
        )
        return [self._row_to_template(r) for r in rows]

    def get_by_id(self, tid: str) -> Optional[CompareTemplate]:
        row = self._storage.fetchone(
            "SELECT * FROM templates WHERE id=?", [tid]
        )
        return self._row_to_template(row) if row else None

    # ------------------------------------------------------------------ helper

    def _row_to_template(self, row) -> CompareTemplate:
        keys = ["id", "name", "description", "job_type",
                "config", "use_count", "created_at", "updated_at"]
        return CompareTemplate.from_dict(dict(zip(keys, row)))
