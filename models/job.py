# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
models/job.py
Model data untuk Compare Job.
"""

from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any

from config.constants import JOB_STATUS_QUEUED


@dataclass
class CompareJob:
    """Representasi satu pekerjaan perbandingan data."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    job_type: str = ""          # 'file_vs_file' | 'file_vs_pg'
    status: str = JOB_STATUS_QUEUED
    config: Dict[str, Any] = field(default_factory=dict)
    result_summary: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    # ------------------------------------------------------------------ helpers

    @property
    def job_number(self) -> str:
        """Nomor job yang ditampilkan ke user, contoh: JOB-001"""
        return f"JOB-{self.id[:8].upper()[:8]}"

    @property
    def total_rows(self) -> int:
        if self.result_summary:
            return self.result_summary.get("total_rows", 0)
        return 0

    @property
    def match_pct(self) -> float:
        if self.result_summary:
            t = self.result_summary.get("total_rows", 0)
            m = self.result_summary.get("match", 0)
            return round(m / t * 100, 1) if t else 0.0
        return 0.0

    @property
    def mismatch_pct(self) -> float:
        if self.result_summary:
            t = self.result_summary.get("total_rows", 0)
            mm = self.result_summary.get("mismatch", 0)
            return round(mm / t * 100, 1) if t else 0.0
        return 0.0

    @property
    def missing_pct(self) -> float:
        if self.result_summary:
            t = self.result_summary.get("total_rows", 0)
            ml = self.result_summary.get("missing_left", 0)
            mr = self.result_summary.get("missing_right", 0)
            return round((ml + mr) / t * 100, 1) if t else 0.0
        return 0.0

    @property
    def duration_str(self) -> str:
        """Durasi proses dari created_at ke updated_at, format '1m 23s'."""
        secs = max(0, int((self.updated_at - self.created_at).total_seconds()))
        if secs < 60:
            return f"{secs}s"
        m, s = divmod(secs, 60)
        if m < 60:
            return f"{m}m {s}s"
        h, m = divmod(m, 60)
        return f"{h}h {m}m"

    @property
    def completed_at_str(self) -> str:
        return self.updated_at.strftime("%d %b %Y %H:%M:%S")

    @property
    def time_ago_str(self) -> str:
        """Waktu relatif sejak job selesai, misal '2 min ago'."""
        secs = max(0, int((datetime.now() - self.updated_at).total_seconds()))
        if secs < 60:
            return "baru saja"
        m = secs // 60
        if m < 60:
            return f"{m} min ago"
        h = m // 60
        if h < 24:
            return f"{h} hr ago"
        return self.updated_at.strftime("%d %b %Y")

    def to_dict(self) -> Dict[str, Any]:
        import json
        return {
            "id": self.id,
            "name": self.name,
            "job_type": self.job_type,
            "status": self.status,
            "config": json.dumps(self.config, ensure_ascii=False),
            "result_summary": json.dumps(self.result_summary, ensure_ascii=False) if self.result_summary else None,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompareJob":
        import json
        config = data.get("config")
        if isinstance(config, str):
            config = json.loads(config) if config else {}

        result_summary = data.get("result_summary")
        if isinstance(result_summary, str):
            result_summary = json.loads(result_summary) if result_summary else None

        return cls(
            id=data["id"],
            name=data.get("name", ""),
            job_type=data.get("job_type", ""),
            status=data.get("status", JOB_STATUS_QUEUED),
            config=config or {},
            result_summary=result_summary,
            error_message=data.get("error_message"),
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
