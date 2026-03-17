# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
storage/job_manager.py
CRUD operations untuk data Compare Job. Simpan, ambil, hapus — itu doang.
"""

from __future__ import annotations
import json
import logging
from datetime import datetime
from typing import List, Optional

from models.job import CompareJob
from storage.duckdb_storage import DuckDBStorage

logger = logging.getLogger(__name__)


class JobManager:
    """Kelola penyimpanan dan pengambilan data CompareJob. antiGalau CRUD."""

    def __init__(self, storage: DuckDBStorage):
        self._storage = storage

    # == write ==

    def save(self, job: CompareJob) -> None:
        """Simpan job baru atau update job yang sudah ada."""
        job.updated_at = datetime.now()
        existing = self.get_by_id(job.id)
        d = job.to_dict()

        if existing is None:
            self._storage.execute(
                """INSERT INTO jobs
                   (id, name, job_type, status, config, result_summary,
                    error_message, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    d["id"], d["name"], d["job_type"], d["status"],
                    d["config"], d["result_summary"], d["error_message"],
                    d["created_at"], d["updated_at"],
                ],
            )
            logger.debug("Job baru disimpan: %s", job.id)
        else:
            self._storage.execute(
                """UPDATE jobs SET
                   name=?, job_type=?, status=?, config=?,
                   result_summary=?, error_message=?, updated_at=?
                   WHERE id=?""",
                [
                    d["name"], d["job_type"], d["status"], d["config"],
                    d["result_summary"], d["error_message"], d["updated_at"],
                    d["id"],
                ],
            )
            logger.debug("Job diperbarui: %s", job.id)

    def update_status(self, job_id: str, status: str, error: str = None) -> None:
        self._storage.execute(
            "UPDATE jobs SET status=?, error_message=?, updated_at=? WHERE id=?",
            [status, error, datetime.now().isoformat(), job_id],
        )

    def update_result_summary(self, job_id: str, summary: dict) -> None:
        self._storage.execute(
            "UPDATE jobs SET result_summary=?, updated_at=? WHERE id=?",
            [json.dumps(summary), datetime.now().isoformat(), job_id],
        )

    def delete(self, job_id: str) -> None:
        self._storage.execute("DELETE FROM jobs WHERE id=?", [job_id])
        logger.debug("Job dihapus: %s", job_id)

    # == read ==

    def get_all(self, limit: int = 500) -> List[CompareJob]:
        rows = self._storage.fetchall(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", [limit]
        )
        return [self._row_to_job(r) for r in rows]

    def get_by_id(self, job_id: str) -> Optional[CompareJob]:
        row = self._storage.fetchone("SELECT * FROM jobs WHERE id=?", [job_id])
        return self._row_to_job(row) if row else None

    def get_recent(self, n: int = 5) -> List[CompareJob]:
        rows = self._storage.fetchall(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", [n]
        )
        return [self._row_to_job(r) for r in rows]

    def count(self) -> int:
        row = self._storage.fetchone("SELECT COUNT(*) FROM jobs")
        return row[0] if row else 0

    def count_completed(self) -> int:
        row = self._storage.fetchone(
            "SELECT COUNT(*) FROM jobs WHERE status='completed'"
        )
        return row[0] if row else 0

    def get_jobs_older_than(self, days: int) -> List["CompareJob"]:
        """Kembalikan daftar job yang dibuat lebih dari N hari yang lalu."""
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        rows = self._storage.fetchall(
            "SELECT * FROM jobs WHERE created_at < ? ORDER BY created_at ASC",
            [cutoff],
        )
        return [self._row_to_job(r) for r in rows]

    def delete_with_data(self, job_id: str, jobs_dir=None) -> None:
        """Hapus job dari database beserta folder data DuckDB-nya."""
        import shutil
        from pathlib import Path
        self.delete(job_id)
        if jobs_dir:
            job_dir = Path(jobs_dir) / job_id
            if job_dir.exists():
                shutil.rmtree(job_dir, ignore_errors=True)
                logger.debug("Folder job dihapus: %s", job_dir)
        logger.info("Job dan data berhasil dihapus: %s", job_id)

    # == helper ==

    def _row_to_job(self, row) -> CompareJob:
        keys = ["id", "name", "job_type", "status", "config",
                "result_summary", "error_message", "created_at", "updated_at"]
        return CompareJob.from_dict(dict(zip(keys, row)))
