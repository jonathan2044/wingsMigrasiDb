# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
exporters/csv_exporter.py
Export hasil komparasi ke CSV. Simpel.
"""

from __future__ import annotations
import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from config.constants import RESULT_STATUS_LABELS

logger = logging.getLogger(__name__)


class CSVExporter:
    """Export hasil perbandingan ke file CSV."""

    def __init__(self, output_path: str):
        self._path = Path(output_path)

    def export(
        self,
        records: List[Dict[str, Any]],
        summary: Optional[Dict[str, Any]] = None,
        job_name: str = "",
    ) -> str:
        """Export records ke CSV. Returns path file."""
        self._path.parent.mkdir(parents=True, exist_ok=True)

        with open(str(self._path), "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # Header
            writer.writerow([
                "No", "Status", "Key Values",
                "Data Kiri", "Data Kanan", "Kolom Berbeda"
            ])

            for i, rec in enumerate(records, 1):
                status = rec.get("status", "")
                key_str = "; ".join(
                    f"{k}={v}" for k, v in (rec.get("key_values") or {}).items()
                )
                left_str = json.dumps(rec.get("left_data") or {}, ensure_ascii=False)
                right_str = json.dumps(rec.get("right_data") or {}, ensure_ascii=False)
                diff_str = ", ".join(rec.get("diff_columns") or [])

                writer.writerow([
                    i,
                    RESULT_STATUS_LABELS.get(status, status),
                    key_str,
                    left_str,
                    right_str,
                    diff_str,
                ])

        logger.info("Export CSV selesai: %s", self._path)
        return str(self._path)
