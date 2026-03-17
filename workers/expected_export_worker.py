# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
workers/expected_export_worker.py

Background QThread buat generate & export file ekspektasi migrasi.
Dijalankan di luar UI thread biar gak freeze — rule #1 QThread.
"""

from __future__ import annotations

import logging
import traceback
from typing import List, Optional

from PySide6.QtCore import QThread, Signal

from config.settings import AppSettings
from models.job import CompareJob
from models.compare_config import CompareConfig, ColumnTransformRule, GroupExpansionRule

logger = logging.getLogger(__name__)


class ExpectedExportWorker(QThread):
    """
    Worker thread untuk proses generate & export ekspektasi migrasi.

    Signals:
        progress(step, rows_done, total)    — update progress di UI
        export_done(output_path, row_count) — export berhasil
        failed(error_message)               — export gagal atau dibatalkan
                                              (string kosong = dibatalkan user)
    """

    progress    = Signal(str, int, int)   # step, done, total
    export_done = Signal(str, int)        # output_path, row_count
    failed      = Signal(str)             # error_message (kosong = cancelled)

    def __init__(
        self,
        job: CompareJob,
        config: CompareConfig,
        settings: AppSettings,
        output_path: str,
        fmt: str,
        transform_rules: Optional[List[ColumnTransformRule]] = None,
        group_expansion_rules: Optional[List[GroupExpansionRule]] = None,
        parent=None,
    ):
        super().__init__(parent)
        self._job      = job
        self._config   = config
        self._settings = settings
        self._out_path = output_path
        self._fmt      = fmt          # 'csv' atau 'xlsx'
        self._tx_rules = transform_rules or []
        self._ge_rules = group_expansion_rules or []
        self._cancelled = False

    def cancel(self):
        """Tandai untuk membatalkan export."""
        self._cancelled = True

    # ──────────────────────────────────────────────────────────────── run

    def run(self):
        from core.expected_generator import ExpectedMigrationGenerator

        generator = ExpectedMigrationGenerator(
            config=self._config,
            transform_rules=self._tx_rules,
            group_expansion_rules=self._ge_rules,
            progress_cb=lambda step, done, total: self.progress.emit(step, done, total),
            cancel_cb=lambda: self._cancelled,
        )

        try:
            count = generator.generate(
                output_path=self._out_path,
                fmt=self._fmt,
                settings=self._settings,
            )
            self.export_done.emit(self._out_path, count)

        except InterruptedError:
            # Dibatalkan user — tidak perlu menampilkan pesan error
            self.failed.emit("")

        except Exception as e:
            logger.error(
                "[ExpectedExportWorker] Error saat export:\n%s",
                traceback.format_exc(),
            )
            self.failed.emit(str(e))
