# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
ui/components/status_badge.py
Komponen badge/label status untuk job dan hasil perbandingan.
"""

from __future__ import annotations
from PySide6.QtWidgets import QLabel
from PySide6.QtCore import Qt

from config.constants import (
    JOB_STATUS_LABELS, RESULT_STATUS_LABELS,
)
from ui.styles import get_status_badge_style, get_job_status_badge_style


class StatusBadge(QLabel):
    """Badge status hasil perbandingan (Match, Mismatch, dll)."""

    def __init__(self, status: str, parent=None):
        label = RESULT_STATUS_LABELS.get(status, status)
        super().__init__(label, parent)
        self.setStyleSheet(get_status_badge_style(status))
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)

    def set_status(self, status: str):
        self.setText(RESULT_STATUS_LABELS.get(status, status))
        self.setStyleSheet(get_status_badge_style(status))


class JobStatusBadge(QLabel):
    """Badge status job (Antrian, Diproses, Selesai, Gagal)."""

    def __init__(self, status: str, parent=None):
        label = JOB_STATUS_LABELS.get(status, status)
        super().__init__(label, parent)
        self.setStyleSheet(get_job_status_badge_style(status))
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)

    def set_status(self, status: str):
        self.setText(JOB_STATUS_LABELS.get(status, status))
        self.setStyleSheet(get_job_status_badge_style(status))
