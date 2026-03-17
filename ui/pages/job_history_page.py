# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
ui/pages/job_history_page.py
Halaman Riwayat Job - daftar semua pekerjaan perbandingan.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QTableWidget, QTableWidgetItem, QHeaderView,
    QComboBox, QLineEdit, QMessageBox,
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor

from config.constants import (
    JOB_STATUS_LABELS, JOB_TYPE_LABELS,
    JOB_STATUS_COMPLETED, JOB_STATUS_FAILED,
    JOB_STATUS_QUEUED, JOB_STATUS_PROCESSING,
)
from ui.components.status_badge import JobStatusBadge
from ui.styles import (
    COLOR_TEXT, COLOR_TEXT_MUTED, COLOR_CARD_BG,
    COLOR_BORDER, COLOR_BG, COLOR_DANGER,
)

if TYPE_CHECKING:
    from storage.job_manager import JobManager


class JobHistoryPage(QWidget):
    """Daftar semua job perbandingan dengan filter status."""

    open_job = Signal(str)          # buka hasil job
    new_job = Signal()              # buka halaman job baru

    def __init__(self, job_manager: "JobManager", parent=None):
        super().__init__(parent)
        self._job_manager = job_manager
        self._jobs = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(28, 24, 28, 24)
        layout.setSpacing(16)

        # Header
        header_row = QHBoxLayout()
        title = QLabel("Riwayat Job")
        title.setObjectName("pageTitle")
        header_row.addWidget(title)
        header_row.addStretch()

        new_btn = QPushButton("+ Job Baru")
        new_btn.clicked.connect(self.new_job.emit)
        header_row.addWidget(new_btn)
        layout.addLayout(header_row)

        # Filter bar
        filter_frame = QFrame()
        filter_frame.setObjectName("card")
        filter_layout = QHBoxLayout(filter_frame)
        filter_layout.setContentsMargins(12, 10, 12, 10)

        filter_layout.addWidget(QLabel("Filter Status:"))
        self._status_filter = QComboBox()
        self._status_filter.addItem("Semua Status", "")
        for key, label in JOB_STATUS_LABELS.items():
            self._status_filter.addItem(label, key)
        self._status_filter.currentIndexChanged.connect(self._apply_filter)
        filter_layout.addWidget(self._status_filter)

        filter_layout.addWidget(QLabel("Cari:"))
        self._search = QLineEdit()
        self._search.setPlaceholderText("Nama job atau ID...")
        self._search.setMinimumWidth(200)
        self._search.textChanged.connect(self._apply_filter)
        filter_layout.addWidget(self._search)

        filter_layout.addStretch()

        refresh_btn = QPushButton("↻ Refresh")
        refresh_btn.setObjectName("secondaryBtn")
        refresh_btn.clicked.connect(self.refresh)
        filter_layout.addWidget(refresh_btn)
        layout.addWidget(filter_frame)

        # Tabel
        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels([
            "Status", "Nama Job", "Tipe", "Baris", "Match", "Mismatch", "Dibuat"
        ])
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.doubleClicked.connect(self._on_row_double_click)
        self._table.verticalHeader().setVisible(False)
        layout.addWidget(self._table, 1)

        # Footer
        self._footer_lbl = QLabel("")
        self._footer_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
        layout.addWidget(self._footer_lbl)

    # ------------------------------------------------------------------ public

    def refresh(self):
        self._jobs = self._job_manager.get_all()
        self._apply_filter()

    def _apply_filter(self):
        status_filter = self._status_filter.currentData()
        search = self._search.text().strip().lower()

        filtered = self._jobs
        if status_filter:
            filtered = [j for j in filtered if j.status == status_filter]
        if search:
            filtered = [
                j for j in filtered
                if search in j.name.lower() or search in j.id.lower()
            ]

        self._populate_table(filtered)

    def _populate_table(self, jobs):
        self._table.setRowCount(0)

        for job in jobs:
            row = self._table.rowCount()
            self._table.insertRow(row)

            # Status badge (label saja, bukan widget, agar lebih cepat)
            status_lbl = QTableWidgetItem(JOB_STATUS_LABELS.get(job.status, job.status))
            status_lbl.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self._color_status_item(status_lbl, job.status)
            self._table.setItem(row, 0, status_lbl)

            # Nama
            name_item = QTableWidgetItem(job.name or job.job_number)
            name_item.setData(Qt.ItemDataRole.UserRole, job.id)
            self._table.setItem(row, 1, name_item)

            # Tipe
            self._table.setItem(row, 2, QTableWidgetItem(
                JOB_TYPE_LABELS.get(job.job_type, job.job_type)
            ))

            # Statistik
            if job.result_summary:
                self._table.setItem(row, 3, QTableWidgetItem(f"{job.total_rows:,}"))
                self._table.setItem(row, 4, QTableWidgetItem(f"{job.match_pct}%"))
                self._table.setItem(row, 5, QTableWidgetItem(f"{job.mismatch_pct}%"))
            else:
                self._table.setItem(row, 3, QTableWidgetItem("—"))
                self._table.setItem(row, 4, QTableWidgetItem("—"))
                self._table.setItem(row, 5, QTableWidgetItem("—"))

            # Tanggal
            created = job.created_at.strftime("%d %b %Y %H:%M")
            self._table.setItem(row, 6, QTableWidgetItem(created))

            # Center align for numeric columns
            for col in [3, 4, 5, 6]:
                item = self._table.item(row, col)
                if item:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

        self._footer_lbl.setText(
            f"Total {len(jobs):,} job ditampilkan dari {len(self._jobs):,} job yang tersimpan."
        )

    def _color_status_item(self, item: QTableWidgetItem, status: str):
        color_map = {
            JOB_STATUS_COMPLETED:  ("#d1fae5", "#065f46"),
            JOB_STATUS_FAILED:     ("#fee2e2", "#991b1b"),
            JOB_STATUS_PROCESSING: ("#dbeafe", "#1e40af"),
            JOB_STATUS_QUEUED:     ("#fef3c7", "#92400e"),
        }
        if status in color_map:
            bg, fg = color_map[status]
            item.setBackground(QColor(bg))
            item.setForeground(QColor(fg))

    def _on_row_double_click(self, index):
        row = index.row()
        name_item = self._table.item(row, 1)
        if name_item:
            job_id = name_item.data(Qt.ItemDataRole.UserRole)
            if job_id:
                self.open_job.emit(job_id)
