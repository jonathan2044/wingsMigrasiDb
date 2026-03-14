"""
ui/pages/dashboard_page.py
Halaman Dashboard - ringkasan statistik dan daftar job terbaru.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QFrame, QScrollArea, QGridLayout,
    QSizePolicy,
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont

from config.constants import (
    JOB_STATUS_LABELS, JOB_TYPE_LABELS,
    JOB_TYPE_FILE_VS_FILE, JOB_TYPE_FILE_VS_PG,
)
from ui.components.status_badge import JobStatusBadge
from ui.styles import (
    COLOR_PRIMARY, COLOR_TEXT, COLOR_TEXT_MUTED, COLOR_CARD_BG,
    COLOR_BORDER, COLOR_BG, COLOR_SUCCESS, COLOR_INFO, COLOR_WARNING,
)

if TYPE_CHECKING:
    from storage.job_manager import JobManager


class StatCard(QFrame):
    """Kartu statistik kecil di bagian atas dashboard."""

    def __init__(
        self,
        title: str,
        value: str,
        subtitle: str = "",
        icon: str = "",
        accent_color: str = COLOR_PRIMARY,
        parent=None,
    ):
        super().__init__(parent)
        self.setObjectName("statCard")
        self.setMinimumWidth(180)
        layout = QVBoxLayout(self)
        layout.setSpacing(4)

        top = QHBoxLayout()
        lbl_title = QLabel(title)
        lbl_title.setObjectName("statLabel")
        lbl_title.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px; font-weight: 500;")
        top.addWidget(lbl_title)
        top.addStretch()

        icon_lbl = QLabel(icon)
        icon_lbl.setStyleSheet(f"font-size: 20px; color: {accent_color};")
        top.addWidget(icon_lbl)
        layout.addLayout(top)

        self.value_lbl = QLabel(value)
        self.value_lbl.setObjectName("statValue")
        self.value_lbl.setStyleSheet(
            f"font-size: 28px; font-weight: bold; color: {COLOR_TEXT};"
        )
        layout.addWidget(self.value_lbl)

        if subtitle:
            sub = QLabel(subtitle)
            sub.setObjectName("statSub")
            sub.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
            layout.addWidget(sub)

    def update_value(self, value: str, subtitle: str = ""):
        self.value_lbl.setText(value)


class JobRowWidget(QFrame):
    """Satu baris item job di daftar recent jobs."""

    clicked = Signal(str)   # job_id

    def __init__(self, job, parent=None):
        super().__init__(parent)
        self.job_id = job.id
        self.setObjectName("card")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setStyleSheet(
            f"QFrame#card {{ background: {COLOR_CARD_BG}; border: 1px solid {COLOR_BORDER}; "
            f"border-radius: 8px; }} "
            f"QFrame#card:hover {{ border-color: {COLOR_PRIMARY}; }}"
        )

        layout = QHBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(12)

        # Badge status
        status_badge = JobStatusBadge(job.status)
        status_badge.setFixedWidth(90)
        layout.addWidget(status_badge)

        # Info job
        info = QVBoxLayout()
        info.setSpacing(2)
        name_lbl = QLabel(job.name or job.job_number)
        name_lbl.setStyleSheet("font-weight: 600; font-size: 13px;")
        info.addWidget(name_lbl)

        type_label = JOB_TYPE_LABELS.get(job.job_type, job.job_type)
        from datetime import datetime
        created = job.created_at
        now = datetime.now()
        delta = now - created
        if delta.total_seconds() < 60:
            time_str = "Baru saja"
        elif delta.total_seconds() < 3600:
            time_str = f"{int(delta.total_seconds() // 60)} menit lalu"
        elif delta.total_seconds() < 86400:
            time_str = f"{int(delta.total_seconds() // 3600)} jam lalu"
        else:
            time_str = created.strftime("%d %b %Y")

        meta_lbl = QLabel(f"{type_label}  ·  {job.job_number}  ·  {time_str}")
        meta_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
        info.addWidget(meta_lbl)
        layout.addLayout(info)

        layout.addStretch()

        # Statistik hasil (bila ada)
        if job.result_summary:
            stats = QHBoxLayout()
            stats.setSpacing(16)

            match_pct = job.match_pct
            mismatch_pct = job.mismatch_pct
            missing_pct = job.missing_pct

            self._add_stat(stats, f"{match_pct}% cocok", COLOR_SUCCESS)
            if mismatch_pct > 0:
                self._add_stat(stats, f"{mismatch_pct}% tdk cocok", "#ef4444")
            if missing_pct > 0:
                self._add_stat(stats, f"{missing_pct}% hilang", "#f97316")

            rows_lbl = QLabel(f"{job.total_rows:,} baris")
            rows_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
            stats.addWidget(rows_lbl)
            layout.addLayout(stats)

        # Tombol detail
        detail_btn = QPushButton("›")
        detail_btn.setObjectName("secondaryBtn")
        detail_btn.setFixedSize(28, 28)
        detail_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        detail_btn.clicked.connect(lambda: self.clicked.emit(self.job_id))
        layout.addWidget(detail_btn)

    def _add_stat(self, layout, text: str, color: str):
        lbl = QLabel(text)
        lbl.setStyleSheet(f"color: {color}; font-size: 11px; font-weight: 600;")
        layout.addWidget(lbl)

    def mousePressEvent(self, event):
        self.clicked.emit(self.job_id)
        super().mousePressEvent(event)


class DashboardPage(QWidget):
    """Halaman utama dashboard aplikasi."""

    navigate_to = Signal(str)           # page key
    open_job = Signal(str)              # job id

    def __init__(self, job_manager: "JobManager", parent=None):
        super().__init__(parent)
        self._job_manager = job_manager
        self._setup_ui()

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Scrollable content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setStyleSheet("background: transparent;")

        content = QWidget()
        content.setStyleSheet("background: transparent;")
        layout = QVBoxLayout(content)
        layout.setContentsMargins(28, 24, 28, 24)
        layout.setSpacing(24)

        # ---- Page header ----
        header = QVBoxLayout()
        title = QLabel("Dashboard")
        title.setObjectName("pageTitle")
        subtitle = QLabel("Ringkasan semua pekerjaan perbandingan Anda")
        subtitle.setObjectName("pageSubtitle")
        header.addWidget(title)
        header.addWidget(subtitle)
        layout.addLayout(header)

        # ---- Stat cards ----
        self._stat_total = StatCard("Total Job", "0", "Sepanjang waktu", "⚡", COLOR_PRIMARY)
        self._stat_done = StatCard("Selesai", "0", "89.5% tingkat sukses", "✓", COLOR_SUCCESS)
        self._stat_rows = StatCard("Baris Diproses", "0", "Bulan ini", "📄", COLOR_INFO)
        self._stat_mismatch = StatCard("Rata-rata Mismatch", "0%", "10 job terakhir", "⚠", COLOR_WARNING)

        cards_layout = QHBoxLayout()
        cards_layout.setSpacing(16)
        for card in [self._stat_total, self._stat_done, self._stat_rows, self._stat_mismatch]:
            card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            cards_layout.addWidget(card)
        layout.addLayout(cards_layout)

        # ---- Bottom row: Recent Jobs + Quick Start + Templates ----
        bottom = QHBoxLayout()
        bottom.setSpacing(16)

        # Recent jobs (2/3 lebar)
        self._jobs_frame = self._build_recent_jobs_panel()
        bottom.addWidget(self._jobs_frame, 2)

        # Quick start + Templates (1/3 lebar)
        right_col = QVBoxLayout()
        right_col.setSpacing(16)
        right_col.addWidget(self._build_quick_start_panel())
        right_col.addWidget(self._build_templates_panel())
        right_widget = QWidget()
        right_widget.setStyleSheet("background: transparent;")
        right_widget.setLayout(right_col)
        bottom.addWidget(right_widget, 1)

        layout.addLayout(bottom)
        layout.addStretch()

        scroll.setWidget(content)
        root.addWidget(scroll)

    # ------------------------------------------------------------------ panels

    def _build_recent_jobs_panel(self) -> QFrame:
        frame = QFrame()
        frame.setObjectName("card")
        layout = QVBoxLayout(frame)
        layout.setSpacing(12)

        header = QHBoxLayout()
        title = QLabel("Job Terbaru")
        title.setObjectName("sectionHeader")
        header.addWidget(title)
        header.addStretch()
        view_all = QPushButton("Lihat Semua ›")
        view_all.setObjectName("secondaryBtn")
        view_all.setCursor(Qt.CursorShape.PointingHandCursor)
        view_all.clicked.connect(lambda: self.navigate_to.emit("job_history"))
        header.addWidget(view_all)
        layout.addLayout(header)

        self._jobs_container = QVBoxLayout()
        self._jobs_container.setSpacing(6)
        layout.addLayout(self._jobs_container)

        self._no_jobs_lbl = QLabel("Belum ada job yang dibuat.\nKlik '+ Job Baru' untuk memulai.")
        self._no_jobs_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        self._no_jobs_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._no_jobs_lbl)

        return frame

    def _build_quick_start_panel(self) -> QFrame:
        frame = QFrame()
        frame.setObjectName("card")
        layout = QVBoxLayout(frame)
        layout.setSpacing(8)

        title = QLabel("Shortcut")
        title.setObjectName("sectionHeader")
        layout.addWidget(title)

        btn_ff = QPushButton("📄  File vs File\nExcel / CSV")
        btn_ff.setObjectName("quickStartBtn")
        btn_ff.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_ff.clicked.connect(lambda: self._quick_start(JOB_TYPE_FILE_VS_FILE))
        layout.addWidget(btn_ff)

        btn_fp = QPushButton("🗄  File vs PostgreSQL\nFile dibandingkan tabel database")
        btn_fp.setObjectName("quickStartBtn")
        btn_fp.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_fp.clicked.connect(lambda: self._quick_start(JOB_TYPE_FILE_VS_PG))
        layout.addWidget(btn_fp)

        return frame

    def _build_templates_panel(self) -> QFrame:
        frame = QFrame()
        frame.setObjectName("card")
        layout = QVBoxLayout(frame)
        layout.setSpacing(8)

        header = QHBoxLayout()
        title = QLabel("Template Tersimpan")
        title.setObjectName("sectionHeader")
        header.addWidget(title)
        header.addStretch()
        see_all = QPushButton("Semua")
        see_all.setObjectName("secondaryBtn")
        see_all.setCursor(Qt.CursorShape.PointingHandCursor)
        see_all.clicked.connect(lambda: self.navigate_to.emit("templates"))
        header.addWidget(see_all)
        layout.addLayout(header)

        self._templates_container = QVBoxLayout()
        self._templates_container.setSpacing(4)
        layout.addLayout(self._templates_container)

        return frame

    # ------------------------------------------------------------------ refresh

    def refresh(self):
        """Refresh semua data di dashboard."""
        self._refresh_stats()
        self._refresh_recent_jobs()
        self._refresh_templates()

    def _refresh_stats(self):
        total = self._job_manager.count()
        done = self._job_manager.count_completed()
        pct = f"{done/total*100:.1f}%" if total else "0%"

        self._stat_total.update_value(str(total))
        self._stat_done.update_value(str(done), f"{pct} tingkat sukses")

        # Hitung total rows dari recent jobs
        recent = self._job_manager.get_recent(50)
        total_rows = sum(j.total_rows for j in recent)
        if total_rows >= 1_000_000:
            rows_str = f"{total_rows/1_000_000:.1f}M"
        elif total_rows >= 1_000:
            rows_str = f"{total_rows/1_000:.1f}K"
        else:
            rows_str = str(total_rows)
        self._stat_rows.update_value(rows_str)

        # Rata-rata mismatch
        last10 = [j for j in recent if j.result_summary][:10]
        if last10:
            avg_mm = sum(j.mismatch_pct for j in last10) / len(last10)
            self._stat_mismatch.update_value(f"{avg_mm:.1f}%")

    def _refresh_recent_jobs(self):
        # Hapus widget lama
        while self._jobs_container.count():
            item = self._jobs_container.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        jobs = self._job_manager.get_recent(5)
        if jobs:
            self._no_jobs_lbl.hide()
            for job in jobs:
                row = JobRowWidget(job)
                row.clicked.connect(self._on_job_clicked)
                self._jobs_container.addWidget(row)
        else:
            self._no_jobs_lbl.show()

    def _refresh_templates(self):
        while self._templates_container.count():
            item = self._templates_container.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        # Di-render oleh MainWindow setelah template manager tersedia
        # Untuk sementara tampilkan placeholder
        lbl = QLabel("Belum ada template tersimpan.")
        lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
        self._templates_container.addWidget(lbl)

    def update_templates(self, templates: list):
        """Update daftar template dari luar (dipanggil MainWindow)."""
        while self._templates_container.count():
            item = self._templates_container.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        if not templates:
            lbl = QLabel("Belum ada template tersimpan.")
            lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
            self._templates_container.addWidget(lbl)
            return

        for tmpl in templates[:5]:  # max 5 di panel ini
            row = QHBoxLayout()
            icon = "📋"
            name_lbl = QLabel(f"{icon}  {tmpl.name}")
            name_lbl.setStyleSheet("font-size: 12px; font-weight: 600;")

            type_lbl = QLabel(
                f"{JOB_TYPE_LABELS.get(tmpl.job_type, tmpl.job_type)}  ·  "
                f"dipakai {tmpl.use_count}×"
            )
            type_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 10px;")

            info_col = QVBoxLayout()
            info_col.setSpacing(0)
            info_col.addWidget(name_lbl)
            info_col.addWidget(type_lbl)

            row.addLayout(info_col)
            row.addStretch()

            container = QWidget()
            container.setStyleSheet(
                f"background: transparent; padding: 4px 0;"
            )
            container.setLayout(row)
            self._templates_container.addWidget(container)

    def _quick_start(self, job_type: str):
        self.navigate_to.emit(f"new_job?type={job_type}")

    def _on_job_clicked(self, job_id: str):
        self.open_job.emit(job_id)
