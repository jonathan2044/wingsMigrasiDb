# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
ui/pages/new_job_page.py

Halaman buat job perbandingan baru — wizard 4 langkah:
  Step 1 — Select Source   : pilih mode + nama job + load template
  Step 2 — Import Files    : upload file kiri/kanan (atau setup koneksi PostgreSQL)
  Step 3 — Column Mapping  : mapping kolom + toggle Key / Compare
  Step 4 — Options & Run   : opsi normalisasi + job summary + run button
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, List, Optional, Tuple

from PySide6.QtCore import Qt, Signal, QAbstractTableModel, QModelIndex
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QLineEdit, QComboBox, QCheckBox, QFileDialog,
    QScrollArea, QTableWidget, QTableWidgetItem, QHeaderView,
    QTextEdit, QSpinBox, QStackedWidget,
    QAbstractItemView, QTableView,
)


class _NoScrollComboBox(QComboBox):
    """QComboBox yang mengabaikan wheel event saat tidak dalam fokus.
    Mencegah nilai berubah ketika user scroll halaman tanpa klik terlebih dulu.
    """
    def wheelEvent(self, event):
        # Hanya proses wheel jika combo sedang aktif (popup terbuka / hasFocus)
        if self.view().isVisible():
            super().wheelEvent(event)
        else:
            event.ignore()

from config.constants import (
    JOB_TYPE_FILE_VS_FILE, JOB_TYPE_FILE_VS_PG, JOB_TYPE_DB_VS_DB,
    JOB_STATUS_QUEUED,
)
from models.job import CompareJob
from models.compare_config import (
    CompareConfig, DataSourceConfig, CompareOptions, ColumnMapping,
)
from ui.styles import (
    COLOR_PRIMARY, COLOR_PRIMARY_DARK, COLOR_PRIMARY_LIGHT,
    COLOR_TEXT, COLOR_TEXT_MUTED, COLOR_TEXT_LIGHT,
    COLOR_CARD_BG, COLOR_BORDER, COLOR_BG,
    COLOR_SUCCESS, COLOR_SUCCESS_BG,
    COLOR_DANGER, COLOR_DANGER_BG,
    COLOR_WARNING, COLOR_WARNING_BG,
    COLOR_PURPLE, COLOR_PURPLE_BG,
    msg_info, msg_warning, msg_critical, msg_question,
)

if TYPE_CHECKING:
    from config.settings import AppSettings
    from storage.job_manager import JobManager
    from storage.connection_store import ConnectionStore
    from storage.template_manager import TemplateManager


# ──────────────────────────────────────────────────────────────────────────────
# Shared tiny widgets
# ──────────────────────────────────────────────────────────────────────────────

class _Divider(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.HLine)
        self.setStyleSheet(f"color: {COLOR_BORDER}; margin: 0;")
        self.setFixedHeight(1)


class _PandasTableModel(QAbstractTableModel):
    """Model virtual berbasis pandas DataFrame.
    Qt hanya meminta data untuk baris yang terlihat, sehingga tetap cepat
    meski DataFrame berisi ratusan ribu baris.
    """

    def __init__(self, df, parent=None):
        super().__init__(parent)
        self._df = df

    def rowCount(self, parent=QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        return len(self._df.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.ItemDataRole.DisplayRole:
            try:
                val = self._df.iloc[index.row(), index.column()]
                if val is None:
                    return "\u2014"
                s = str(val)
                return "\u2014" if s in ("nan", "None", "") else s
            except Exception:
                return "\u2014"
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            try:
                return str(self._df.columns[section])
            except Exception:
                return str(section)
        return str(section + 1)


class _Card(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("card")
        self.setStyleSheet(
            f"QFrame#card {{ background: {COLOR_CARD_BG}; border: 1px solid {COLOR_BORDER}; "
            "border-radius: 10px; }}"
        )


class _InfoBanner(QFrame):
    def __init__(self, text: str, color: str = COLOR_WARNING, bg: str = COLOR_WARNING_BG,
                 parent=None):
        super().__init__(parent)
        self.setStyleSheet(
            f"background: {bg}; border: 1px solid {color}; border-radius: 8px;"
        )
        hl = QHBoxLayout(self)
        hl.setContentsMargins(14, 10, 14, 10)
        hl.setSpacing(10)
        icon = QLabel("\u26a0")
        icon.setStyleSheet(f"color: {color}; font-size: 15px; background: transparent;")
        self._lbl = QLabel(text)
        self._lbl.setWordWrap(True)
        self._lbl.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 13px; background: transparent;")
        hl.addWidget(icon)
        hl.addWidget(self._lbl, 1)

    def set_text(self, text: str):
        self._lbl.setText(text)


class _TagBadge(QLabel):
    def __init__(self, text: str, parent=None):
        super().__init__(text, parent)
        self.setStyleSheet(
            f"background: {COLOR_BG}; color: {COLOR_TEXT_MUTED}; "
            "border: 1px solid #cbd5e1; border-radius: 4px; "
            "font-size: 11px; padding: 2px 7px;"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar step indicator
# ──────────────────────────────────────────────────────────────────────────────

class _SideStepIndicator(QWidget):
    STEPS = ["Pilih Source", "Import File &\nsetting Koneksi", "Pemetaan Kolom", "Opsi", "Jalankan &\nAnda akan bahagia"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(200)
        self._current = 0
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        header = QLabel("STEP2 COMPARASI")
        header.setStyleSheet(
            f"color: {COLOR_TEXT_MUTED}; font-size: 10px; font-weight: 700; "
            "letter-spacing: 1px; padding: 20px 20px 12px 20px;"
        )
        layout.addWidget(header)
        self._rows: list = []
        for i, name in enumerate(self.STEPS):
            row = self._make_row(i, name)
            self._rows.append(row)
            layout.addWidget(row)
        layout.addStretch()

    def _make_row(self, idx: int, name: str) -> QWidget:
        row = QWidget()
        row.setObjectName(f"stepRow_{idx}")
        hl = QHBoxLayout(row)
        hl.setContentsMargins(20, 8, 12, 8)
        hl.setSpacing(10)
        circle = QLabel()
        circle.setFixedSize(20, 20)
        circle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        circle.setObjectName(f"stepCircle_{idx}")
        lbl = QLabel(name)
        lbl.setWordWrap(True)
        lbl.setObjectName(f"stepLabel_{idx}")
        lbl.setStyleSheet(f"font-size: 13px; color: {COLOR_TEXT_MUTED};")
        hl.addWidget(circle)
        hl.addWidget(lbl)
        hl.addStretch()
        return row

    def set_step(self, current: int):
        self._current = current
        for i, row in enumerate(self._rows):
            circle = row.findChild(QLabel, f"stepCircle_{i}")
            lbl    = row.findChild(QLabel, f"stepLabel_{i}")
            if circle is None or lbl is None:
                continue
            if i < current:
                circle.setStyleSheet(
                    f"background-color: {COLOR_PRIMARY}; color: white; "
                    "border-radius: 10px; font-size: 12px; font-weight: 700;"
                )
                circle.setText("\u2713")
                lbl.setStyleSheet(f"font-size: 13px; color: {COLOR_TEXT_MUTED};")
            elif i == current:
                circle.setStyleSheet(
                    f"background-color: {COLOR_PRIMARY}; color: white; "
                    "border-radius: 10px; font-size: 11px; font-weight: 700;"
                )
                circle.setText(str(i + 1))
                lbl.setStyleSheet(
                    f"font-size: 13px; color: {COLOR_TEXT}; font-weight: 600;"
                )
            else:
                circle.setStyleSheet(
                    f"background-color: transparent; color: {COLOR_TEXT_MUTED}; "
                    f"border: 2px solid {COLOR_BORDER}; border-radius: 10px; font-size: 11px;"
                )
                circle.setText(str(i + 1))
                lbl.setStyleSheet(f"font-size: 13px; color: {COLOR_TEXT_MUTED};")


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — Select Source
# ──────────────────────────────────────────────────────────────────────────────

class _Step1SelectSource(QWidget):
    mode_changed = Signal(str)

    def __init__(self, template_manager: "Optional[TemplateManager]" = None, parent=None):
        super().__init__(parent)
        self._template_manager = template_manager
        self._job_type = JOB_TYPE_FILE_VS_FILE
        self._setup_ui()

    def _setup_ui(self):
        vl = QVBoxLayout(self)
        vl.setContentsMargins(36, 28, 36, 20)
        vl.setSpacing(0)

        bc = QLabel("Dashboard  \u203a  Job Baru")
        bc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        vl.addWidget(bc)
        vl.addSpacing(4)

        title = QLabel("Langkah 1 \u2014 Pilih Mode Perbandingan")
        title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 20px; font-weight: 700;")
        vl.addWidget(title)
        vl.addSpacing(24)

        sub = QLabel(
            "Pilih dulu mode perbandingan datanya. Bisa File vs File, "
            "atau File vs tabel di PostgreSQL."
        )
        sub.setWordWrap(True)
        sub.setAlignment(Qt.AlignmentFlag.AlignCenter)
        sub.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px;")
        vl.addWidget(sub)
        vl.addSpacing(20)

        card_row = QHBoxLayout()
        card_row.setSpacing(18)
        self._card_file = self._make_mode_card(
            icons=["\U0001f5cb", "\u21c4", "\U0001f5cb"],
            icon_color=COLOR_PRIMARY,
            title="File vs File",
            desc="Bandingkan dua file Excel (.xlsx) atau CSV secara berdampingan. Kalau pakai Excel, bisa pilih sheet-nya juga. kalau CSV belum bisa pilih sheet guys. uangel ternyata. T_T",
            tags=[".xlsx", ".csv"],
            job_type=JOB_TYPE_FILE_VS_FILE,
        )
        self._card_pg = self._make_mode_card(
            icons=["\U0001f5cb", "\u21c4", "\U0001f5c3"],
            icon_color=COLOR_PURPLE,
            title="File vs Database",
            desc="Bandingkan file Excel atau CSV dengan tabel atau custom query di database PostgreSQL.",
            tags=[".xlsx", ".csv", "PostgreSQL"],
            job_type=JOB_TYPE_FILE_VS_PG,
        )
        self._card_db = self._make_mode_card(
            icons=["\U0001f5c3", "\u21c4", "\U0001f5c3"],
            icon_color="#16a34a",
            title="Database vs Database",
            desc="Bandingkan dua tabel atau query langsung antara dua database (PostgreSQL atau MySQL). Cocok untuk data skala besar jutaan baris.",
            tags=["PostgreSQL", "MySQL"],
            job_type=JOB_TYPE_DB_VS_DB,
        )
        card_row.addWidget(self._card_file, 1)
        card_row.addWidget(self._card_pg, 1)
        card_row.addWidget(self._card_db, 1)
        vl.addLayout(card_row)
        vl.addSpacing(24)

        # Job Name card
        name_card = _Card()
        nl = QVBoxLayout(name_card)
        nl.setContentsMargins(20, 18, 20, 18)
        nl.setSpacing(8)
        name_hdr = QHBoxLayout()
        lbl_name = QLabel("Nama Job")
        lbl_name.setStyleSheet(f"font-weight: 600; color: {COLOR_TEXT}; font-size: 13px;")
        star = QLabel("*")
        star.setStyleSheet(f"color: {COLOR_DANGER}; font-size: 13px;")
        name_hdr.addWidget(lbl_name)
        name_hdr.addWidget(star)
        name_hdr.addStretch()
        nl.addLayout(name_hdr)
        self._job_name = QLineEdit()
        self._job_name.setPlaceholderText("pda_sampling-m_parameter")
        self._job_name.setMinimumHeight(38)
        nl.addWidget(self._job_name)
        hint = QLabel("Kasih nama yang gampang diingat biar gampang dicari nanti di riwayat. jangan salah lagi ya om! wkwk")
        hint.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        nl.addWidget(hint)
        vl.addWidget(name_card)
        vl.addSpacing(14)

        # Template card
        tmpl_card = _Card()
        tl = QVBoxLayout(tmpl_card)
        tl.setContentsMargins(20, 18, 20, 18)
        tl.setSpacing(8)
        tmpl_title = QLabel("Pakai Template Tersimpan")
        tmpl_title.setStyleSheet(f"font-weight: 600; color: {COLOR_TEXT}; font-size: 13px;")
        tl.addWidget(tmpl_title)
        tmpl_sub = QLabel("Skip konfigurasi — tinggal pilih template yang pernah disimpan sebelumnya.")
        tmpl_sub.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        tl.addWidget(tmpl_sub)
        self._template_combo = _NoScrollComboBox()
        self._template_combo.setMinimumHeight(38)
        self._refresh_templates()
        tl.addWidget(self._template_combo)
        vl.addWidget(tmpl_card)
        vl.addStretch()

        self._select_mode(JOB_TYPE_FILE_VS_FILE)

    def _make_mode_card(self, icons, icon_color, title, desc, tags, job_type) -> QFrame:
        card = QFrame()
        card.setCursor(Qt.CursorShape.PointingHandCursor)
        card.setProperty("job_type", job_type)
        card.setObjectName("modeCard")
        vl = QVBoxLayout(card)
        vl.setContentsMargins(22, 20, 22, 20)
        vl.setSpacing(10)
        icon_row = QHBoxLayout()
        icon_row.setSpacing(6)
        for txt in icons:
            ic = QLabel(txt)
            ic.setAlignment(Qt.AlignmentFlag.AlignCenter)
            ic.setFixedSize(34, 34)
            ic.setStyleSheet(
                f"background: {COLOR_PRIMARY_LIGHT}; color: {icon_color}; "
                "border-radius: 8px; font-size: 16px;"
            )
            icon_row.addWidget(ic)
        icon_row.addStretch()
        vl.addLayout(icon_row)
        title_lbl = QLabel(title)
        title_lbl.setStyleSheet(f"font-size: 15px; font-weight: 700; color: {COLOR_TEXT};")
        vl.addWidget(title_lbl)
        desc_lbl = QLabel(desc)
        desc_lbl.setWordWrap(True)
        desc_lbl.setStyleSheet(f"font-size: 13px; color: {COLOR_TEXT_MUTED};")
        vl.addWidget(desc_lbl)
        tag_row = QHBoxLayout()
        tag_row.setSpacing(6)
        for t in tags:
            tag_row.addWidget(_TagBadge(t))
        tag_row.addStretch()
        vl.addLayout(tag_row)
        card.mousePressEvent = lambda e, jt=job_type: self._select_mode(jt)
        return card

    def _select_mode(self, job_type: str):
        self._job_type = job_type
        selected_style = (
            f"QFrame#modeCard {{ background: white; border: 2px solid {COLOR_PRIMARY}; "
            "border-radius: 10px; }}"
        )
        default_style = (
            "QFrame#modeCard { background: white; border: 2px solid #e2e8f0; "
            "border-radius: 10px; }"
        )
        self._card_file.setStyleSheet(selected_style if job_type == JOB_TYPE_FILE_VS_FILE else default_style)
        self._card_pg.setStyleSheet(selected_style if job_type == JOB_TYPE_FILE_VS_PG else default_style)
        self._card_db.setStyleSheet(selected_style if job_type == JOB_TYPE_DB_VS_DB else default_style)
        self.mode_changed.emit(job_type)

    def _refresh_templates(self):
        self._template_combo.clear()
        self._template_combo.addItem("\u2014 No template (start fresh) \u2014", None)
        if self._template_manager:
            try:
                for t in self._template_manager.get_all():
                    self._template_combo.addItem(t.name, t.id)
            except Exception:
                pass

    def get_job_name(self) -> str:
        return self._job_name.text().strip()

    def get_job_type(self) -> str:
        return self._job_type

    def get_template_id(self) -> "Optional[str]":
        return self._template_combo.currentData()

    def set_job_type(self, jt: str):
        self._select_mode(jt)


# ──────────────────────────────────────────────────────────────────────────────
# Step 2a — File source card
# ──────────────────────────────────────────────────────────────────────────────

class _FileSourceCard(QFrame):
    headers_loaded = Signal(list)

    def __init__(self, side: str, color: str, parent=None):
        super().__init__(parent)
        self._side = side
        self._color = color
        self._file_path = ""
        self._sheets: List[str] = []
        self._headers: List[str] = []
        self._row_count = 0
        self._setup_ui()

    def _setup_ui(self):
        self.setObjectName("fileCard")
        self.setStyleSheet(
            "QFrame#fileCard { background: white; border: 1px solid #e2e8f0; border-radius: 10px; }"
        )
        vl = QVBoxLayout(self)
        vl.setContentsMargins(16, 14, 16, 16)
        vl.setSpacing(12)

        # Header row
        header_row = QHBoxLayout()
        badge = QLabel(f" {self._side} ")
        badge.setStyleSheet(
            f"background: {self._color}; color: white; border-radius: 4px; "
            "font-size: 12px; font-weight: 700; padding: 2px 8px;"
        )
        label = QLabel(f"Source {'Kiri' if self._side == 'L' else 'Kanan'} (File)")
        label.setStyleSheet(f"font-size: 14px; font-weight: 600; color: {COLOR_TEXT};")
        header_row.addWidget(badge)
        header_row.addWidget(label)
        header_row.addStretch()
        self._remove_btn = QPushButton("\u2715 Remove")
        self._remove_btn.setStyleSheet(
            f"background: transparent; color: {COLOR_DANGER}; border: none; font-size: 12px; padding: 0;"
        )
        header_row.addWidget(self._remove_btn)
        vl.addLayout(header_row)

        # Drop zone
        self._drop_zone = QFrame()
        self._drop_zone.setStyleSheet(
            "QFrame { border: 2px dashed #cbd5e1; border-radius: 8px; "
            f"background: {COLOR_BG}; min-height: 70px; }}"
        )
        dz_vl = QVBoxLayout(self._drop_zone)
        dz_vl.setContentsMargins(12, 12, 12, 12)
        dz_vl.setSpacing(4)
        self._file_icon = QLabel("\U0001f5cb")
        self._file_icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._file_icon.setStyleSheet(f"font-size: 28px; color: {COLOR_TEXT_MUTED}; border: none;")
        self._file_name_lbl = QLabel("Drop file disini atau klik Browse File dibawah")
        self._file_name_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._file_name_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px; border: none;")
        self._file_meta_lbl = QLabel("")
        self._file_meta_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._file_meta_lbl.setStyleSheet(f"color: {COLOR_TEXT_LIGHT}; font-size: 12px; border: none;")
        self._change_file_btn = QPushButton("\u2191 Ganti file")
        self._change_file_btn.setStyleSheet(
            f"background: transparent; color: {COLOR_TEXT_MUTED}; border: none; "
            "font-size: 12px; text-decoration: underline; padding: 0;"
        )
        self._change_file_btn.hide()
        self._change_file_btn.clicked.connect(self._browse_file)
        dz_vl.addWidget(self._file_icon)
        dz_vl.addWidget(self._file_name_lbl)
        dz_vl.addWidget(self._file_meta_lbl)
        dz_vl.addWidget(self._change_file_btn)
        self._drop_zone.mousePressEvent = lambda e: self._browse_file()
        vl.addWidget(self._drop_zone)

        # Sheet selector
        self._sheet_section = QWidget()
        sh_vl = QVBoxLayout(self._sheet_section)
        sh_vl.setContentsMargins(0, 0, 0, 0)
        sh_vl.setSpacing(6)
        sh_lbl = QLabel("\u229e  Select Sheet")
        sh_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px; font-weight: 600;")
        sh_vl.addWidget(sh_lbl)
        self._sheet_tab_row = QHBoxLayout()
        self._sheet_tab_row.setSpacing(6)
        self._sheet_tab_row.setContentsMargins(0, 0, 0, 0)
        sheet_tab_w = QWidget()
        sheet_tab_w.setLayout(self._sheet_tab_row)
        sh_vl.addWidget(sheet_tab_w)
        self._sheet_info_lbl = QLabel("")
        self._sheet_info_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
        sh_vl.addWidget(self._sheet_info_lbl)
        self._sheet_section.hide()
        vl.addWidget(self._sheet_section)

        # Preview table
        self._preview_section = QWidget()
        pv_vl = QVBoxLayout(self._preview_section)
        pv_vl.setContentsMargins(0, 0, 0, 0)
        pv_vl.setSpacing(6)
        self._preview_title = QLabel("")
        self._preview_title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 13px; font-weight: 600;")
        pv_vl.addWidget(self._preview_title)
        self._preview_table = QTableView()
        self._preview_table.setMinimumHeight(300)
        self._preview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._preview_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self._preview_table.verticalHeader().setDefaultSectionSize(24)
        self._preview_table.setAlternatingRowColors(True)
        self._preview_table.setStyleSheet(
            "QTableView { border: 1px solid #e2e8f0; border-radius: 6px; font-size: 12px; }"
        )
        pv_vl.addWidget(self._preview_table)
        self._preview_footer = QLabel("")
        self._preview_footer.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._preview_footer.setStyleSheet(f"color: {COLOR_TEXT_LIGHT}; font-size: 11px; font-style: italic;")
        pv_vl.addWidget(self._preview_footer)
        self._preview_section.hide()
        vl.addWidget(self._preview_section)

        # Bottom row: file type + browse
        bottom_row = QHBoxLayout()
        self._file_type_combo = _NoScrollComboBox()
        self._file_type_combo.addItems(["CSV (.csv)", "Excel (.xlsx/.xls)"])
        self._file_type_combo.setFixedWidth(160)
        self._file_type_combo.setFixedHeight(34)
        self._file_type_combo.currentIndexChanged.connect(self._on_file_type_changed)
        self._browse_btn = QPushButton("\U0001f4c2  Browse File")
        self._browse_btn.setObjectName("secondaryBtn")
        self._browse_btn.setFixedHeight(34)
        self._browse_btn.clicked.connect(self._browse_file)
        bottom_row.addWidget(QLabel("Format:"))
        bottom_row.addWidget(self._file_type_combo)
        bottom_row.addSpacing(8)
        bottom_row.addWidget(self._browse_btn)
        bottom_row.addStretch()
        vl.addLayout(bottom_row)

        # CSV options
        self._csv_opts = QWidget()
        csv_hl = QHBoxLayout(self._csv_opts)
        csv_hl.setContentsMargins(0, 0, 0, 0)
        csv_hl.setSpacing(8)
        csv_hl.addWidget(QLabel("Separator:"))
        self._csv_sep = _NoScrollComboBox()
        self._csv_sep.addItems([", (comma)", "; (semicolon)", "\\t (tab)", "| (pipe)"])
        self._csv_sep.setFixedWidth(150)
        csv_hl.addWidget(self._csv_sep)
        csv_hl.addWidget(QLabel("Encoding:"))
        self._csv_enc = _NoScrollComboBox()
        self._csv_enc.addItems(["utf-8", "utf-8-sig", "latin-1", "cp1252"])
        self._csv_enc.setFixedWidth(110)
        csv_hl.addWidget(self._csv_enc)
        csv_hl.addStretch()
        # CSV is default (index 0), so show csv opts initially
        vl.addWidget(self._csv_opts)

    def _on_file_type_changed(self, idx: int):
        self._csv_opts.setVisible(idx == 0)  # idx 0 = CSV
        self._clear_file()

    def _browse_file(self):
        is_csv = self._file_type_combo.currentIndex() == 0
        if is_csv:
            path, _ = QFileDialog.getOpenFileName(self, "Pilih File CSV", "", "CSV Files (*.csv)")
        else:
            path, _ = QFileDialog.getOpenFileName(self, "Pilih File Excel", "", "Excel Files (*.xlsx *.xls)")
        if path:
            self._load_file(path)

    def _load_file(self, path: str):
        self._file_path = path
        fname    = os.path.basename(path)
        size_mb  = os.path.getsize(path) / (1024 * 1024)
        is_csv   = self._file_type_combo.currentIndex() == 0
        try:
            if is_csv:
                from services.file_reader import CSVReader
                import csv as _csv
                enc = self._csv_enc.currentText()
                sep_map = {0: ",", 1: ";", 2: "\t", 3: "|"}
                rev_sep_map = {",": 0, ";": 1, "\t": 2, "|": 3}
                # Auto-detect separator from file content and update dropdown
                try:
                    with open(path, "r", encoding=enc, errors="replace") as _f:
                        _sample = _f.read(8192)
                    _dialect = _csv.Sniffer().sniff(_sample, delimiters=",;\t|")
                    _detected = _dialect.delimiter
                    if _detected in rev_sep_map:
                        self._csv_sep.blockSignals(True)
                        self._csv_sep.setCurrentIndex(rev_sep_map[_detected])
                        self._csv_sep.blockSignals(False)
                except Exception:
                    pass  # keep user's manual selection
                sep = sep_map.get(self._csv_sep.currentIndex(), ",")
                reader = CSVReader(path, sep, enc)
                preview_df = reader.preview()          # baca semua baris
                headers = list(preview_df.columns)
                self._headers = headers
                self._sheets  = []
                self._row_count = len(preview_df)
                self._show_file_loaded(fname, size_mb, None, headers, preview_df)
            else:
                from services.file_reader import ExcelReader
                reader = ExcelReader(path)
                sheets = reader.list_sheets()
                self._sheets = sheets
                self._show_file_loaded(fname, size_mb, sheets, [])
                if sheets:
                    self._load_sheet(sheets[0], reader)
        except Exception as e:
            msg_critical(self, "Gagal Buka File", str(e))

    def _load_sheet(self, sheet_name: str, reader=None):
        try:
            if reader is None:
                from services.file_reader import ExcelReader
                reader = ExcelReader(self._file_path)
            preview_df = reader.preview(sheet_name, 0)    # baca semua baris
            headers    = list(preview_df.columns)
            row_count  = len(preview_df)
            self._headers   = headers
            self._row_count = row_count
            self._update_preview(sheet_name, headers, row_count, preview_df)
            self.headers_loaded.emit(headers)
            self._sheet_info_lbl.setText(
                f"Header row: Row 1 \u00b7 {row_count:,} data rows \u00b7 {len(headers)} columns"
            )
        except Exception as e:
            msg_critical(self, "Gagal Muat Sheet", str(e))

    def _show_file_loaded(self, fname: str, size_mb: float, sheets, headers: List[str], preview_df=None):
        self._file_icon.setText("\U0001f5cb")
        self._file_icon.setStyleSheet(f"font-size: 20px; color: {COLOR_PRIMARY}; border: none;")
        self._file_name_lbl.setText(fname)
        self._file_name_lbl.setStyleSheet(
            f"color: {COLOR_TEXT}; font-size: 13px; font-weight: 600; border: none;"
        )
        meta = f"{size_mb:.1f} MB"
        if sheets is not None:
            meta += f" \u00b7 {len(sheets)} sheet{'s' if len(sheets) != 1 else ''}"
            if sheets:
                meta += f" \u00b7 {self._row_count:,} rows loaded"
        self._file_meta_lbl.setText(meta)
        self._change_file_btn.show()
        if sheets:
            self._sheet_section.show()
            self._build_sheet_tabs(sheets)
        else:
            self._sheet_section.hide()
        if not sheets and headers:
            self._update_preview(None, headers, self._row_count, preview_df)
            self.headers_loaded.emit(headers)

    def _build_sheet_tabs(self, sheets: List[str]):
        while self._sheet_tab_row.count():
            item = self._sheet_tab_row.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        for i, sh in enumerate(sheets):
            btn = QPushButton(sh)
            btn.setCheckable(True)
            btn.setFixedHeight(30)
            btn.setStyleSheet(
                "QPushButton { background: white; color: #374151; border: 1px solid #d1d5db; "
                "border-radius: 5px; padding: 4px 12px; font-size: 12px; } "
                "QPushButton:checked { background: #2563eb; color: white; border-color: #2563eb; }"
            )
            btn.clicked.connect(lambda ch, s=sh: self._on_sheet_tab_clicked(s))
            self._sheet_tab_row.addWidget(btn)
            if i == 0:
                btn.setChecked(True)
        self._sheet_tab_row.addStretch()

    def _on_sheet_tab_clicked(self, sheet_name: str):
        for i in range(self._sheet_tab_row.count()):
            w = self._sheet_tab_row.itemAt(i).widget()
            if isinstance(w, QPushButton):
                w.setChecked(w.text() == sheet_name)
        self._load_sheet(sheet_name)

    def _update_preview(self, sheet_name, headers: List[str], row_count: int, preview_df=None):
        import pandas as pd
        sheet_label = f" \u2014 {sheet_name}" if sheet_name else ""
        self._preview_title.setText(f"Column Preview{sheet_label}")
        if preview_df is not None and not preview_df.empty:
            model = _PandasTableModel(preview_df)
        else:
            model = _PandasTableModel(pd.DataFrame(columns=headers))
        self._preview_table.setModel(model)
        self._preview_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self._preview_footer.setText(
            f"Showing {row_count:,} rows \u00b7 {len(headers)} columns \u00b7 Scroll untuk lihat semua data"
        )
        self._preview_section.show()

    def _clear_file(self):
        self._file_path = ""
        self._headers   = []
        self._sheets    = []
        self._file_name_lbl.setText("Drop file here or click Browse")
        self._file_name_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px; border: none;")
        self._file_meta_lbl.setText("")
        self._change_file_btn.hide()
        self._sheet_section.hide()
        self._preview_section.hide()

    def get_headers(self) -> List[str]:
        return self._headers

    def get_source_config(self) -> DataSourceConfig:
        cfg = DataSourceConfig()
        is_csv = self._file_type_combo.currentIndex() == 0
        if is_csv:
            cfg.source_type  = "csv"
            cfg.file_path    = self._file_path
            sep_map = {0: ",", 1: ";", 2: "\t", 3: "|"}
            cfg.csv_separator = sep_map.get(self._csv_sep.currentIndex(), ",")
            cfg.csv_encoding  = self._csv_enc.currentText()
        else:
            cfg.source_type = "excel"
            cfg.file_path   = self._file_path
            for i in range(self._sheet_tab_row.count()):
                w = self._sheet_tab_row.itemAt(i).widget()
                if isinstance(w, QPushButton) and w.isChecked():
                    cfg.sheet_name = w.text()
                    break
        return cfg

    def is_loaded(self) -> bool:
        return bool(self._file_path and self._headers)


# ──────────────────────────────────────────────────────────────────────────────
# Step 2b — PostgreSQL connection card
# ──────────────────────────────────────────────────────────────────────────────

class _PgConnectionCard(QFrame):
    headers_loaded = Signal(list)

    def __init__(self, connection_store: "ConnectionStore", parent=None):
        super().__init__(parent)
        self._connection_store = connection_store
        self._headers: List[str] = []
        self._setup_ui()

    def _setup_ui(self):
        self.setObjectName("pgCard")
        self.setStyleSheet(
            "QFrame#pgCard { background: white; border: 1px solid #e2e8f0; border-radius: 10px; }"
        )
        vl = QVBoxLayout(self)
        vl.setContentsMargins(20, 18, 20, 20)
        vl.setSpacing(14)

        # Header
        hdr_row = QHBoxLayout()
        ic = QLabel("\U0001f5c3")
        ic.setStyleSheet("font-size: 18px;")
        head_lbl = QLabel("Connection Profile")
        head_lbl.setStyleSheet(f"font-size: 14px; font-weight: 600; color: {COLOR_TEXT};")
        hdr_row.addWidget(ic)
        hdr_row.addWidget(head_lbl)
        hdr_row.addStretch()
        self._saved_conn_combo = _NoScrollComboBox()
        self._saved_conn_combo.setFixedWidth(220)
        self._saved_conn_combo.setFixedHeight(34)
        self._load_saved_connections()
        self._saved_conn_combo.currentIndexChanged.connect(self._on_saved_conn_changed)
        hdr_row.addWidget(self._saved_conn_combo)
        vl.addLayout(hdr_row)

        # Form grid
        row1 = QHBoxLayout(); row1.setSpacing(12)
        host_vl = QVBoxLayout(); host_vl.addWidget(QLabel("Host"))
        self._host = QLineEdit(); self._host.setPlaceholderText("localhost kek atau IP juga bisa")
        self._host.setMinimumHeight(36); host_vl.addWidget(self._host)
        row1.addLayout(host_vl, 3)
        port_vl = QVBoxLayout(); port_vl.addWidget(QLabel("Port"))
        self._port = QLineEdit("5432"); self._port.setFixedWidth(90)
        self._port.setMinimumHeight(36); port_vl.addWidget(self._port)
        row1.addLayout(port_vl, 1)
        vl.addLayout(row1)

        row2 = QHBoxLayout(); row2.setSpacing(12)
        db_vl = QVBoxLayout(); db_vl.addWidget(QLabel("Database"))
        self._database = QLineEdit(); self._database.setPlaceholderText("pda_sampling")
        self._database.setMinimumHeight(36); db_vl.addWidget(self._database)
        row2.addLayout(db_vl, 1)
        schema_vl = QVBoxLayout(); schema_vl.addWidget(QLabel("Schema"))
        self._schema_input = QLineEdit("public")
        self._schema_input.setMinimumHeight(36); schema_vl.addWidget(self._schema_input)
        row2.addLayout(schema_vl, 1)
        vl.addLayout(row2)

        row3 = QHBoxLayout(); row3.setSpacing(12)
        user_vl = QVBoxLayout(); user_vl.addWidget(QLabel("Username"))
        self._username = QLineEdit(); self._username.setPlaceholderText("username database")
        self._username.setMinimumHeight(36); user_vl.addWidget(self._username)
        row3.addLayout(user_vl, 1)
        pass_vl = QVBoxLayout(); pass_vl.addWidget(QLabel("Password"))
        pass_row = QHBoxLayout(); pass_row.setSpacing(0)
        self._password = QLineEdit(); self._password.setEchoMode(QLineEdit.EchoMode.Password)
        self._password.setPlaceholderText("apa hayoo passwordnya")
        self._password.setMinimumHeight(36)
        self._toggle_pass = QPushButton("\U0001f441")
        self._toggle_pass.setObjectName("secondaryBtn"); self._toggle_pass.setFixedSize(36, 36)
        self._toggle_pass.clicked.connect(self._toggle_pass_visibility)
        pass_row.addWidget(self._password, 1); pass_row.addWidget(self._toggle_pass)
        pass_vl.addLayout(pass_row)
        row3.addLayout(pass_vl, 1)
        vl.addLayout(row3)

        row4 = QHBoxLayout(); row4.setSpacing(12)
        ssl_vl = QVBoxLayout(); ssl_vl.addWidget(QLabel("SSL Mode"))
        self._ssl_mode = _NoScrollComboBox(); self._ssl_mode.setMinimumHeight(36)
        self._ssl_mode.addItems(["disable", "prefer", "require", "verify-ca", "verify-full"])
        ssl_vl.addWidget(self._ssl_mode)
        row4.addLayout(ssl_vl, 1); row4.addStretch(2)
        vl.addLayout(row4)

        # Action buttons
        action_row = QHBoxLayout()
        self._test_btn = QPushButton("\u21bb  Test Connection")
        self._test_btn.setStyleSheet(
            f"QPushButton {{ background: white; color: {COLOR_PRIMARY}; "
            f"border: 1px solid {COLOR_PRIMARY}; border-radius: 6px; padding: 6px 14px; }}"
            f"QPushButton:hover {{ background: {COLOR_PRIMARY_LIGHT}; }}"
        )
        self._test_btn.clicked.connect(self._test_connection)
        self._save_conn_btn = QPushButton("\U0001f4be  Save connection")
        self._save_conn_btn.setStyleSheet(
            f"background: transparent; color: {COLOR_TEXT_MUTED}; border: none; font-size: 12px;"
        )
        self._save_conn_btn.clicked.connect(self._save_connection)
        action_row.addWidget(self._test_btn)
        action_row.addStretch()
        action_row.addWidget(self._save_conn_btn)
        vl.addLayout(action_row)

        # ── SSH Tunnel ────────────────────────────────────────────────────────
        self._ssh_check = QCheckBox("Gunakan SSH Tunnel")
        self._ssh_check.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 13px;")
        self._ssh_check.toggled.connect(self._toggle_ssh_panel)
        vl.addWidget(self._ssh_check)

        self._ssh_panel = QFrame()
        self._ssh_panel.setObjectName("sshPanel")
        self._ssh_panel.setStyleSheet(
            "QFrame#sshPanel { background: #eef2ff; border: 1px solid #c7d2fe; border-radius: 8px; }"
        )
        ssh_vl = QVBoxLayout(self._ssh_panel)
        ssh_vl.setContentsMargins(14, 12, 14, 12)
        ssh_vl.setSpacing(10)

        ssh_title = QLabel("\U0001f510 SSH TUNNEL")
        ssh_title.setStyleSheet(
            "font-size: 12px; font-weight: 700; color: #4338ca; background: transparent;"
        )
        ssh_vl.addWidget(ssh_title)

        ssh_row1 = QHBoxLayout(); ssh_row1.setSpacing(12)
        ssh_host_vl = QVBoxLayout()
        _ssh_host_lbl = QLabel("SSH Host"); _ssh_host_lbl.setStyleSheet("background: transparent;")
        ssh_host_vl.addWidget(_ssh_host_lbl)
        self._ssh_host = QLineEdit()
        self._ssh_host.setPlaceholderText("bastion.example.com")
        self._ssh_host.setMinimumHeight(34)
        ssh_host_vl.addWidget(self._ssh_host)
        ssh_row1.addLayout(ssh_host_vl, 3)
        ssh_port_vl = QVBoxLayout()
        _ssh_port_lbl = QLabel("SSH Port"); _ssh_port_lbl.setStyleSheet("background: transparent;")
        ssh_port_vl.addWidget(_ssh_port_lbl)
        self._ssh_port = QLineEdit("22")
        self._ssh_port.setFixedWidth(80)
        self._ssh_port.setMinimumHeight(34)
        ssh_port_vl.addWidget(self._ssh_port)
        ssh_row1.addLayout(ssh_port_vl, 1)
        ssh_vl.addLayout(ssh_row1)

        ssh_row2 = QHBoxLayout(); ssh_row2.setSpacing(12)
        ssh_user_vl = QVBoxLayout()
        _ssh_user_lbl = QLabel("SSH User"); _ssh_user_lbl.setStyleSheet("background: transparent;")
        ssh_user_vl.addWidget(_ssh_user_lbl)
        self._ssh_user = QLineEdit()
        self._ssh_user.setPlaceholderText("ubuntu")
        self._ssh_user.setMinimumHeight(34)
        ssh_user_vl.addWidget(self._ssh_user)
        ssh_row2.addLayout(ssh_user_vl, 1)
        ssh_auth_vl = QVBoxLayout()
        _ssh_auth_lbl = QLabel("Auth Method"); _ssh_auth_lbl.setStyleSheet("background: transparent;")
        ssh_auth_vl.addWidget(_ssh_auth_lbl)
        self._ssh_auth = _NoScrollComboBox()
        self._ssh_auth.addItems(["Password", "Key"])
        self._ssh_auth.setMinimumHeight(34)
        self._ssh_auth.currentTextChanged.connect(self._toggle_ssh_auth)
        ssh_auth_vl.addWidget(self._ssh_auth)
        ssh_row2.addLayout(ssh_auth_vl, 1)
        ssh_vl.addLayout(ssh_row2)

        # SSH Password widget
        self._ssh_pass_widget = QWidget()
        self._ssh_pass_widget.setStyleSheet("background: transparent;")
        ssh_pass_vl = QVBoxLayout(self._ssh_pass_widget)
        ssh_pass_vl.setContentsMargins(0, 0, 0, 0)
        _ssh_pass_lbl = QLabel("SSH Password"); _ssh_pass_lbl.setStyleSheet("background: transparent;")
        ssh_pass_vl.addWidget(_ssh_pass_lbl)
        self._ssh_password = QLineEdit()
        self._ssh_password.setEchoMode(QLineEdit.EchoMode.Password)
        self._ssh_password.setMinimumHeight(34)
        ssh_pass_vl.addWidget(self._ssh_password)
        ssh_vl.addWidget(self._ssh_pass_widget)

        # SSH Key Path widget
        self._ssh_key_widget = QWidget()
        self._ssh_key_widget.setStyleSheet("background: transparent;")
        ssh_key_vl = QVBoxLayout(self._ssh_key_widget)
        ssh_key_vl.setContentsMargins(0, 0, 0, 0)
        _ssh_key_lbl = QLabel("SSH Private Key Path"); _ssh_key_lbl.setStyleSheet("background: transparent;")
        ssh_key_vl.addWidget(_ssh_key_lbl)
        ssh_key_row = QHBoxLayout(); ssh_key_row.setSpacing(6)
        self._ssh_key_path = QLineEdit()
        self._ssh_key_path.setPlaceholderText("~/.ssh/id_rsa")
        self._ssh_key_path.setMinimumHeight(34)
        _browse_key_btn = QPushButton("Browse")
        _browse_key_btn.setFixedHeight(34)
        _browse_key_btn.clicked.connect(self._browse_ssh_key)
        ssh_key_row.addWidget(self._ssh_key_path, 1)
        ssh_key_row.addWidget(_browse_key_btn)
        ssh_key_vl.addLayout(ssh_key_row)
        self._ssh_key_widget.hide()
        ssh_vl.addWidget(self._ssh_key_widget)

        self._ssh_panel.hide()
        vl.addWidget(self._ssh_panel)
        # ── end SSH Tunnel ────────────────────────────────────────────────────

        vl.addWidget(_Divider())

        ds_lbl = QLabel("Data Source")
        ds_lbl.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        vl.addWidget(ds_lbl)

        # Toggle table / custom query
        mode_row = QHBoxLayout(); mode_row.setSpacing(0)
        self._table_mode_btn = QPushButton("\u229e  Select Table")
        self._table_mode_btn.setCheckable(True); self._table_mode_btn.setChecked(True)
        self._table_mode_btn.setFixedHeight(32)
        self._table_mode_btn.clicked.connect(lambda: self._set_ds_mode("table"))
        self._table_mode_btn.setStyleSheet(
            f"QPushButton {{ background: {COLOR_PRIMARY}; color: white; "
            "border-radius: 6px 0 0 6px; padding: 4px 14px; font-size: 12px; border: none; }}"
            f"QPushButton:!checked {{ background: white; color: {COLOR_TEXT}; "
            f"border: 1px solid {COLOR_BORDER}; }}"
        )
        self._query_mode_btn = QPushButton("</> Custom Query")
        self._query_mode_btn.setCheckable(True)
        self._query_mode_btn.setFixedHeight(32)
        self._query_mode_btn.clicked.connect(lambda: self._set_ds_mode("query"))
        self._query_mode_btn.setStyleSheet(
            f"QPushButton {{ background: white; color: {COLOR_TEXT}; "
            f"border: 1px solid {COLOR_BORDER}; border-left: none; "
            "border-radius: 0 6px 6px 0; padding: 4px 14px; font-size: 12px; }}"
            f"QPushButton:checked {{ background: {COLOR_PRIMARY}; color: white; border: none; }}"
        )
        mode_row.addWidget(self._table_mode_btn)
        mode_row.addWidget(self._query_mode_btn)
        mode_row.addStretch()
        vl.addLayout(mode_row)

        # Table selector
        self._table_mode_widget = QWidget()
        tm_hl = QHBoxLayout(self._table_mode_widget); tm_hl.setContentsMargins(0,0,0,0); tm_hl.setSpacing(12)
        s_vl = QVBoxLayout(); s_vl.addWidget(QLabel("Schema"))
        self._pg_schema_combo = _NoScrollComboBox(); self._pg_schema_combo.setMinimumHeight(36)
        self._pg_schema_combo.currentTextChanged.connect(self._on_schema_changed)
        s_vl.addWidget(self._pg_schema_combo); tm_hl.addLayout(s_vl, 1)
        t_vl = QVBoxLayout(); t_vl.addWidget(QLabel("Table"))
        self._pg_table_combo = _NoScrollComboBox(); self._pg_table_combo.setMinimumHeight(36)
        self._pg_table_combo.currentTextChanged.connect(self._on_table_changed)
        t_vl.addWidget(self._pg_table_combo); tm_hl.addLayout(t_vl, 2)
        vl.addWidget(self._table_mode_widget)

        # Custom query
        self._query_mode_widget = QWidget()
        qm_vl = QVBoxLayout(self._query_mode_widget); qm_vl.setContentsMargins(0,0,0,0)
        self._custom_query = QTextEdit()
        self._custom_query.setPlaceholderText("SELECT * FROM schema.table WHERE ...")
        self._custom_query.setFixedHeight(80); qm_vl.addWidget(self._custom_query)
        self._query_mode_widget.hide()
        vl.addWidget(self._query_mode_widget)

        self._load_cols_btn = QPushButton("\U0001f50d  Ambil Data")
        self._load_cols_btn.setObjectName("secondaryBtn")
        self._load_cols_btn.clicked.connect(lambda: self._load_pg_columns(silent=False))
        self._columns_status_lbl = QLabel("")
        self._columns_status_lbl.setStyleSheet(f"color: {COLOR_SUCCESS}; font-size: 12px;")
        vl.addWidget(self._load_cols_btn)
        vl.addWidget(self._columns_status_lbl)
        vl.addStretch()

    def _toggle_pass_visibility(self):
        if self._password.echoMode() == QLineEdit.EchoMode.Password:
            self._password.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self._password.setEchoMode(QLineEdit.EchoMode.Password)

    def _toggle_ssh_panel(self, enabled: bool):
        self._ssh_panel.setVisible(enabled)

    def _toggle_ssh_auth(self, method: str):
        is_password = method == "Password"
        self._ssh_pass_widget.setVisible(is_password)
        self._ssh_key_widget.setVisible(not is_password)

    def _browse_ssh_key(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Pilih SSH Private Key", os.path.expanduser("~/.ssh")
        )
        if path:
            self._ssh_key_path.setText(path)

    def _set_ds_mode(self, mode: str):
        is_table = mode == "table"
        self._table_mode_btn.setChecked(is_table)
        self._query_mode_btn.setChecked(not is_table)
        self._table_mode_widget.setVisible(is_table)
        self._query_mode_widget.setVisible(not is_table)
        if is_table:
            # Reload kolom dari tabel saat balik ke mode tabel
            table = self._pg_table_combo.currentText()
            if table:
                self._load_pg_columns(silent=True)
        else:
            # Bersihkan kolom saat masuk query mode — user harus klik Ambil Data
            self._headers = []
            self._columns_status_lbl.setText("Klik \"Ambil Data\" untuk load kolom dari query")
            self.headers_loaded.emit([])

    def _load_saved_connections(self):
        self._saved_conn_combo.clear()
        self._saved_conn_combo.addItem("\u2014 Load saved connection \u2014", None)
        try:
            for p in self._connection_store.get_all():
                self._saved_conn_combo.addItem(p.name, p.id)
        except Exception:
            pass

    def _on_saved_conn_changed(self, idx: int):
        conn_id = self._saved_conn_combo.currentData()
        if not conn_id:
            return
        try:
            profile = self._connection_store.get_by_id(conn_id)
            if profile:
                self._host.setText(profile.host)
                self._port.setText(str(profile.port))
                self._database.setText(profile.database)
                self._username.setText(profile.username)
                self._password.setText(profile.password)
                self._schema_input.setText(getattr(profile, "default_schema", "public") or "public")
                idx = self._ssl_mode.findText(profile.ssl_mode)
                if idx >= 0:
                    self._ssl_mode.setCurrentIndex(idx)
                # SSH Tunnel fields
                self._ssh_check.setChecked(bool(getattr(profile, "use_ssh_tunnel", False)))
                self._ssh_host.setText(getattr(profile, "ssh_host", ""))
                self._ssh_port.setText(str(getattr(profile, "ssh_port", 22)))
                self._ssh_user.setText(getattr(profile, "ssh_user", ""))
                auth = getattr(profile, "ssh_auth_method", "password")
                self._ssh_auth.setCurrentText("Key" if auth == "key" else "Password")
                self._ssh_password.setText(getattr(profile, "ssh_password", ""))
                self._ssh_key_path.setText(getattr(profile, "ssh_key_path", ""))
        except Exception:
            pass

    def _build_profile(self):
        from models.connection_profile import ConnectionProfile
        p = ConnectionProfile()
        p.host     = self._host.text().strip()
        p.port     = int(self._port.text().strip() or "5432")
        p.database = self._database.text().strip()
        p.username = self._username.text().strip()
        p.password = self._password.text()
        p.ssl_mode = self._ssl_mode.currentText()
        p.default_schema = self._schema_input.text().strip() or "public"
        p.name     = f"{p.username}@{p.host}/{p.database}"
        # SSH Tunnel
        p.use_ssh_tunnel = self._ssh_check.isChecked()
        p.ssh_host        = self._ssh_host.text().strip()
        p.ssh_port        = int(self._ssh_port.text().strip() or "22")
        p.ssh_user        = self._ssh_user.text().strip()
        p.ssh_auth_method = self._ssh_auth.currentText().lower()
        p.ssh_password    = self._ssh_password.text()
        p.ssh_key_path    = self._ssh_key_path.text().strip()
        return p

    def _test_connection(self):
        pg = None
        try:
            from services.postgres_connector import PostgresConnector
            profile = self._build_profile()
            if not profile.host or not profile.database:
                msg_warning(self, "Input Tidak Lengkap", "Isi Host dan Database terlebih dahulu.")
                return
            pg = PostgresConnector.from_profile(profile)
            ok, msg = pg.test_connection()
            if ok:
                schemas = pg.list_schemas()
                self._pg_schema_combo.clear()
                self._pg_schema_combo.addItems(schemas)
                msg_info(self, "Koneksi Berhasil \u2713", f"Terkoneksi!\n{msg}")
            else:
                msg_critical(self, "Koneksi Gagal", msg)
        except Exception as e:
            msg_critical(self, "Koneksi Error", str(e))
        finally:
            if pg:
                pg.close()

    def _on_schema_changed(self, schema: str):
        if not schema:
            return
        pg = None
        try:
            from services.postgres_connector import PostgresConnector
            pg = PostgresConnector.from_profile(self._build_profile())
            tables = pg.list_tables(schema)
            self._pg_table_combo.clear()
            self._pg_table_combo.addItems(tables)
        except Exception:
            pass
        finally:
            if pg:
                pg.close()

    def _on_table_changed(self, table: str):
        """Auto-load columns when table selection changes (silent, no popup)."""
        if not table:
            self._headers = []
            self._columns_status_lbl.setText("")
            return
        # Hanya auto-load jika sedang di mode tabel, bukan query
        if not self._query_mode_btn.isChecked():
            self._load_pg_columns(silent=True)

    def _load_pg_columns(self, silent: bool = False):
        pg = None
        try:
            from services.postgres_connector import PostgresConnector
            import pandas as pd
            profile = self._build_profile()
            if not profile.host or not profile.database:
                return  # belum ada koneksi, abaikan
            pg = PostgresConnector.from_profile(profile)
            engine = pg._get_engine()
            if self._query_mode_btn.isChecked():
                query = self._custom_query.toPlainText().strip()
                if not query:
                    if not silent:
                        msg_warning(self, "Query Kosong", "Isi SQL query dulu.")
                    return
                df = pd.read_sql(f"SELECT * FROM ({query}) AS q LIMIT 0", engine)
                headers = list(df.columns)
                row_count_df = pd.read_sql(f"SELECT COUNT(*) AS n FROM ({query}) AS q", engine)
                row_count = int(row_count_df.iloc[0, 0])
            else:
                schema = self._pg_schema_combo.currentText()
                table  = self._pg_table_combo.currentText()
                if not schema or not table:
                    return
                headers = pg.get_columns(schema, table)
                row_count_df = pd.read_sql(f'SELECT COUNT(*) AS n FROM "{schema}"."{table}"', engine)
                row_count = int(row_count_df.iloc[0, 0])
            self._headers = headers
            self.headers_loaded.emit(headers)
            self._columns_status_lbl.setText(
                f"\u2713 {len(headers)} kolom dimuat \u00b7 {row_count:,} baris data"
            )
            if not silent:
                msg_info(self, "Data Dimuat", f"{len(headers)} kolom · {row_count:,} baris berhasil dimuat.")
        except Exception as e:
            self._columns_status_lbl.setText("")
            if not silent:
                msg_critical(self, "Gagal Load Kolom", str(e))
        finally:
            if pg:
                pg.close()

    def _save_connection(self):
        try:
            if msg_question(self, "Simpan Koneksi", "Simpan koneksi ini ke daftar saved connections?"):
                self._connection_store.save(self._build_profile())
                self._load_saved_connections()
                msg_info(self, "Tersimpan", "Profil koneksi berhasil disimpan.")
        except Exception as e:
            msg_critical(self, "Gagal Simpan", str(e))

    def get_headers(self) -> List[str]:
        return self._headers

    def get_source_config(self) -> DataSourceConfig:
        cfg = DataSourceConfig()
        cfg.source_type = "postgres"
        cfg.connection_id  = self._saved_conn_combo.currentData() or ""
        cfg.schema_name    = self._pg_schema_combo.currentText()
        cfg.table_name     = self._pg_table_combo.currentText()
        cfg.use_custom_query = self._query_mode_btn.isChecked()
        cfg.custom_query   = self._custom_query.toPlainText().strip()
        # Selalu simpan detail koneksi inline agar worker bisa konek meski connection_id kosong
        cfg.pg_connection_inline = self._build_profile().to_dict()
        return cfg

    def is_ready(self) -> bool:
        return bool(self._host.text().strip() and self._database.text().strip())


# ──────────────────────────────────────────────────────────────────────────────
# Step 2c — Generic DB source card (PostgreSQL OR MySQL, both sides)
# ──────────────────────────────────────────────────────────────────────────────

class _DbSourceCard(QFrame):
    """Reusable DB connection card — works for left AND right side of DB vs DB jobs.
    Supports PostgreSQL and MySQL with dynamic SSL options and SSH tunnel.
    """
    headers_loaded = Signal(list)

    def __init__(self, side: str, connection_store: "ConnectionStore", parent=None):
        super().__init__(parent)
        self._side = side  # "L" or "R"
        self._connection_store = connection_store
        self._headers: List[str] = []
        self._db_type = "postgresql"  # "postgresql" or "mysql"
        self._setup_ui()

    def _setup_ui(self):
        self.setObjectName("dbCard")
        self.setStyleSheet(
            "QFrame#dbCard { background: white; border: 1px solid #e2e8f0; border-radius: 10px; }"
        )
        vl = QVBoxLayout(self)
        vl.setContentsMargins(20, 18, 20, 20)
        vl.setSpacing(12)

        # Header with side badge and db type toggle
        hdr_row = QHBoxLayout()
        side_color = COLOR_PRIMARY if self._side == "L" else COLOR_PURPLE
        badge = QLabel(f" {self._side} ")
        badge.setStyleSheet(
            f"background: {side_color}; color: white; border-radius: 4px; "
            "font-size: 12px; font-weight: 700; padding: 2px 8px;"
        )
        side_lbl = QLabel(f"Source {'Kiri' if self._side == 'L' else 'Kanan'} (Database)")
        side_lbl.setStyleSheet(f"font-size: 14px; font-weight: 600; color: {COLOR_TEXT};")
        hdr_row.addWidget(badge)
        hdr_row.addWidget(side_lbl)
        hdr_row.addStretch()
        vl.addLayout(hdr_row)

        # DB type toggle
        db_type_row = QHBoxLayout()
        self._rb_pg = QPushButton("PostgreSQL")
        self._rb_mysql = QPushButton("MySQL")
        for btn in (self._rb_pg, self._rb_mysql):
            btn.setCheckable(True)
            btn.setFixedHeight(30)
        self._rb_pg.setChecked(True)
        self._rb_pg.clicked.connect(lambda: self._set_db_type("postgresql"))
        self._rb_mysql.clicked.connect(lambda: self._set_db_type("mysql"))
        db_type_row.addWidget(self._rb_pg)
        db_type_row.addWidget(self._rb_mysql)
        db_type_row.addStretch()
        vl.addLayout(db_type_row)
        self._update_db_type_buttons()

        # Saved connection dropdown
        saved_row = QHBoxLayout()
        self._saved_combo = _NoScrollComboBox()
        self._saved_combo.setMinimumHeight(34)
        self._load_saved_connections()
        self._saved_combo.currentIndexChanged.connect(self._on_saved_conn_changed)
        saved_row.addWidget(QLabel("Load:"))
        saved_row.addWidget(self._saved_combo, 1)
        vl.addLayout(saved_row)

        # Host / Port
        row1 = QHBoxLayout(); row1.setSpacing(12)
        host_vl = QVBoxLayout(); host_vl.addWidget(QLabel("Host"))
        self._host = QLineEdit(); self._host.setPlaceholderText("localhost")
        self._host.setMinimumHeight(36); host_vl.addWidget(self._host)
        row1.addLayout(host_vl, 3)
        port_vl = QVBoxLayout(); port_vl.addWidget(QLabel("Port"))
        self._port = QLineEdit("5432"); self._port.setFixedWidth(90)
        self._port.setMinimumHeight(36); port_vl.addWidget(self._port)
        row1.addLayout(port_vl, 1)
        vl.addLayout(row1)

        # Database / Schema
        row2 = QHBoxLayout(); row2.setSpacing(12)
        db_vl = QVBoxLayout(); db_vl.addWidget(QLabel("Database"))
        self._database = QLineEdit(); self._database.setPlaceholderText("nama_database")
        self._database.setMinimumHeight(36); db_vl.addWidget(self._database)
        row2.addLayout(db_vl, 1)
        schema_vl = QVBoxLayout()
        self._schema_lbl = QLabel("Schema")
        schema_vl.addWidget(self._schema_lbl)
        self._schema = QLineEdit("public")
        self._schema.setMinimumHeight(36); schema_vl.addWidget(self._schema)
        row2.addLayout(schema_vl, 1)
        vl.addLayout(row2)

        # Username / Password
        row3 = QHBoxLayout(); row3.setSpacing(12)
        user_vl = QVBoxLayout(); user_vl.addWidget(QLabel("Username"))
        self._username = QLineEdit(); self._username.setMinimumHeight(36)
        user_vl.addWidget(self._username); row3.addLayout(user_vl, 1)
        pass_vl = QVBoxLayout(); pass_vl.addWidget(QLabel("Password"))
        pass_row = QHBoxLayout(); pass_row.setSpacing(0)
        self._password = QLineEdit()
        self._password.setEchoMode(QLineEdit.EchoMode.Password)
        self._password.setMinimumHeight(36)
        _toggle = QPushButton("\U0001f441")
        _toggle.setObjectName("secondaryBtn"); _toggle.setFixedSize(36, 36)
        _toggle.clicked.connect(self._toggle_password)
        pass_row.addWidget(self._password, 1); pass_row.addWidget(_toggle)
        pass_vl.addLayout(pass_row); row3.addLayout(pass_vl, 1)
        vl.addLayout(row3)

        # SSL
        ssl_row = QHBoxLayout(); ssl_row.setSpacing(12)
        ssl_vl = QVBoxLayout()
        self._ssl_lbl = QLabel("SSL Mode")
        ssl_vl.addWidget(self._ssl_lbl)
        self._ssl_combo = _NoScrollComboBox(); self._ssl_combo.setMinimumHeight(36)
        self._ssl_combo.addItems(["disable", "prefer", "require", "verify-ca", "verify-full"])
        ssl_vl.addWidget(self._ssl_combo)
        ssl_row.addLayout(ssl_vl, 1); ssl_row.addStretch(2)
        vl.addLayout(ssl_row)

        # Action buttons
        action_row = QHBoxLayout()
        self._test_btn = QPushButton("\u21bb  Test Connection")
        self._test_btn.setStyleSheet(
            f"QPushButton {{ background: white; color: {COLOR_PRIMARY}; "
            f"border: 1px solid {COLOR_PRIMARY}; border-radius: 6px; padding: 6px 14px; }}"
            f"QPushButton:hover {{ background: {COLOR_PRIMARY_LIGHT}; }}"
        )
        self._test_btn.clicked.connect(self._test_connection)
        self._save_btn = QPushButton("\U0001f4be  Save connection")
        self._save_btn.setStyleSheet(
            f"background: transparent; color: {COLOR_TEXT_MUTED}; border: none; font-size: 12px;"
        )
        self._save_btn.clicked.connect(self._save_connection)
        action_row.addWidget(self._test_btn)
        action_row.addStretch()
        action_row.addWidget(self._save_btn)
        vl.addLayout(action_row)

        # SSH Tunnel
        self._ssh_check = QCheckBox("Gunakan SSH Tunnel")
        self._ssh_check.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 13px;")
        self._ssh_check.toggled.connect(lambda enabled: self._ssh_panel.setVisible(enabled))
        vl.addWidget(self._ssh_check)

        self._ssh_panel = QFrame()
        self._ssh_panel.setObjectName("sshPanel2")
        self._ssh_panel.setStyleSheet(
            "QFrame#sshPanel2 { background: #eef2ff; border: 1px solid #c7d2fe; border-radius: 8px; }"
        )
        ssh_vl = QVBoxLayout(self._ssh_panel)
        ssh_vl.setContentsMargins(14, 12, 14, 12); ssh_vl.setSpacing(10)
        ssh_title = QLabel("\U0001f510 SSH TUNNEL")
        ssh_title.setStyleSheet("font-size: 12px; font-weight: 700; color: #4338ca; background: transparent;")
        ssh_vl.addWidget(ssh_title)
        ssh_r1 = QHBoxLayout(); ssh_r1.setSpacing(12)
        sh_vl = QVBoxLayout()
        _l = QLabel("SSH Host"); _l.setStyleSheet("background: transparent;"); sh_vl.addWidget(_l)
        self._ssh_host = QLineEdit(); self._ssh_host.setPlaceholderText("bastion.example.com")
        self._ssh_host.setMinimumHeight(34); sh_vl.addWidget(self._ssh_host)
        ssh_r1.addLayout(sh_vl, 3)
        sp_vl = QVBoxLayout()
        _l2 = QLabel("SSH Port"); _l2.setStyleSheet("background: transparent;"); sp_vl.addWidget(_l2)
        self._ssh_port = QLineEdit("22"); self._ssh_port.setFixedWidth(80)
        self._ssh_port.setMinimumHeight(34); sp_vl.addWidget(self._ssh_port)
        ssh_r1.addLayout(sp_vl, 1); ssh_vl.addLayout(ssh_r1)
        ssh_r2 = QHBoxLayout(); ssh_r2.setSpacing(12)
        su_vl = QVBoxLayout()
        _l3 = QLabel("SSH User"); _l3.setStyleSheet("background: transparent;"); su_vl.addWidget(_l3)
        self._ssh_user = QLineEdit(); self._ssh_user.setPlaceholderText("ubuntu")
        self._ssh_user.setMinimumHeight(34); su_vl.addWidget(self._ssh_user)
        ssh_r2.addLayout(su_vl, 1)
        sa_vl = QVBoxLayout()
        _l4 = QLabel("Auth Method"); _l4.setStyleSheet("background: transparent;"); sa_vl.addWidget(_l4)
        self._ssh_auth = _NoScrollComboBox()
        self._ssh_auth.addItems(["Password", "Key"]); self._ssh_auth.setMinimumHeight(34)
        self._ssh_auth.currentTextChanged.connect(self._toggle_ssh_auth)
        sa_vl.addWidget(self._ssh_auth); ssh_r2.addLayout(sa_vl, 1); ssh_vl.addLayout(ssh_r2)
        self._ssh_pass_w = QWidget(); self._ssh_pass_w.setStyleSheet("background: transparent;")
        ssp_vl = QVBoxLayout(self._ssh_pass_w); ssp_vl.setContentsMargins(0,0,0,0)
        _l5 = QLabel("SSH Password"); _l5.setStyleSheet("background: transparent;"); ssp_vl.addWidget(_l5)
        self._ssh_password = QLineEdit(); self._ssh_password.setEchoMode(QLineEdit.EchoMode.Password)
        self._ssh_password.setMinimumHeight(34); ssp_vl.addWidget(self._ssh_password)
        ssh_vl.addWidget(self._ssh_pass_w)
        self._ssh_key_w = QWidget(); self._ssh_key_w.setStyleSheet("background: transparent;")
        ssk_vl = QVBoxLayout(self._ssh_key_w); ssk_vl.setContentsMargins(0,0,0,0)
        _l6 = QLabel("SSH Private Key Path"); _l6.setStyleSheet("background: transparent;"); ssk_vl.addWidget(_l6)
        skr = QHBoxLayout(); skr.setSpacing(6)
        self._ssh_key_path = QLineEdit(); self._ssh_key_path.setPlaceholderText("~/.ssh/id_rsa")
        self._ssh_key_path.setMinimumHeight(34)
        _bk = QPushButton("Browse"); _bk.setFixedHeight(34)
        _bk.clicked.connect(self._browse_ssh_key)
        skr.addWidget(self._ssh_key_path, 1); skr.addWidget(_bk)
        ssk_vl.addLayout(skr); self._ssh_key_w.hide(); ssh_vl.addWidget(self._ssh_key_w)
        self._ssh_panel.hide(); vl.addWidget(self._ssh_panel)

        vl.addWidget(_Divider())

        # Data Source: Table or Custom Query
        ds_lbl = QLabel("Data Source")
        ds_lbl.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        vl.addWidget(ds_lbl)

        mode_row = QHBoxLayout(); mode_row.setSpacing(0)
        self._table_mode_btn = QPushButton("\u229e  Select Table")
        self._table_mode_btn.setCheckable(True); self._table_mode_btn.setChecked(True)
        self._table_mode_btn.setFixedHeight(32)
        self._table_mode_btn.clicked.connect(lambda: self._set_ds_mode("table"))
        self._table_mode_btn.setStyleSheet(
            f"QPushButton {{ background: {COLOR_PRIMARY}; color: white; "
            "border-radius: 6px 0 0 6px; padding: 4px 14px; font-size: 12px; border: none; }}"
            f"QPushButton:!checked {{ background: white; color: {COLOR_TEXT}; "
            f"border: 1px solid {COLOR_BORDER}; }}"
        )
        self._query_mode_btn = QPushButton("</> Custom Query")
        self._query_mode_btn.setCheckable(True)
        self._query_mode_btn.setFixedHeight(32)
        self._query_mode_btn.clicked.connect(lambda: self._set_ds_mode("query"))
        self._query_mode_btn.setStyleSheet(
            f"QPushButton {{ background: white; color: {COLOR_TEXT}; "
            f"border: 1px solid {COLOR_BORDER}; border-left: none; "
            "border-radius: 0 6px 6px 0; padding: 4px 14px; font-size: 12px; }}"
            f"QPushButton:checked {{ background: {COLOR_PRIMARY}; color: white; border: none; }}"
        )
        mode_row.addWidget(self._table_mode_btn)
        mode_row.addWidget(self._query_mode_btn)
        mode_row.addStretch()
        vl.addLayout(mode_row)

        # Table selector
        self._table_mode_widget = QWidget()
        tm_hl = QHBoxLayout(self._table_mode_widget); tm_hl.setContentsMargins(0,0,0,0); tm_hl.setSpacing(12)
        s_vl = QVBoxLayout(); s_vl.addWidget(QLabel("Schema"))
        self._schema_combo = _NoScrollComboBox(); self._schema_combo.setMinimumHeight(36)
        self._schema_combo.currentTextChanged.connect(self._on_schema_changed)
        s_vl.addWidget(self._schema_combo); tm_hl.addLayout(s_vl, 1)
        t_vl = QVBoxLayout(); t_vl.addWidget(QLabel("Table"))
        self._table_combo = _NoScrollComboBox(); self._table_combo.setMinimumHeight(36)
        self._table_combo.currentTextChanged.connect(self._on_table_changed)
        t_vl.addWidget(self._table_combo); tm_hl.addLayout(t_vl, 2)
        vl.addWidget(self._table_mode_widget)

        # Custom query
        self._query_mode_widget = QWidget()
        qm_vl = QVBoxLayout(self._query_mode_widget); qm_vl.setContentsMargins(0,0,0,0)
        self._custom_query = QTextEdit()
        placeholder = "SELECT * FROM schema.table WHERE ..."
        self._custom_query.setPlaceholderText(placeholder)
        self._custom_query.setFixedHeight(80); qm_vl.addWidget(self._custom_query)
        self._query_mode_widget.hide()
        vl.addWidget(self._query_mode_widget)

        self._load_cols_btn = QPushButton("\U0001f50d  Ambil Data")
        self._load_cols_btn.setObjectName("secondaryBtn")
        self._load_cols_btn.clicked.connect(lambda: self._load_columns(silent=False))
        self._columns_status_lbl = QLabel("")
        self._columns_status_lbl.setStyleSheet(f"color: {COLOR_SUCCESS}; font-size: 12px;")
        vl.addWidget(self._load_cols_btn)
        vl.addWidget(self._columns_status_lbl)
        vl.addStretch()

    # ------------------------------------------------------------------ db type

    def _set_db_type(self, db_type: str):
        self._db_type = db_type
        self._update_db_type_buttons()
        # Adjust port default
        if db_type == "mysql" and self._port.text() == "5432":
            self._port.setText("3306")
        elif db_type == "postgresql" and self._port.text() == "3306":
            self._port.setText("5432")
        # Adjust SSL options
        self._ssl_combo.blockSignals(True)
        current = self._ssl_combo.currentText()
        self._ssl_combo.clear()
        if db_type == "mysql":
            self._ssl_combo.addItems(["disabled", "required", "verify_ca"])
            self._ssl_lbl.setText("SSL")
            self._schema_lbl.setText("Database (schema)")
        else:
            self._ssl_combo.addItems(["disable", "prefer", "require", "verify-ca", "verify-full"])
            self._ssl_lbl.setText("SSL Mode")
            self._schema_lbl.setText("Schema")
        self._ssl_combo.blockSignals(False)
        # Re-select same value if still valid
        idx = self._ssl_combo.findText(current)
        if idx >= 0:
            self._ssl_combo.setCurrentIndex(idx)
        # Reload saved connections filtered by db_type
        self._load_saved_connections()

    def _update_db_type_buttons(self):
        is_pg = self._db_type == "postgresql"
        selected = (
            f"background: {COLOR_PRIMARY}; color: white; border-radius: 4px; "
            "padding: 2px 12px; border: none;"
        )
        unselected = (
            f"background: white; color: {COLOR_TEXT}; border-radius: 4px; "
            f"border: 1px solid {COLOR_BORDER}; padding: 2px 12px;"
        )
        self._rb_pg.setChecked(is_pg)
        self._rb_mysql.setChecked(not is_pg)
        self._rb_pg.setStyleSheet(selected if is_pg else unselected)
        self._rb_mysql.setStyleSheet(selected if not is_pg else unselected)

    # ------------------------------------------------------------------ saved connections

    def _load_saved_connections(self):
        self._saved_combo.clear()
        self._saved_combo.addItem("\u2014 Load saved connection \u2014", None)
        try:
            for p in self._connection_store.get_all():
                db_type = getattr(p, "db_type", "postgresql")
                if db_type == self._db_type:
                    label = f"{p.name}"
                    self._saved_combo.addItem(label, p.id)
        except Exception:
            pass

    def _on_saved_conn_changed(self, idx: int):
        conn_id = self._saved_combo.currentData()
        if not conn_id:
            return
        try:
            profile = self._connection_store.get_by_id(conn_id)
            if profile:
                db_type = getattr(profile, "db_type", "postgresql")
                self._set_db_type(db_type)
                self._host.setText(profile.host)
                self._port.setText(str(profile.port))
                self._database.setText(profile.database)
                self._username.setText(profile.username)
                self._password.setText(profile.password)
                idx2 = self._ssl_combo.findText(profile.ssl_mode)
                if idx2 >= 0:
                    self._ssl_combo.setCurrentIndex(idx2)
                self._ssh_check.setChecked(bool(getattr(profile, "use_ssh_tunnel", False)))
                self._ssh_host.setText(getattr(profile, "ssh_host", ""))
                self._ssh_port.setText(str(getattr(profile, "ssh_port", 22)))
                self._ssh_user.setText(getattr(profile, "ssh_user", ""))
                auth = getattr(profile, "ssh_auth_method", "password")
                self._ssh_auth.setCurrentText("Key" if auth == "key" else "Password")
                self._ssh_password.setText(getattr(profile, "ssh_password", ""))
                self._ssh_key_path.setText(getattr(profile, "ssh_key_path", ""))
        except Exception:
            pass

    # ------------------------------------------------------------------ SSH helpers

    def _toggle_password(self):
        if self._password.echoMode() == QLineEdit.EchoMode.Password:
            self._password.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self._password.setEchoMode(QLineEdit.EchoMode.Password)

    def _toggle_ssh_auth(self, method: str):
        is_password = method == "Password"
        self._ssh_pass_w.setVisible(is_password)
        self._ssh_key_w.setVisible(not is_password)

    def _browse_ssh_key(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Pilih SSH Private Key", os.path.expanduser("~/.ssh")
        )
        if path:
            self._ssh_key_path.setText(path)

    # ------------------------------------------------------------------ data source mode

    def _set_ds_mode(self, mode: str):
        is_table = mode == "table"
        self._table_mode_btn.setChecked(is_table)
        self._query_mode_btn.setChecked(not is_table)
        self._table_mode_widget.setVisible(is_table)
        self._query_mode_widget.setVisible(not is_table)
        if is_table and self._table_combo.currentText():
            self._load_columns(silent=True)
        elif not is_table:
            self._headers = []
            self._columns_status_lbl.setText("Klik \"Ambil Data\" untuk load kolom dari query")
            self.headers_loaded.emit([])

    def _on_schema_changed(self, schema: str):
        if not schema:
            return
        connector = None
        try:
            connector = self._build_connector()
            tables = connector.list_tables(schema)
            self._table_combo.clear()
            self._table_combo.addItems(tables)
        except Exception:
            pass
        finally:
            if connector:
                connector.close()

    def _on_table_changed(self, table: str):
        if not table:
            self._headers = []
            self._columns_status_lbl.setText("")
            return
        if not self._query_mode_btn.isChecked():
            self._load_columns(silent=True)

    def _load_columns(self, silent: bool = False):
        connector = None
        try:
            import pandas as pd
            connector = self._build_connector()
            engine = connector._get_engine()
            if self._query_mode_btn.isChecked():
                query = self._custom_query.toPlainText().strip()
                if not query:
                    if not silent:
                        msg_warning(self, "Query Kosong", "Isi SQL query dulu.")
                    return
                df = pd.read_sql(f"SELECT * FROM ({query}) AS q LIMIT 0", engine)
                headers = list(df.columns)
                row_count_df = pd.read_sql(f"SELECT COUNT(*) AS n FROM ({query}) AS q", engine)
                row_count = int(row_count_df.iloc[0, 0])
            else:
                schema = self._schema_combo.currentText()
                table = self._table_combo.currentText()
                if not schema or not table:
                    return
                headers = connector.get_columns(schema, table)
                row_count_df = pd.read_sql(
                    f'SELECT COUNT(*) AS n FROM `{schema}`.`{table}`'
                    if self._db_type == "mysql"
                    else f'SELECT COUNT(*) AS n FROM "{schema}"."{table}"',
                    engine
                )
                row_count = int(row_count_df.iloc[0, 0])
            self._headers = headers
            self.headers_loaded.emit(headers)
            self._columns_status_lbl.setText(
                f"\u2713 {len(headers)} kolom dimuat \u00b7 {row_count:,} baris data"
            )
            if not silent:
                msg_info(self, "Data Dimuat", f"{len(headers)} kolom · {row_count:,} baris.")
        except Exception as e:
            self._columns_status_lbl.setText("")
            if not silent:
                msg_critical(self, "Gagal Load Kolom", str(e))
        finally:
            if connector:
                connector.close()

    # ------------------------------------------------------------------ connection builder

    def _build_profile(self):
        from models.connection_profile import ConnectionProfile
        p = ConnectionProfile()
        p.db_type  = self._db_type
        p.host     = self._host.text().strip()
        p.port     = int(self._port.text().strip() or ("3306" if self._db_type == "mysql" else "5432"))
        p.database = self._database.text().strip()
        p.username = self._username.text().strip()
        p.password = self._password.text()
        p.ssl_mode = self._ssl_combo.currentText()
        p.name     = f"{p.username}@{p.host}/{p.database}"
        p.use_ssh_tunnel = self._ssh_check.isChecked()
        p.ssh_host        = self._ssh_host.text().strip()
        p.ssh_port        = int(self._ssh_port.text().strip() or "22")
        p.ssh_user        = self._ssh_user.text().strip()
        p.ssh_auth_method = self._ssh_auth.currentText().lower()
        p.ssh_password    = self._ssh_password.text()
        p.ssh_key_path    = self._ssh_key_path.text().strip()
        return p

    def _build_connector(self):
        profile = self._build_profile()
        if self._db_type == "mysql":
            from services.mysql_connector import MySQLConnector
            return MySQLConnector.from_profile(profile)
        else:
            from services.postgres_connector import PostgresConnector
            return PostgresConnector.from_profile(profile)

    def _test_connection(self):
        connector = None
        try:
            profile = self._build_profile()
            if not profile.host or not profile.database:
                msg_warning(self, "Input Tidak Lengkap", "Isi Host dan Database terlebih dahulu.")
                return
            connector = self._build_connector()
            ok, msg_txt = connector.test_connection()
            if ok:
                schemas = connector.list_schemas()
                self._schema_combo.clear()
                self._schema_combo.addItems(schemas)
                msg_info(self, "Koneksi Berhasil \u2713", f"Terkoneksi!\n{msg_txt}")
            else:
                msg_critical(self, "Koneksi Gagal", msg_txt)
        except Exception as e:
            msg_critical(self, "Koneksi Error", str(e))
        finally:
            if connector:
                connector.close()

    def _save_connection(self):
        try:
            if msg_question(self, "Simpan Koneksi", "Simpan koneksi ini ke daftar saved connections?"):
                self._connection_store.save(self._build_profile())
                self._load_saved_connections()
                msg_info(self, "Tersimpan", "Profil koneksi berhasil disimpan.")
        except Exception as e:
            msg_critical(self, "Gagal Simpan", str(e))

    # ------------------------------------------------------------------ public API

    def get_headers(self) -> List[str]:
        return self._headers

    def get_source_config(self) -> DataSourceConfig:
        cfg = DataSourceConfig()
        cfg.source_type = self._db_type  # "postgresql" or "mysql"
        cfg.connection_id = self._saved_combo.currentData() or ""
        cfg.schema_name = self._schema_combo.currentText()
        cfg.table_name  = self._table_combo.currentText()
        cfg.use_custom_query = self._query_mode_btn.isChecked()
        cfg.custom_query = self._custom_query.toPlainText().strip()
        cfg.pg_connection_inline = self._build_profile().to_dict()
        return cfg

    def is_ready(self) -> bool:
        return bool(self._host.text().strip() and self._database.text().strip())


# ──────────────────────────────────────────────────────────────────────────────
# Step 2 — Import Files container
# ──────────────────────────────────────────────────────────────────────────────

class _Step2ImportFiles(QWidget):
    headers_left_loaded  = Signal(list)
    headers_right_loaded = Signal(list)

    def __init__(self, connection_store: "ConnectionStore", parent=None):
        super().__init__(parent)
        self._connection_store = connection_store
        self._job_type = JOB_TYPE_FILE_VS_FILE
        self._setup_ui()

    def _setup_ui(self):
        vl = QVBoxLayout(self)
        vl.setContentsMargins(36, 28, 36, 20)
        vl.setSpacing(0)

        bc = QLabel("New Compare Job  \u203a  Import Files")
        bc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        vl.addWidget(bc)
        vl.addSpacing(4)
        self._title = QLabel("Step 2 \u2014 Import Files & Select Sheets")
        self._title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 20px; font-weight: 700;")
        vl.addWidget(self._title)
        vl.addSpacing(20)

        self._cards_row = QHBoxLayout()
        self._cards_row.setSpacing(16)
        self._left_file_card  = _FileSourceCard("L", COLOR_PRIMARY)
        self._right_file_card = _FileSourceCard("R", COLOR_PURPLE)
        self._right_pg_card   = _PgConnectionCard(self._connection_store)
        self._left_db_card    = _DbSourceCard("L", self._connection_store)
        self._right_db_card   = _DbSourceCard("R", self._connection_store)

        self._left_file_card.headers_loaded.connect(self._on_left_headers)
        self._right_file_card.headers_loaded.connect(self._on_right_headers)
        self._right_pg_card.headers_loaded.connect(self._on_right_headers)
        self._left_db_card.headers_loaded.connect(self._on_left_headers)
        self._right_db_card.headers_loaded.connect(self._on_right_headers)

        self._cards_row.addWidget(self._left_file_card, 1)
        self._cards_row.addWidget(self._right_file_card, 1)
        self._cards_row.addWidget(self._right_pg_card, 1)
        self._cards_row.addWidget(self._left_db_card, 1)
        self._cards_row.addWidget(self._right_db_card, 1)
        self._right_pg_card.hide()
        self._left_db_card.hide()
        self._right_db_card.hide()
        vl.addLayout(self._cards_row, 1)
        vl.addSpacing(14)

        self._diff_banner = _InfoBanner(
            "Column names differ between sources. You will map them in the next step."
        )
        self._diff_banner.hide()
        vl.addWidget(self._diff_banner)

    def _on_left_headers(self, headers: list):
        self.headers_left_loaded.emit(headers)
        self._check_column_diff()

    def _on_right_headers(self, headers: list):
        self.headers_right_loaded.emit(headers)
        self._check_column_diff()

    def _check_column_diff(self):
        lh = self.get_left_headers()
        rh = self.get_right_headers()
        if lh and rh and set(lh) != set(rh):
            self._diff_banner.show()
        else:
            self._diff_banner.hide()

    def set_job_type(self, job_type: str):
        self._job_type = job_type
        is_ff  = job_type == JOB_TYPE_FILE_VS_FILE
        is_fpg = job_type == JOB_TYPE_FILE_VS_PG
        is_db  = job_type == JOB_TYPE_DB_VS_DB
        self._left_file_card.setVisible(is_ff or is_fpg)
        self._right_file_card.setVisible(is_ff)
        self._right_pg_card.setVisible(is_fpg)
        self._left_db_card.setVisible(is_db)
        self._right_db_card.setVisible(is_db)
        if is_ff:
            self._title.setText("Step 2 \u2014 Import Files & Select Sheets")
        elif is_fpg:
            self._title.setText("Step 2 \u2014 File Kiri & Koneksi Database Kanan")
        else:
            self._title.setText("Step 2 \u2014 Pilih Tabel / Query Database Kiri & Kanan")

    def get_left_source(self) -> DataSourceConfig:
        if self._job_type == JOB_TYPE_DB_VS_DB:
            return self._left_db_card.get_source_config()
        return self._left_file_card.get_source_config()

    def get_right_source(self) -> DataSourceConfig:
        if self._job_type == JOB_TYPE_FILE_VS_FILE:
            return self._right_file_card.get_source_config()
        if self._job_type == JOB_TYPE_DB_VS_DB:
            return self._right_db_card.get_source_config()
        return self._right_pg_card.get_source_config()

    def get_left_headers(self) -> List[str]:
        if self._job_type == JOB_TYPE_DB_VS_DB:
            return self._left_db_card.get_headers()
        return self._left_file_card.get_headers()

    def get_right_headers(self) -> List[str]:
        if self._job_type == JOB_TYPE_FILE_VS_FILE:
            return self._right_file_card.get_headers()
        if self._job_type == JOB_TYPE_DB_VS_DB:
            return self._right_db_card.get_headers()
        return self._right_pg_card.get_headers()

    def is_ready(self) -> bool:
        if self._job_type == JOB_TYPE_FILE_VS_FILE:
            return self._left_file_card.is_loaded() and self._right_file_card.is_loaded()
        if self._job_type == JOB_TYPE_DB_VS_DB:
            return self._left_db_card.is_ready() and self._right_db_card.is_ready()
        # FILE_VS_PG
        return self._left_file_card.is_loaded() and self._right_pg_card.is_ready()


# ──────────────────────────────────────────────────────────────────────────────
# Step 3 — Column Mapping
# ──────────────────────────────────────────────────────────────────────────────

class _Step3ColumnMapping(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._left_headers:  List[str] = []
        self._right_headers: List[str] = []
        self._row_order_mode: bool = False
        self._setup_ui()

    def _setup_ui(self):
        vl = QVBoxLayout(self)
        vl.setContentsMargins(36, 28, 36, 20)
        vl.setSpacing(0)

        bc = QLabel("New Compare Job  \u203a  Column Mapping")
        bc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        vl.addWidget(bc)
        vl.addSpacing(4)
        title = QLabel("Step 3 \u2014 Column Mapping & Key Selection")
        title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 20px; font-weight: 700;")
        vl.addWidget(title)
        vl.addSpacing(16)

        # Summary bar
        self._summary_bar = QFrame()
        self._summary_bar.setStyleSheet(
            f"background: {COLOR_PRIMARY_LIGHT}; border-radius: 8px;"
        )
        sb_hl = QHBoxLayout(self._summary_bar)
        sb_hl.setContentsMargins(14, 8, 14, 8)
        sb_hl.setSpacing(16)
        self._lbl_key_count = QLabel("0 key column selected")
        self._lbl_key_count.setStyleSheet(f"color: {COLOR_PRIMARY}; font-size: 12px; font-weight: 600;")
        sep1 = QLabel("|"); sep1.setStyleSheet(f"color: {COLOR_BORDER};")
        self._lbl_cmp_count = QLabel("0 columns will be compared")
        self._lbl_cmp_count.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 12px;")
        self._lbl_col_info = QLabel("")
        self._lbl_col_info.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        self._lbl_col_info.setAlignment(Qt.AlignmentFlag.AlignRight)
        sb_hl.addWidget(self._lbl_key_count)
        sb_hl.addWidget(sep1)
        sb_hl.addWidget(self._lbl_cmp_count)
        sb_hl.addStretch()
        sb_hl.addWidget(self._lbl_col_info)
        vl.addWidget(self._summary_bar)
        vl.addSpacing(10)

        # ── Match mode toggle ──
        toggle_row = QHBoxLayout()
        toggle_row.setSpacing(0)
        toggle_row.setContentsMargins(0, 0, 0, 0)
        lbl_mode = QLabel("Mode pencocokan baris:")
        lbl_mode.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px; margin-right: 8px;")
        toggle_row.addWidget(lbl_mode)
        self._btn_key_mode = QPushButton("\U0001f511  Pakai Key Column")
        self._btn_key_mode.setCheckable(True)
        self._btn_key_mode.setChecked(True)
        self._btn_key_mode.setFixedHeight(30)
        self._btn_row_mode = QPushButton("\U0001f522  Pakai Urutan Baris")
        self._btn_row_mode.setCheckable(True)
        self._btn_row_mode.setChecked(False)
        self._btn_row_mode.setFixedHeight(30)
        self._style_mode_buttons()
        self._btn_key_mode.clicked.connect(lambda: self._set_match_mode(False))
        self._btn_row_mode.clicked.connect(lambda: self._set_match_mode(True))
        toggle_row.addWidget(self._btn_key_mode)
        toggle_row.addWidget(self._btn_row_mode)
        toggle_row.addStretch()
        vl.addLayout(toggle_row)
        vl.addSpacing(10)

        # Mapping table
        # Kolom: 0=Hapus, 1=Kiri, 2=panah, 3=Kanan, 4=Key, 5=Compare
        self._table = QTableWidget()
        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels(
            ["", "SOURCE KIRI (FILE)", "", "SOURCE KANAN (FILE)", "KEY?", "COMPARE?"]
        )
        hh = self._table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        hh.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        hh.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        hh.setSectionResizeMode(5, QHeaderView.ResizeMode.Fixed)
        self._table.setColumnWidth(0, 36)
        self._table.setColumnWidth(2, 32)
        self._table.setColumnWidth(4, 72)
        self._table.setColumnWidth(5, 90)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self._table.verticalHeader().setVisible(False)
        self._table.setShowGrid(False)
        self._table.setStyleSheet(
            "QTableWidget { border: 1px solid #e2e8f0; border-radius: 8px; background: white; } "
            "QHeaderView::section { background: #f8fafc; border-bottom: 1px solid #e2e8f0; "
            "padding: 8px 10px; font-size: 12px; color: #64748b; font-weight: 600; border-right: none; }"
        )
        vl.addWidget(self._table, 1)
        vl.addSpacing(8)

        add_btn = QPushButton("+ Tambah mapping manual")
        add_btn.setStyleSheet(
            f"background: transparent; color: {COLOR_PRIMARY}; border: none; "
            "font-size: 13px; padding: 4px 0;"
        )
        add_btn.clicked.connect(self._add_manual_row)
        vl.addWidget(add_btn)
        vl.addSpacing(12)

        self._unmapped_banner = _InfoBanner(
            "Kolom yang tidak di-mapping akan di-skip\n"
            "Kolom yang tidak masuk daftar di atas tidak akan dibandingkan. "
            "Pastikan semua kolom yang relevan sudah ter-mapping."
        )
        vl.addWidget(self._unmapped_banner)

    def _style_mode_buttons(self):
        active_style = (
            f"background: {COLOR_PRIMARY}; color: white; border: 1px solid {COLOR_PRIMARY}; "
            "border-radius: 6px; font-size: 12px; font-weight: 600; padding: 0 14px;"
        )
        inactive_style = (
            f"background: white; color: {COLOR_TEXT_MUTED}; border: 1px solid {COLOR_BORDER}; "
            "border-radius: 6px; font-size: 12px; padding: 0 14px;"
        )
        if self._row_order_mode:
            self._btn_key_mode.setStyleSheet(inactive_style)
            self._btn_row_mode.setStyleSheet(active_style)
        else:
            self._btn_key_mode.setStyleSheet(active_style)
            self._btn_row_mode.setStyleSheet(inactive_style)

    def _set_match_mode(self, row_order: bool):
        self._row_order_mode = row_order
        self._btn_key_mode.setChecked(not row_order)
        self._btn_row_mode.setChecked(row_order)
        self._style_mode_buttons()
        # Tampilkan/sembunyikan kolom KEY berdasarkan mode
        self._table.setColumnHidden(4, row_order)
        self._update_summary()

    def get_use_row_order(self) -> bool:
        return self._row_order_mode

    def load_headers(self, left: List[str], right: List[str]):
        self._left_headers  = left
        self._right_headers = right
        self._table.setRowCount(0)
        right_rem = list(right)
        for lh in left:
            rh = lh if lh in right_rem else (right_rem[0] if right_rem else (right[0] if right else lh))
            if lh in right_rem:
                right_rem.remove(lh)
            self._add_row(lh, rh, is_key=False, compare=True)
        self._update_summary()
        self._lbl_col_info.setText(f"{len(left)} kol. kiri \u00b7 {len(right)} kol. kanan")

    def _add_row(self, left: str, right: str, is_key: bool = False, compare: bool = True):
        row = self._table.rowCount()
        self._table.insertRow(row)
        self._table.setRowHeight(row, 44)

        # Kolom 0 — tombol hapus
        del_btn = QPushButton("X")
        del_btn.setFixedSize(32, 28)
        del_btn.setToolTip("Hapus mapping ini")
        del_btn.setStyleSheet(
            "QPushButton { background: #fee2e2; color: #ef4444; border: 1px solid #fca5a5; "
            "border-radius: 5px; font-size: 13px; font-weight: 900; padding: 0; } "
            "QPushButton:hover { background: #ef4444; color: white; border-color: #ef4444; }"
        )
        del_btn.clicked.connect(lambda _, r=row: self._delete_row_by_btn(del_btn))
        del_wrap = QWidget(); del_hl = QHBoxLayout(del_wrap)
        del_hl.setContentsMargins(2, 0, 2, 0); del_hl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        del_hl.addWidget(del_btn)
        self._table.setCellWidget(row, 0, del_wrap)

        # Kolom 1 — dropdown kolom kiri
        left_combo = _NoScrollComboBox()
        left_combo.addItems(self._left_headers or [left])
        left_combo.setCurrentText(left)
        left_combo.setStyleSheet(
            "QComboBox { border: 1px solid #e2e8f0; border-radius: 5px; padding: 4px 8px; min-height: 28px; }"
        )
        left_combo.currentTextChanged.connect(self._update_summary)
        self._table.setCellWidget(row, 1, left_combo)

        # Kolom 2 — panah
        arrow = QLabel("\u2192")
        arrow.setAlignment(Qt.AlignmentFlag.AlignCenter)
        arrow.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 14px;")
        self._table.setCellWidget(row, 2, arrow)

        # Kolom 3 — dropdown kolom kanan
        right_combo = _NoScrollComboBox()
        right_combo.addItems(self._right_headers or [right])
        right_combo.setCurrentText(right)
        right_combo.setStyleSheet(
            "QComboBox { border: 1px solid #e2e8f0; border-radius: 5px; padding: 4px 8px; min-height: 28px; }"
        )
        right_combo.currentTextChanged.connect(self._update_summary)
        self._table.setCellWidget(row, 3, right_combo)

        # Kolom 4 — tombol Key
        key_btn = QPushButton("\U0001f511 Key" if is_key else "\U0001f511 \u2014")
        key_btn.setCheckable(True); key_btn.setChecked(is_key); key_btn.setFixedSize(62, 28)
        self._style_key_btn(key_btn)
        key_btn.clicked.connect(lambda _, b=key_btn: self._on_key_toggled(b))
        self._table.setCellWidget(row, 4, key_btn)

        # Kolom 5 — tombol Compare
        cmp_btn = QPushButton("\u2713 Ya" if compare else "\u2014 Tidak")
        cmp_btn.setCheckable(True); cmp_btn.setChecked(compare); cmp_btn.setFixedSize(72, 28)
        self._style_cmp_btn(cmp_btn)
        cmp_btn.clicked.connect(lambda _, b=cmp_btn: self._on_cmp_toggled(b))
        self._table.setCellWidget(row, 5, cmp_btn)

    def _style_key_btn(self, btn: QPushButton):
        if btn.isChecked():
            btn.setText("\U0001f511 Key")
            btn.setStyleSheet(
                "background: #f97316; color: white; border: none; "
                "border-radius: 5px; font-size: 11px; font-weight: 700;"
            )
        else:
            btn.setText("\U0001f511 \u2014")
            btn.setStyleSheet(
                f"background: white; color: {COLOR_TEXT_MUTED}; "
                f"border: 1px solid {COLOR_BORDER}; border-radius: 5px; font-size: 11px;"
            )

    def _style_cmp_btn(self, btn: QPushButton):
        if btn.isChecked():
            btn.setText("\u2713 Ya")
            btn.setStyleSheet(
                f"background: {COLOR_PRIMARY}; color: white; border: none; "
                "border-radius: 5px; font-size: 11px; font-weight: 700;"
            )
        else:
            btn.setText("\u2014 Tidak")
            btn.setStyleSheet(
                f"background: white; color: {COLOR_TEXT_MUTED}; "
                f"border: 1px solid {COLOR_BORDER}; border-radius: 5px; font-size: 11px;"
            )

    def _delete_row_by_btn(self, btn: QPushButton):
        """Cari baris yang mengandung tombol hapus ini, lalu hapus."""
        for r in range(self._table.rowCount()):
            wrap = self._table.cellWidget(r, 0)
            if wrap is not None:
                # tombol del ada di dalam QWidget wrapper
                del_child = wrap.findChild(QPushButton)
                if del_child is btn:
                    self._table.removeRow(r)
                    self._update_summary()
                    return

    def _on_key_toggled(self, btn: QPushButton):
        self._style_key_btn(btn); self._update_summary()

    def _on_cmp_toggled(self, btn: QPushButton):
        self._style_cmp_btn(btn); self._update_summary()

    def _add_manual_row(self):
        lh = self._left_headers[0] if self._left_headers else ""
        rh = self._right_headers[0] if self._right_headers else ""
        self._add_row(lh, rh, is_key=False, compare=True)
        self._update_summary()

    def _update_summary(self, *_):
        key_count = cmp_count = total = 0
        for r in range(self._table.rowCount()):
            total += 1
            kb = self._table.cellWidget(r, 4)
            cb = self._table.cellWidget(r, 5)
            if isinstance(kb, QPushButton) and kb.isChecked(): key_count += 1
            if isinstance(cb, QPushButton) and cb.isChecked(): cmp_count += 1
        if self._row_order_mode:
            self._lbl_key_count.setText("\U0001f522 Mode: Urutan Baris")
            self._lbl_key_count.setStyleSheet(f"color: #7c3aed; font-size: 12px; font-weight: 600;")
        else:
            self._lbl_key_count.setText(f"\U0001f511 {key_count} kolom key terpilih")
            self._lbl_key_count.setStyleSheet(f"color: {COLOR_PRIMARY}; font-size: 12px; font-weight: 600;")
        self._lbl_cmp_count.setText(
            f"{cmp_count} dari {total} kolom akan dibandingkan"
        )

    def get_key_mappings(self) -> List[ColumnMapping]:
        result = []
        for r in range(self._table.rowCount()):
            kb = self._table.cellWidget(r, 4)
            lc = self._table.cellWidget(r, 1)
            rc = self._table.cellWidget(r, 3)
            if isinstance(kb, QPushButton) and kb.isChecked() and isinstance(lc, QComboBox):
                result.append(ColumnMapping(lc.currentText(), rc.currentText()))
        return result

    def get_compare_mappings(self) -> List[ColumnMapping]:
        result = []
        for r in range(self._table.rowCount()):
            kb = self._table.cellWidget(r, 4)
            cb = self._table.cellWidget(r, 5)
            lc = self._table.cellWidget(r, 1)
            rc = self._table.cellWidget(r, 3)
            is_key = isinstance(kb, QPushButton) and kb.isChecked()
            is_cmp = isinstance(cb, QPushButton) and cb.isChecked()
            if not is_key and is_cmp and isinstance(lc, QComboBox):
                result.append(ColumnMapping(lc.currentText(), rc.currentText()))
        return result

    def validate(self) -> "Optional[str]":
        if not self.get_compare_mappings() and (self._row_order_mode or not self.get_key_mappings()):
            return (
                "Belum ada kolom yang akan dibandingkan.\n"
                "Pastikan minimal 1 kolom bertanda '\u2713 Ya'."
            )
        if not self._row_order_mode and not self.get_key_mappings():
            return (
                "Belum ada Key Column yang dipilih.\n"
                "Tandai minimal 1 kolom sebagai Key Column — ini yang dipakai untuk mencocokkan baris kiri dan kanan.\n"
                "Atau, pilih mode 'Pakai Urutan Baris' di atas jika tabel tidak memiliki kolom unik."
            )
        if not self.get_compare_mappings():
            return (
                "Belum ada kolom yang akan dibandingkan nilainya.\n"
                "Centang minimal 1 kolom di kolom 'Bandingkan'."
            )
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Step 4 — Compare Options
# ──────────────────────────────────────────────────────────────────────────────

class _NormToggleRow(QWidget):
    def __init__(self, title: str, desc: str, default: bool = False, parent=None):
        super().__init__(parent)
        hl = QHBoxLayout(self)
        hl.setContentsMargins(0, 6, 0, 6)
        hl.setSpacing(14)
        self._toggle = QCheckBox()
        self._toggle.setChecked(default)
        # Styling toggle switch-like
        self._toggle.setStyleSheet(
            f"QCheckBox::indicator {{ width: 38px; height: 20px; border-radius: 10px; "
            f"background: {COLOR_BORDER}; border: none; }}"
            f"QCheckBox::indicator:checked {{ background: {COLOR_PRIMARY}; }}"
        )
        hl.addWidget(self._toggle)
        text_vl = QVBoxLayout(); text_vl.setSpacing(1); text_vl.setContentsMargins(0,0,0,0)
        t1 = QLabel(title); t1.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        t2 = QLabel(desc);  t2.setStyleSheet(f"font-size: 12px; color: {COLOR_TEXT_MUTED};")
        text_vl.addWidget(t1); text_vl.addWidget(t2)
        hl.addLayout(text_vl); hl.addStretch()

    def is_checked(self) -> bool:
        return self._toggle.isChecked()


class _Step4Options(QWidget):
    def __init__(self, settings=None, parent=None):
        super().__init__(parent)
        self._settings = settings
        self._setup_ui()

    def _setup_ui(self):
        vl = QVBoxLayout(self)
        vl.setContentsMargins(36, 28, 36, 20)
        vl.setSpacing(16)

        bc = QLabel("New Compare Job  \u203a  Compare Options")
        bc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        vl.addWidget(bc)
        vl.addSpacing(4)
        title = QLabel("Step 4 \u2014 Konfigurasi Perbandingan & Normalisasi Data")
        title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 20px; font-weight: 700;")
        vl.addWidget(title)

        # Normalization card
        norm_card = _Card()
        n_vl = QVBoxLayout(norm_card)
        n_vl.setContentsMargins(20, 18, 20, 18)
        n_vl.setSpacing(0)
        norm_hdr = QHBoxLayout()
        norm_hdr.addWidget(QLabel("\u2699"))
        norm_title = QLabel("Normalisasi Data")
        norm_title.setStyleSheet(f"font-size: 14px; font-weight: 700; color: {COLOR_TEXT};")
        norm_hdr.addWidget(norm_title); norm_hdr.addStretch()
        n_vl.addLayout(norm_hdr)
        norm_sub = QLabel(
            "Pilihan di bawah akan diterapkan sebelum proses perbandingan, "
            "tujuannya agar perbedaan format penulisan tidak dianggap sebagai perbedaan data."
        )
        norm_sub.setWordWrap(True)
        norm_sub.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        n_vl.addWidget(norm_sub)
        n_vl.addSpacing(10)

        self._opt_trim = _NormToggleRow("Trim Whitespace", "Hapus spasi di awal dan akhir value (contoh: '  aktif  ' jadi 'aktif')", True)
        self._opt_case = _NormToggleRow("Ignore Case", "Abaikan perbedaan huruf besar/kecil (contoh: 'Aktif' dan 'aktif' dianggap sama)", True)
        self._opt_null = _NormToggleRow("Treat Empty as Null", "String kosong ('') dianggap sama dengan NULL", False)
        self._opt_date = _NormToggleRow("Normalize Date Format", "Samakan format tanggal sebelum dibandingkan (MM/DD vs YYYY-MM-DD, dll.)", True)
        self._opt_num  = _NormToggleRow("Normalize Number Format", "Abaikan trailing zero pada desimal (contoh: 1.50 dianggap sama dengan 1.5)", False)

        for w in [self._opt_trim, self._opt_case, self._opt_null, self._opt_date, self._opt_num]:
            n_vl.addWidget(w)
            n_vl.addWidget(_Divider())
        vl.addWidget(norm_card)

        # Global Transform Rules card
        tr_card = _Card()
        tr_vl = QVBoxLayout(tr_card)
        tr_vl.setContentsMargins(20, 18, 20, 18)
        tr_vl.setSpacing(0)
        tr_hdr = QHBoxLayout()
        tr_hdr.addWidget(QLabel("\U0001f504"))
        tr_title = QLabel("Global Transformation Rules")
        tr_title.setStyleSheet(f"font-size: 14px; font-weight: 700; color: {COLOR_TEXT};")
        tr_hdr.addWidget(tr_title)
        tr_hdr.addStretch()
        tr_vl.addLayout(tr_hdr)
        tr_sub = QLabel(
            "Terapkan aturan transformasi kolom global yang dikonfigurasi di Settings. "
            "Contoh: prefix/suffix, zero-pad (LPAD), strip karakter, replace teks."
        )
        tr_sub.setWordWrap(True)
        tr_sub.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        tr_vl.addWidget(tr_sub)
        tr_vl.addSpacing(10)
        self._opt_transforms = _NormToggleRow(
            "Terapkan Global Transformation Rules",
            "ON = rules dari Settings diterapkan pada kolom yang cocok namanya",
            True,
        )
        tr_vl.addWidget(self._opt_transforms)
        tr_vl.addWidget(_Divider())
        self._transform_rules_summary = QLabel("")
        self._transform_rules_summary.setWordWrap(True)
        self._transform_rules_summary.setStyleSheet(
            f"color: {COLOR_TEXT_MUTED}; font-size: 12px; padding: 4px 0;"
        )
        tr_vl.addWidget(self._transform_rules_summary)
        vl.addWidget(tr_card)
        self._refresh_transform_summary()

        sum_card = _Card()
        s_vl = QVBoxLayout(sum_card)
        s_vl.setContentsMargins(20, 18, 20, 18)
        s_vl.setSpacing(0)
        sum_title = QLabel("Ringkasan Job")
        sum_title.setStyleSheet(f"font-size: 14px; font-weight: 700; color: {COLOR_TEXT};")
        s_vl.addWidget(sum_title)
        s_vl.addSpacing(10)

        self._summary_rows: list = []
        for key in ["Nama Job", "Mode", "Source Kiri", "Source Kanan",
                    "Kolom Key", "Kolom yang\nDibandingkan", "Normalisasi"]:
            row_hl = QHBoxLayout()
            k_lbl = QLabel(key); k_lbl.setFixedWidth(140)
            k_lbl.setWordWrap(True)
            k_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px;")
            v_lbl = QLabel("\u2014"); v_lbl.setWordWrap(True)
            v_lbl.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 13px;")
            row_hl.addWidget(k_lbl); row_hl.addWidget(v_lbl, 1)
            self._summary_rows.append((k_lbl, v_lbl))
            s_vl.addLayout(row_hl)
            s_vl.addSpacing(6)
        vl.addWidget(sum_card)

        # Save as template
        tmpl_card = _Card()
        t_hl = QHBoxLayout(tmpl_card)
        t_hl.setContentsMargins(20, 14, 20, 14)
        t_hl.setSpacing(12)
        self._save_tmpl_toggle = QCheckBox()
        self._save_tmpl_toggle.setStyleSheet(
            f"QCheckBox::indicator {{ width: 38px; height: 20px; border-radius: 10px; "
            f"background: {COLOR_BORDER}; border: none; }}"
            f"QCheckBox::indicator:checked {{ background: {COLOR_PRIMARY}; }}"
        )
        t1 = QLabel("Simpan sebagai Template"); t1.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        t2 = QLabel("Konfigurasi ini bisa dipakai ulang untuk job berikutnya"); t2.setStyleSheet(f"font-size: 12px; color: {COLOR_TEXT_MUTED};")
        txt_vl = QVBoxLayout(); txt_vl.setSpacing(1); txt_vl.setContentsMargins(0,0,0,0)
        txt_vl.addWidget(t1); txt_vl.addWidget(t2)
        t_hl.addWidget(self._save_tmpl_toggle); t_hl.addLayout(txt_vl); t_hl.addStretch()
        self._tmpl_name_input = QLineEdit()
        self._tmpl_name_input.setPlaceholderText("Nama template...")
        self._tmpl_name_input.setFixedWidth(200); self._tmpl_name_input.setFixedHeight(34)
        self._tmpl_name_input.hide()
        self._save_tmpl_toggle.toggled.connect(self._tmpl_name_input.setVisible)
        t_hl.addWidget(self._tmpl_name_input)
        vl.addWidget(tmpl_card)
        vl.addStretch()

    def update_summary(self, job_name, mode, left_desc, right_desc, key_cols, cmp_cols):
        values = [job_name, mode, left_desc, right_desc, key_cols, cmp_cols, "\u2014"]
        for (_, v), val in zip(self._summary_rows, values):
            v.setText(val)

    def update_normalizations_label(self):
        active = []
        if self._opt_trim.is_checked(): active.append("Trim whitespace")
        if self._opt_case.is_checked(): active.append("Ignore case")
        if self._opt_null.is_checked(): active.append("Empty = null")
        if self._opt_date.is_checked(): active.append("Normalize date")
        if self._opt_num.is_checked():  active.append("Normalize number")
        if self._opt_transforms.is_checked():
            active.append("Global transforms")
        self._summary_rows[6][1].setText(", ".join(active) if active else "Tidak ada")

    def _refresh_transform_summary(self):
        """Tampilkan ringkasan global rules yang aktif."""
        if self._settings is None:
            self._transform_rules_summary.setText("(Settings tidak tersedia)")
            return
        try:
            rules = self._settings.get_transform_rules()
        except Exception:
            rules = []
        active = [r for r in rules if r.enabled]
        if not rules:
            txt = "Belum ada rule — konfigurasi di Settings > Transformasi Kolom"
        elif not active:
            txt = f"{len(rules)} rule terdaftar, semua nonaktif"
        else:
            names = ", ".join(f"{r.column_name} ({r.transform_type})" for r in active[:5])
            suffix = f" +{len(active)-5} lainnya" if len(active) > 5 else ""
            txt = f"{len(active)} rule aktif: {names}{suffix}"
        self._transform_rules_summary.setText(txt)

    def get_options(self) -> CompareOptions:
        return CompareOptions(
            trim_whitespace=self._opt_trim.is_checked(),
            ignore_case=self._opt_case.is_checked(),
            treat_empty_as_null=self._opt_null.is_checked(),
            normalize_date=self._opt_date.is_checked(),
            normalize_number=self._opt_num.is_checked(),
            apply_global_transforms=self._opt_transforms.is_checked(),
        )

    def should_save_template(self) -> bool:
        return self._save_tmpl_toggle.isChecked()

    def get_template_name(self) -> str:
        return self._tmpl_name_input.text().strip()


# ──────────────────────────────────────────────────────────────────────────────
# NewJobPage — main wizard container
# ──────────────────────────────────────────────────────────────────────────────

class NewJobPage(QWidget):
    """Wizard 4-langkah untuk buat job perbandingan baru."""

    job_submitted = Signal(CompareJob, CompareConfig)

    _STEP_COUNT = 4

    def __init__(
        self,
        settings: "AppSettings",
        job_manager: "JobManager",
        connection_store: "ConnectionStore",
        template_manager: "Optional[TemplateManager]" = None,
        parent=None,
    ):
        super().__init__(parent)
        self._settings         = settings
        self._job_manager      = job_manager
        self._connection_store = connection_store
        self._template_manager = template_manager
        self._current_step     = 0
        self._setup_ui()

    def _setup_ui(self):
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Sidebar step indicator
        self._step_indicator = _SideStepIndicator()
        root.addWidget(self._step_indicator)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setStyleSheet(f"color: {COLOR_BORDER};")
        sep.setFixedWidth(1)
        root.addWidget(sep)

        # Content
        content_widget = QWidget()
        content_widget.setStyleSheet(f"background: {COLOR_BG};")
        content_vl = QVBoxLayout(content_widget)
        content_vl.setContentsMargins(0, 0, 0, 0)
        content_vl.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setStyleSheet("background: transparent;")

        self._step_stack = QStackedWidget()
        self._step_stack.setStyleSheet("background: transparent;")

        self._step1 = _Step1SelectSource(self._template_manager)
        self._step2 = _Step2ImportFiles(self._connection_store)
        self._step3 = _Step3ColumnMapping()
        self._step4 = _Step4Options(settings=self._settings)

        for w in [self._step1, self._step2, self._step3, self._step4]:
            self._step_stack.addWidget(w)

        self._step1.mode_changed.connect(self._step2.set_job_type)
        self._step2.headers_left_loaded.connect(lambda h: None)
        self._step2.headers_right_loaded.connect(lambda h: None)

        scroll.setWidget(self._step_stack)
        content_vl.addWidget(scroll, 1)

        # Navigation bar
        nav_frame = QFrame()
        nav_frame.setStyleSheet(f"background: white; border-top: 1px solid {COLOR_BORDER};")
        nav_hl = QHBoxLayout(nav_frame)
        nav_hl.setContentsMargins(36, 14, 36, 14)
        nav_hl.setSpacing(12)

        self._btn_cancel = QPushButton("\u2190 Cancel")
        self._btn_cancel.setObjectName("secondaryBtn")
        self._btn_cancel.setFixedHeight(38)
        self._btn_cancel.clicked.connect(self._on_cancel)

        self._btn_back = QPushButton("\u2190 Back")
        self._btn_back.setObjectName("secondaryBtn")
        self._btn_back.setFixedHeight(38)
        self._btn_back.hide()
        self._btn_back.clicked.connect(self._go_back)

        self._btn_next = QPushButton("Continue \u2192")
        self._btn_next.setFixedHeight(38)
        self._btn_next.setFixedWidth(200)
        self._btn_next.setStyleSheet(
            "background-color: #2563eb; color: #ffffff; font-weight: 600;"
            "border-radius: 6px; border: none;"
        )
        self._btn_next.clicked.connect(self._go_next)

        self._btn_run = QPushButton("\u25b6  Jalankan perbandingan data")
        self._btn_run.setFixedHeight(38)
        self._btn_run.setFixedWidth(250)
        self._btn_run.setStyleSheet(
            f"background: {COLOR_SUCCESS}; color: white; font-weight: 700; border-radius: 6px;"
        )
        self._btn_run.hide()
        self._btn_run.clicked.connect(self._submit_job)

        nav_hl.addWidget(self._btn_cancel)
        nav_hl.addWidget(self._btn_back)
        nav_hl.addStretch()
        nav_hl.addWidget(self._btn_next)
        nav_hl.addWidget(self._btn_run)
        content_vl.addWidget(nav_frame)
        root.addWidget(content_widget, 1)

        self._update_step_ui()

    # ── navigation ──

    def _go_next(self):
        err = self._validate()
        if err:
            msg_warning(self, "Belum Lengkap", err)
            return
        if self._current_step == 1:
            lh = self._step2.get_left_headers()
            rh = self._step2.get_right_headers()
            if not lh or not rh:
                msg_warning(
                    self, "Kolom Belum Dimuat",
                    "Pastikan kedua sumber data sudah dipilih dan kolom berhasil dimuat."
                )
                return
            self._step3.load_headers(lh, rh)
        elif self._current_step == 2:
            self._refresh_step4_summary()
        self._current_step += 1
        self._update_step_ui()

    def _go_back(self):
        if self._current_step > 0:
            self._current_step -= 1
            self._update_step_ui()

    def _on_cancel(self):
        parent = self.parent()
        while parent:
            if hasattr(parent, "_navigate_to"):
                parent._navigate_to("dashboard")
                return
            parent = parent.parent() if parent else None

    def _update_step_ui(self):
        self._step_stack.setCurrentIndex(self._current_step)
        self._step_indicator.set_step(self._current_step)
        is_first = self._current_step == 0
        is_last  = self._current_step == self._STEP_COUNT - 1
        self._btn_cancel.setVisible(is_first)
        self._btn_back.setVisible(not is_first)
        self._btn_next.setVisible(not is_last)
        self._btn_run.setVisible(is_last)
        labels = [
            "Lanjut \u2192",
            "Lanjut ke Mapping \u2192",
            "Lanjut ke Options \u2192",
        ]
        if self._current_step < len(labels):
            self._btn_next.setText(labels[self._current_step])

    # ── validation ──

    def _validate(self) -> "Optional[str]":
        if self._current_step == 0:
            if not self._step1.get_job_name():
                return "Nama job tidak boleh kosong. Isi dulu kolom 'Nama Job' ya."
        elif self._current_step == 2:
            return self._step3.validate()
        return None

    # ── summary ──

    def _refresh_step4_summary(self):
        left_cfg  = self._step2.get_left_source()
        right_cfg = self._step2.get_right_source()
        mode_label = "File vs File" if self._step1.get_job_type() == JOB_TYPE_FILE_VS_FILE else "File vs PostgreSQL"

        def _desc(cfg: DataSourceConfig) -> str:
            if cfg.source_type in ("excel", "csv"):
                p  = os.path.basename(cfg.file_path) if cfg.file_path else "\u2014"
                sh = f" \u2014 {cfg.sheet_name}" if cfg.sheet_name else ""
                return f"{p}{sh}"
            return f"{cfg.schema_name}.{cfg.table_name} (PostgreSQL)"

        key_maps = self._step3.get_key_mappings()
        cmp_maps = self._step3.get_compare_mappings()
        key_str  = ", ".join(f"{m.left_col} \u2192 {m.right_col}" for m in key_maps) or "\u2014"
        cmp_str  = (
            ", ".join(m.left_col for m in cmp_maps[:4])
            + (f" (+{len(cmp_maps)-4} lainnya)" if len(cmp_maps) > 4 else "")
        ) if cmp_maps else "\u2014"

        self._step4.update_summary(
            job_name=self._step1.get_job_name(),
            mode=mode_label,
            left_desc=_desc(left_cfg),
            right_desc=_desc(right_cfg),
            key_cols=key_str,
            cmp_cols=f"{cmp_str} ({len(cmp_maps)} kolom)",
        )
        self._step4.update_normalizations_label()

    # ── submit ──

    def _submit_job(self):
        # Validasi step 3 sebelum submit
        step3_err = self._step3.validate()
        if step3_err:
            msg_warning(self, "Konfigurasi Belum Lengkap", step3_err)
            return
        self._step4.update_normalizations_label()
        config = CompareConfig()
        config.job_type        = self._step1.get_job_type()
        config.left_source     = self._step2.get_left_source()
        config.right_source    = self._step2.get_right_source()
        config.key_columns     = self._step3.get_key_mappings()
        config.compare_columns = self._step3.get_compare_mappings()
        config.options         = self._step4.get_options()
        config.use_row_order   = self._step3.get_use_row_order()

        job = CompareJob(
            name=self._step1.get_job_name(),
            job_type=config.job_type,
            status=JOB_STATUS_QUEUED,
            config=config.to_dict(),
        )
        self._job_manager.save(job)

        if self._step4.should_save_template() and self._template_manager:
            from models.template import CompareTemplate
            tmpl = CompareTemplate(
                name=self._step4.get_template_name() or job.name,
                config=config.to_dict(),
            )
            self._template_manager.save(tmpl)

        self.job_submitted.emit(job, config)
        self._reset()

    def _reset(self):
        self._current_step = 0
        self._update_step_ui()

    # ── public API ──

    def set_initial_job_type(self, job_type: str):
        self._step1.set_job_type(job_type)
        self._step2.set_job_type(job_type)
