# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
ui/pages/result_page.py

Halaman hasil perbandingan — terdiri dari dua sub-view:
  _SummaryView : 5 kartu status, distribusi bar, breakdown mismatch per kolom, detail job
  _DetailView  : tabel dinamis per pemetaan kolom, filter status, pencarian key, pagination

Semua data dibaca dari file DuckDB per-job, tidak di-load semua ke memori sekaligus.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Optional, Dict, Any, TYPE_CHECKING

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QTableWidget, QTableWidgetItem, QHeaderView,
    QLineEdit, QFileDialog, QScrollArea, QMessageBox,
    QSizePolicy, QAbstractItemView, QStackedWidget, QTextEdit,
    QProgressBar, QDialog, QProgressDialog,
)

from config.constants import (
    RESULT_MATCH, RESULT_MISMATCH,
    RESULT_MISSING_LEFT, RESULT_MISSING_RIGHT, RESULT_DUPLICATE_KEY,
    RESULT_STATUS_LABELS, RESULT_STATUS_COLORS,
    JOB_STATUS_PROCESSING, JOB_STATUS_QUEUED,
    JOB_TYPE_LABELS,
)
from ui.components.pagination_widget import PaginationWidget
from ui.styles import (
    COLOR_PRIMARY, COLOR_PRIMARY_LIGHT,
    COLOR_TEXT, COLOR_TEXT_MUTED, COLOR_TEXT_LIGHT,
    COLOR_CARD_BG, COLOR_BORDER, COLOR_BG,
    COLOR_SUCCESS, COLOR_DANGER, COLOR_WARNING, COLOR_PURPLE, COLOR_ORANGE,
    msg_info, msg_critical,
)

if TYPE_CHECKING:
    from config.settings import AppSettings
    from models.job import CompareJob
    from storage.result_repository import ResultRepository


# ─── helpers ───────────────────────────────────────────────────────────────────

_STATUS_ROW_BG = {
    RESULT_MATCH:         "#f0fdf4",
    RESULT_MISMATCH:      "#fef2f2",
    RESULT_MISSING_LEFT:  "#fff7ed",
    RESULT_MISSING_RIGHT: "#faf5ff",
    RESULT_DUPLICATE_KEY: "#fffbeb",
}

_STATUS_BADGE_STYLE = {
    RESULT_MATCH:         f"background:#dcfce7;color:#15803d;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:700;",
    RESULT_MISMATCH:      f"background:#fee2e2;color:#dc2626;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:700;",
    RESULT_MISSING_LEFT:  f"background:#ffedd5;color:#c2410c;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:700;",
    RESULT_MISSING_RIGHT: f"background:#f3e8ff;color:#7e22ce;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:700;",
    RESULT_DUPLICATE_KEY: f"background:#fef9c3;color:#a16207;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:700;",
}


def _fmt(n: int) -> str:
    return f"{n:,}"


def _pct(value: int, total: int) -> str:
    return f"{value / total * 100:.1f}%" if total else "0.0%"


def _card_frame() -> QFrame:
    f = QFrame()
    f.setObjectName("card")
    return f


# ─── Format picker dialog ────────────────────────────────────────────────────

class _ExportExpectedDialog(QDialog):
    """
    Dialog pemilihan format untuk download ekspektasi migrasi.
    Menampilkan dua opsi (Excel dan CSV) beserta keterangan singkat.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._fmt: Optional[str] = None
        self.setWindowTitle("Download Ekspektasi Migrasi")
        self.setFixedWidth(480)
        self.setModal(True)
        self._setup_ui()

    def _setup_ui(self):
        vl = QVBoxLayout(self)
        vl.setContentsMargins(24, 20, 24, 18)
        vl.setSpacing(14)

        title = QLabel("\u2B07\u00A0\u00A0Download Ekspektasi Migrasi")
        title.setStyleSheet(
            f"font-size: 15px; font-weight: 700; color: {COLOR_TEXT};"
        )
        vl.addWidget(title)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color: {COLOR_BORDER};")
        vl.addWidget(sep)

        desc = QLabel(
            "Hasilkan file berisi data sumber yang sudah diproses — normalisasi, "
            "transform rules, dan ekspansi grup — dengan nama kolom sesuai format TARGET.\n"
            "Gunakan untuk validasi manual atau VLOOKUP dengan data hasil migrasi."
        )
        desc.setWordWrap(True)
        desc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        vl.addWidget(desc)

        fmt_lbl = QLabel("Pilih format output:")
        fmt_lbl.setStyleSheet(
            f"color: {COLOR_TEXT}; font-size: 12px; font-weight: 600; margin-top: 2px;"
        )
        vl.addWidget(fmt_lbl)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(10)

        self._btn_xlsx = QPushButton("\U0001f4ca\u00A0\u00A0Excel (.xlsx)")
        self._btn_xlsx.setFixedHeight(48)
        self._btn_xlsx.setToolTip(
            "Format tabel dengan header berwarna.\n"
            "Maks. ~1 juta baris (batas aplikasi Excel)."
        )
        self._btn_xlsx.setStyleSheet(
            "QPushButton { background: #eff6ff; color: #1d4ed8; "
            "border: 2px solid #3b82f6; border-radius: 8px; "
            "font-size: 13px; font-weight: 600; padding: 0 20px; }"
            "QPushButton:hover { background: #dbeafe; border-color: #1d4ed8; }"
        )
        self._btn_xlsx.clicked.connect(lambda: self._pick("xlsx"))

        self._btn_csv = QPushButton("\U0001f4c4\u00A0\u00A0CSV (utf-8)")
        self._btn_csv.setFixedHeight(48)
        self._btn_csv.setToolTip(
            "Format CSV dengan BOM utf-8 — langsung terbaca benar di Excel.\n"
            "Tidak ada batas jumlah baris. Optimal untuk data jutaan baris."
        )
        self._btn_csv.setStyleSheet(
            "QPushButton { background: #f0fdf4; color: #15803d; "
            "border: 2px solid #22c55e; border-radius: 8px; "
            "font-size: 13px; font-weight: 600; padding: 0 20px; }"
            "QPushButton:hover { background: #dcfce7; border-color: #15803d; }"
        )
        self._btn_csv.clicked.connect(lambda: self._pick("csv"))

        btn_row.addWidget(self._btn_xlsx, 1)
        btn_row.addWidget(self._btn_csv, 1)
        vl.addLayout(btn_row)

        note = QLabel(
            "\u26a0\ufe0f  Excel mendukung maks. \u00b11 juta baris. "
            "Untuk data lebih besar, pilih CSV."
        )
        note.setWordWrap(True)
        note.setStyleSheet(
            f"color: #92400e; font-size: 11px; background: #fffbeb; "
            f"border: 1px solid #fcd34d; border-radius: 4px; padding: 6px 8px;"
        )
        vl.addWidget(note)

        cancel_row = QHBoxLayout()
        cancel_row.addStretch()
        batal = QPushButton("Batal")
        batal.setObjectName("secondaryBtn")
        batal.setFixedWidth(80)
        batal.clicked.connect(self.reject)
        cancel_row.addWidget(batal)
        vl.addLayout(cancel_row)

    def _pick(self, fmt: str):
        self._fmt = fmt
        self.accept()

    @property
    def selected_fmt(self) -> Optional[str]:
        return self._fmt


# ─── Left navigation panel ────────────────────────────────────────────────────

class _LeftNav(QWidget):
    """Panel navigasi kiri — Summary | Detail | ← Dashboard."""

    switched    = Signal(int)   # 0=summary, 1=detail
    go_back     = Signal()

    _ITEMS = [(0, "\U0001f4ca", "Hasil Konversi"), (1, "\U0001f4cb", "Cek Detail")]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(192)
        self.setStyleSheet(f"background: {COLOR_CARD_BG}; border-right: 1px solid {COLOR_BORDER};")
        vl = QVBoxLayout(self)
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(0)

        hdr = QLabel("HASIL PERBANDINGAN")
        hdr.setStyleSheet(
            f"color: {COLOR_TEXT_MUTED}; font-size: 10px; font-weight: 700; "
            "letter-spacing: 1px; padding: 20px 20px 12px 20px; background: transparent;"
        )
        vl.addWidget(hdr)

        self._btns: List[QPushButton] = []
        for idx, icon, label in self._ITEMS:
            btn = QPushButton(f"  {icon}  {label}")
            btn.setCheckable(True)
            btn.setFixedHeight(40)
            btn.setProperty("navIdx", idx)
            btn.clicked.connect(lambda _checked, i=idx: self._click(i))
            self._btns.append(btn)
            vl.addWidget(btn)

        vl.addStretch()

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color: {COLOR_BORDER}; margin: 0;")
        vl.addWidget(sep)

        back_btn = QPushButton("  \u2190  Dashboard")
        back_btn.setFixedHeight(40)
        back_btn.setStyleSheet(
            f"background: transparent; color: {COLOR_TEXT_MUTED}; border: none; "
            "text-align: left; padding: 0 16px; font-size: 12px;"
        )
        back_btn.clicked.connect(self.go_back)
        vl.addWidget(back_btn)

        self.set_active(0)

    def _btn_style(self, active: bool) -> str:
        if active:
            return (
                f"background: {COLOR_PRIMARY_LIGHT}; color: {COLOR_PRIMARY}; border: none; "
                "text-align: left; padding: 0 16px; font-size: 13px; font-weight: 600; "
                f"border-right: 3px solid {COLOR_PRIMARY};"
            )
        return (
            f"background: transparent; color: {COLOR_TEXT}; border: none; "
            "text-align: left; padding: 0 16px; font-size: 13px;"
        )

    def _click(self, idx: int):
        self.set_active(idx)
        self.switched.emit(idx)

    def set_active(self, idx: int):
        for btn in self._btns:
            active = btn.property("navIdx") == idx
            btn.setChecked(active)
            btn.setStyleSheet(self._btn_style(active))


# ─── Progress view ─────────────────────────────────────────────────────────────

class _ProgressView(QWidget):
    cancel_clicked = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        vl = QVBoxLayout(self)
        vl.setContentsMargins(40, 40, 40, 40)
        vl.setSpacing(16)

        hdr = QHBoxLayout()
        self._title = QLabel("Menjalankan perbandingan...")
        self._title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 18px; font-weight: 700;")
        hdr.addWidget(self._title)
        hdr.addStretch()
        cancel = QPushButton("\u2715  Cancel Job")
        cancel.setObjectName("dangerBtn")
        cancel.clicked.connect(self.cancel_clicked)
        hdr.addWidget(cancel)
        vl.addLayout(hdr)

        self._step = QLabel("Inisialisasi...")
        self._step.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px;")
        vl.addWidget(self._step)

        self._bar = QProgressBar()
        self._bar.setRange(0, 0)
        self._bar.setFixedHeight(12)
        self._bar.setStyleSheet(
            f"QProgressBar {{ border: none; border-radius: 6px; background: {COLOR_BORDER}; }}"
            f"QProgressBar::chunk {{ border-radius: 6px; background: {COLOR_PRIMARY}; }}"
        )
        vl.addWidget(self._bar)

        self._row_info = QLabel("")
        self._row_info.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        vl.addWidget(self._row_info)

        log_hdr = QLabel("Process Log")
        log_hdr.setStyleSheet(f"font-size: 12px; font-weight: 600; color: {COLOR_TEXT};")
        vl.addWidget(log_hdr)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setStyleSheet(
            "QTextEdit { background: #0f172a; color: #94a3b8; "
            "font-family: 'Consolas','Courier New',monospace; "
            "font-size: 12px; border-radius: 8px; padding: 12px; }"
        )
        vl.addWidget(self._log, 1)

    def set_title(self, txt: str):
        self._title.setText(txt)

    def update_progress(self, step: str, done: int, total: int):
        self._step.setText(step)
        if total > 0:
            self._bar.setRange(0, total)
            self._bar.setValue(done)
            self._row_info.setText(f"{_fmt(done)} / {_fmt(total)} baris")
        else:
            self._bar.setRange(0, 0)
            self._row_info.setText(f"{_fmt(done)} baris..." if done else "")

    def append_log(self, msg: str):
        self._log.append(msg)
        sb = self._log.verticalScrollBar()
        sb.setValue(sb.maximum())


# ─── Distribution bar ─────────────────────────────────────────────────────────

class _DistributionBar(QWidget):
    """Bar proporsional berdasarkan jumlah tiap status."""

    _COLORS = {
        RESULT_MATCH:         COLOR_SUCCESS,
        RESULT_MISMATCH:      COLOR_DANGER,
        RESULT_MISSING_LEFT:  COLOR_ORANGE,
        RESULT_MISSING_RIGHT: "#fb923c",
        RESULT_DUPLICATE_KEY: COLOR_PURPLE,
    }

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(32)
        self._segments: List[tuple] = []

    def set_data(self, summary: dict):
        total = summary.get("total_rows", 0) or 1
        self._segments = []
        for s in [RESULT_MATCH, RESULT_MISMATCH, RESULT_MISSING_LEFT,
                  RESULT_MISSING_RIGHT, RESULT_DUPLICATE_KEY]:
            cnt = summary.get(s, 0)
            if cnt > 0:
                self._segments.append((s, cnt / total * 100))
        self.update()

    def paintEvent(self, event):
        from PySide6.QtGui import QPainter, QBrush, QColor as QC, QPen
        if not self._segments:
            return
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        w, h, x = self.width(), self.height(), 0
        for i, (status, pct) in enumerate(self._segments):
            seg_w = int(pct / 100 * w) if i < len(self._segments) - 1 else (w - x)
            p.setBrush(QBrush(QC(self._COLORS.get(status, "#94a3b8"))))
            p.setPen(QPen(Qt.PenStyle.NoPen))
            p.drawRect(x, 0, seg_w, h)
            if seg_w > 38:
                p.setPen(QC("#ffffff"))
                p.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                p.drawText(x + 4, 0, seg_w - 8, h, Qt.AlignmentFlag.AlignCenter, f"{pct:.0f}%")
            x += seg_w
        p.end()


# ─── Summary card ─────────────────────────────────────────────────────────────

class _SummaryCard(QFrame):
    filter_clicked = Signal(str)

    def __init__(self, status: str, parent=None):
        super().__init__(parent)
        self._status = status
        self.setObjectName("card")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        color = RESULT_STATUS_COLORS.get(status, COLOR_TEXT_MUTED)
        vl = QVBoxLayout(self)
        vl.setContentsMargins(16, 14, 16, 14)
        vl.setSpacing(4)
        lbl = QLabel(RESULT_STATUS_LABELS.get(status, status))
        lbl.setStyleSheet(f"color: {color}; font-size: 12px; font-weight: 600;")
        vl.addWidget(lbl)
        self._count = QLabel("0")
        self._count.setStyleSheet(f"color: {color}; font-size: 28px; font-weight: 700;")
        vl.addWidget(self._count)
        self._pct = QLabel("0.0%")
        self._pct.setStyleSheet(f"color: {color}; font-size: 12px;")
        vl.addWidget(self._pct)
        self._note = QLabel("")
        self._note.setWordWrap(True)
        self._note.setStyleSheet(
            f"color: {COLOR_TEXT_MUTED}; font-size: 11px; margin-top: 4px;"
        )
        self._note.hide()
        vl.addWidget(self._note)

    def update_data(self, count: int, total: int):
        self._count.setText(_fmt(count))
        self._pct.setText(_pct(count, total))

    def update_note(self, text: str):
        self._note.setText(text)
        self._note.setVisible(bool(text))

    def mousePressEvent(self, event):
        self.filter_clicked.emit(self._status)
        super().mousePressEvent(event)


# ─── Summary view ─────────────────────────────────────────────────────────────

class _SummaryView(QWidget):
    view_detail_requested    = Signal()
    rerun_requested          = Signal()
    export_excel_requested   = Signal()
    export_csv_requested     = Signal()
    export_expected_requested = Signal()   # download ekspektasi migrasi
    filter_detail             = Signal(str)   # status string → buka detail ter-filter

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget()
        inner.setStyleSheet(f"background: {COLOR_BG};")
        vl = QVBoxLayout(inner)
        vl.setContentsMargins(32, 24, 32, 20)
        vl.setSpacing(16)

        # ── header ──
        hdr = QHBoxLayout()
        self._bc = QLabel("JOB-000  \u203a  Hasil Konversi")
        self._bc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        hdr.addWidget(self._bc)
        hdr.addStretch()
        self._btn_xls = QPushButton("\U0001f4c4  Export Excel")
        self._btn_xls.setObjectName("secondaryBtn")
        self._btn_xls.clicked.connect(self.export_excel_requested)
        self._btn_csv = QPushButton("\u2193  Export CSV")
        self._btn_csv.setObjectName("secondaryBtn")
        self._btn_csv.clicked.connect(self.export_csv_requested)
        self._btn_exp = QPushButton("\u2B07  Ekspektasi")
        self._btn_exp.setToolTip(
            "Download file ekspektasi migrasi — data sumber yang sudah diproses\n"
            "(normalisasi + transform) dengan nama kolom TARGET.\n"
            "Cocok untuk validasi manual atau VLOOKUP."
        )
        self._btn_exp.setStyleSheet(
            "QPushButton { background: #f5f3ff; color: #7c3aed; "
            "border: 1px solid #a78bfa; border-radius: 6px; "
            "padding: 4px 12px; font-size: 12px; font-weight: 600; }"
            "QPushButton:hover { background: #ede9fe; border-color: #7c3aed; }"
            "QPushButton:disabled { background: #f1f5f9; color: #94a3b8; "
            "border-color: #e2e8f0; }"
        )
        self._btn_exp.clicked.connect(self.export_expected_requested)
        hdr.addWidget(self._btn_exp)
        hdr.addWidget(self._btn_xls)
        hdr.addWidget(self._btn_csv)
        vl.addLayout(hdr)

        self._title = QLabel("")
        self._title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 22px; font-weight: 700;")
        vl.addWidget(self._title)
        self._meta = QLabel("")
        self._meta.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px;")
        vl.addWidget(self._meta)

        # ── 5 summary cards ──
        cards_row = QHBoxLayout()
        cards_row.setSpacing(12)
        self._cards: Dict[str, _SummaryCard] = {}
        for s in [RESULT_MATCH, RESULT_MISMATCH, RESULT_MISSING_LEFT,
                  RESULT_MISSING_RIGHT, RESULT_DUPLICATE_KEY]:
            c = _SummaryCard(s)
            c.filter_clicked.connect(self._on_card_clicked)
            c.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            cards_row.addWidget(c)
            self._cards[s] = c
        vl.addLayout(cards_row)

        # ── distribution bar card ──
        dist_card = _card_frame()
        dc_vl = QVBoxLayout(dist_card)
        dc_vl.setContentsMargins(20, 16, 20, 16)
        dc_vl.setSpacing(10)
        dist_title = QLabel("Distribution Overview")
        dist_title.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        dc_vl.addWidget(dist_title)
        self._dist_bar = _DistributionBar()
        dc_vl.addWidget(self._dist_bar)
        legend = QHBoxLayout()
        legend.setSpacing(16)
        for s, color in RESULT_STATUS_COLORS.items():
            row_w = QWidget()
            row_hl = QHBoxLayout(row_w)
            row_hl.setContentsMargins(0, 0, 0, 0)
            row_hl.setSpacing(4)
            dot = QLabel("\u25cf")
            dot.setStyleSheet(f"color: {color}; font-size: 12px;")
            txt = QLabel(RESULT_STATUS_LABELS.get(s, s))
            txt.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
            row_hl.addWidget(dot)
            row_hl.addWidget(txt)
            legend.addWidget(row_w)
        legend.addStretch()
        dc_vl.addLayout(legend)
        vl.addWidget(dist_card)

        # ── lower 2-column ──
        bot = QHBoxLayout()
        bot.setSpacing(16)

        # Mismatch breakdown
        bd_card = _card_frame()
        bd_vl = QVBoxLayout(bd_card)
        bd_vl.setContentsMargins(20, 16, 20, 18)
        bd_vl.setSpacing(10)
        bd_hdr = QHBoxLayout()
        bd_title = QLabel("\u2297  Mismatch Breakdown by Kolom")
        bd_title.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        bd_hdr.addWidget(bd_title)
        bd_hdr.addStretch()
        detail_lnk = QPushButton("View detail \u203a")
        detail_lnk.setStyleSheet(
            f"background: transparent; color: {COLOR_PRIMARY}; border: none; font-size: 12px;"
        )
        detail_lnk.clicked.connect(self.view_detail_requested)
        bd_hdr.addWidget(detail_lnk)
        bd_vl.addLayout(bd_hdr)
        self._breakdown_list = QVBoxLayout()
        self._breakdown_list.setSpacing(8)
        bd_vl.addLayout(self._breakdown_list)
        bd_vl.addStretch()
        bot.addWidget(bd_card, 3)

        # Job details
        jd_card = _card_frame()
        jd_vl = QVBoxLayout(jd_card)
        jd_vl.setContentsMargins(20, 16, 20, 18)
        jd_vl.setSpacing(0)
        jd_title = QLabel("Detail Informasi")
        jd_title.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        jd_vl.addWidget(jd_title)
        jd_vl.addSpacing(10)
        self._detail_vals: List[QLabel] = []
        for key in ["Mode", "Kiri", "Kanan", "Key", "Kolom dicompare",
                    "Total Row Kiri", "Total Row Kanan",
                    "Normalization", "Durasi", "Completed"]:
            hl = QHBoxLayout()
            k_lbl = QLabel(key)
            k_lbl.setFixedWidth(130)
            k_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
            v_lbl = QLabel("\u2014")
            v_lbl.setWordWrap(True)
            v_lbl.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 12px;")
            hl.addWidget(k_lbl)
            hl.addWidget(v_lbl, 1)
            self._detail_vals.append(v_lbl)
            jd_vl.addLayout(hl)
            jd_vl.addSpacing(6)
        jd_vl.addStretch()
        bot.addWidget(jd_card, 2)
        vl.addLayout(bot)

        # ── action buttons ──
        act = QHBoxLayout()
        btn_view = QPushButton("\u229e  View Detailed Results")
        btn_view.setFixedHeight(38)
        btn_view.clicked.connect(self.view_detail_requested)
        btn_rerun = QPushButton("\u21ba  Re-run with same config")
        btn_rerun.setObjectName("secondaryBtn")
        btn_rerun.setFixedHeight(38)
        btn_rerun.clicked.connect(self.rerun_requested)
        act.addWidget(btn_view)
        act.addStretch()
        act.addWidget(btn_rerun)
        vl.addLayout(act)
        vl.addStretch()

        scroll.setWidget(inner)
        root_vl = QVBoxLayout(self)
        root_vl.setContentsMargins(0, 0, 0, 0)
        root_vl.addWidget(scroll)

    def set_expected_btn_busy(self, busy: bool):
        """Ubah tampilan tombol Ekspektasi saat proses export berlangsung."""
        if busy:
            self._btn_exp.setEnabled(False)
            self._btn_exp.setText("\u23f3  Mengekspor...")
        else:
            self._btn_exp.setEnabled(True)
            self._btn_exp.setText("\u2B07  Ekspektasi")

    def _on_card_clicked(self, status: str):
        self.filter_detail.emit(status)
        self.view_detail_requested.emit()

    def load(
        self,
        job: "CompareJob",
        summary: dict,
        breakdown: list,
        dup_breakdown: dict = None,
    ):
        total = summary.get("total_rows", 0)
        dup_count = summary.get(RESULT_DUPLICATE_KEY, 0)
        clean_total = total - dup_count
        self._bc.setText(f"{job.job_number}  \u203a  Hasil Konversi")
        self._title.setText(f"{job.name}  \u2014  Hasil Konversi")
        meta_parts = [f"{_fmt(clean_total)} baris divalidasi"]
        if dup_count:
            meta_parts.append(f"{_fmt(dup_count)} baris duplikat tidak dihitung")
        meta_parts.append(f"selesai dalam {job.duration_str}")
        meta_parts.append(job.time_ago_str)
        self._meta.setText("  \u00b7  ".join(meta_parts))
        for s, card in self._cards.items():
            card.update_data(summary.get(s, 0), total)
            card.update_note("")
        # Penjelasan breakdown duplikat
        if dup_count and dup_breakdown:
            dup_left  = dup_breakdown.get("dup_left", 0)
            dup_right = dup_breakdown.get("dup_right", 0)
            parts = []
            if dup_left:  parts.append(f"Sumber (kiri): {_fmt(dup_left)} baris")
            if dup_right: parts.append(f"Target (kanan): {_fmt(dup_right)} baris")
            note = ("  \u00b7  ".join(parts) + "  \u2014  key ganda, tidak bisa divalidasi") if parts else \
                   "Key ganda di salah satu sisi \u2014 tidak bisa divalidasi"
            self._cards[RESULT_DUPLICATE_KEY].update_note(note)
        self._dist_bar.set_data(summary)
        self._populate_breakdown(breakdown, summary.get(RESULT_MISMATCH, 0))

        from models.compare_config import CompareConfig
        try:
            cfg = CompareConfig.from_dict(job.config)
        except Exception:
            cfg = None

        def _src(src) -> str:
            if not src:
                return "\u2014"
            if src.source_type in ("excel", "csv"):
                name = os.path.basename(src.file_path) if src.file_path else "\u2014"
                return name + (f" \u2014 {src.sheet_name}" if src.sheet_name else "")
            db_label = "MySQL" if src.source_type == "mysql" else "PostgreSQL"
            return f"{src.schema_name}.{src.table_name} ({db_label})"

        norm_parts = []
        if cfg:
            o = cfg.options
            if o.trim_whitespace:      norm_parts.append("Trim")
            if o.ignore_case:          norm_parts.append("Ignore Case")
            if o.treat_empty_as_null:  norm_parts.append("Empty=Null")
            if o.normalize_date:       norm_parts.append("Date")
            if o.normalize_number:     norm_parts.append("Number")

        vals = [
            JOB_TYPE_LABELS.get(job.job_type, job.job_type),
            _src(cfg.left_source if cfg else None),
            _src(cfg.right_source if cfg else None),
            ", ".join(f"{m.left_col}\u2192{m.right_col}" for m in cfg.key_columns) if cfg else "\u2014",
            (", ".join(m.left_col for m in cfg.compare_columns[:5])
             + (" ..." if cfg and len(cfg.compare_columns) > 5 else "")) if cfg else "\u2014",
            _fmt(summary.get("_left_rows", 0)) if summary.get("_left_rows") else "\u2014",
            _fmt(summary.get("_right_rows", 0)) if summary.get("_right_rows") else "\u2014",
            ", ".join(norm_parts) if norm_parts else "None",
            job.duration_str,
            job.completed_at_str,
        ]
        for lbl, val in zip(self._detail_vals, vals):
            lbl.setText(val)

    def _populate_breakdown(self, breakdown: list, total_mm: int):
        while self._breakdown_list.count():
            item = self._breakdown_list.takeAt(0)
            if item and item.widget():
                item.widget().deleteLater()
        if not breakdown:
            no_data = QLabel("Tidak ada data mismatch.")
            no_data.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
            self._breakdown_list.addWidget(no_data)
            return
        max_cnt = breakdown[0][1] if breakdown else 1
        for col_name, cnt in breakdown[:7]:
            row_w = QWidget()
            hl = QHBoxLayout(row_w)
            hl.setContentsMargins(0, 0, 0, 0)
            hl.setSpacing(10)
            name_lbl = QLabel(str(col_name))
            name_lbl.setFixedWidth(115)
            name_lbl.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 12px;")
            # bar
            bar_bg = QFrame()
            bar_bg.setFixedHeight(8)
            bar_bg.setStyleSheet(f"background: {COLOR_BORDER}; border-radius: 4px;")
            bar_fill = QFrame(bar_bg)
            bar_fill.setFixedHeight(8)
            bar_fill.setFixedWidth(max(4, int(cnt / max_cnt * 150)))
            bar_fill.setStyleSheet(f"background: {COLOR_DANGER}; border-radius: 4px;")
            pct_str = f"{cnt:,} rows ({cnt/total_mm*100:.1f}%)" if total_mm else f"{cnt:,}"
            cnt_lbl = QLabel(pct_str)
            cnt_lbl.setFixedWidth(140)
            cnt_lbl.setAlignment(Qt.AlignmentFlag.AlignRight)
            cnt_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
            hl.addWidget(name_lbl)
            hl.addWidget(bar_bg, 1)
            hl.addWidget(cnt_lbl)
            self._breakdown_list.addWidget(row_w)


# ─── Filter pill ───────────────────────────────────────────────────────────────

class _Pill(QPushButton):
    def __init__(self, label: str, value, parent=None):
        super().__init__(label, parent)
        self.setCheckable(True)
        self._value = value
        self.setFixedHeight(30)
        self.toggled.connect(self._restyle)
        self._restyle(False)

    def _restyle(self, checked: bool):
        if checked:
            self.setStyleSheet(
                f"QPushButton {{ background: {COLOR_PRIMARY}; color: white; "
                "border: none; border-radius: 5px; padding: 4px 14px; font-size: 12px; }}"
            )
        else:
            self.setStyleSheet(
                f"QPushButton {{ background: white; color: {COLOR_TEXT}; "
                f"border: 1px solid {COLOR_BORDER}; border-radius: 5px; "
                "padding: 4px 14px; font-size: 12px; }}"
                f"QPushButton:hover {{ background: {COLOR_BG}; }}"
            )

    @property
    def filter_value(self):
        return self._value


# ─── Detail view ───────────────────────────────────────────────────────────────

class _DetailView(QWidget):
    export_filtered_clicked = Signal(object)   # None atau status string

    def __init__(self, parent=None):
        super().__init__(parent)
        self._repo: Optional["ResultRepository"] = None
        self._key_mappings: list = []
        self._compare_mappings: list = []
        self._ge_extra_right_cols: List[str] = []  # GE right_cols not in compare_columns
        self._status_filter: Optional[str] = None
        self._search_text: str = ""
        self._current_page: int = 1
        self._page_size: int = 100
        self._setup_ui()

    def _setup_ui(self):
        vl = QVBoxLayout(self)
        vl.setContentsMargins(32, 20, 32, 16)
        vl.setSpacing(12)

        # header
        hdr = QHBoxLayout()
        self._bc = QLabel("JOB-000  \u203a  Cek Detail")
        self._bc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        hdr.addWidget(self._bc)
        hdr.addStretch()
        self._export_btn = QPushButton("\u2193  Export filtered")
        self._export_btn.setObjectName("secondaryBtn")
        self._export_btn.clicked.connect(
            lambda: self.export_filtered_clicked.emit(self._status_filter)
        )
        hdr.addWidget(self._export_btn)
        vl.addLayout(hdr)

        self._title = QLabel("Cek Detail")
        self._title.setStyleSheet(f"color: {COLOR_TEXT}; font-size: 18px; font-weight: 700;")
        vl.addWidget(self._title)

        # filter bar
        fbar = QHBoxLayout()
        fbar.setSpacing(8)
        self._search = QLineEdit()
        self._search.setPlaceholderText("Search by key or value...")
        self._search.setFixedWidth(256)
        self._search.setFixedHeight(34)
        self._search.textChanged.connect(self._on_search)
        fbar.addWidget(self._search)

        self._pills: List[_Pill] = []
        pills_data = [
            ("All", None),
            (RESULT_STATUS_LABELS[RESULT_MATCH], RESULT_MATCH),
            (RESULT_STATUS_LABELS[RESULT_MISMATCH], RESULT_MISMATCH),
            (RESULT_STATUS_LABELS[RESULT_MISSING_LEFT], RESULT_MISSING_LEFT),
            (RESULT_STATUS_LABELS[RESULT_MISSING_RIGHT], RESULT_MISSING_RIGHT),
            (RESULT_STATUS_LABELS[RESULT_DUPLICATE_KEY], RESULT_DUPLICATE_KEY),
        ]
        for label, val in pills_data:
            p = _Pill(label, val)
            p.toggled.connect(lambda checked, v=val: self._on_pill(v, checked))
            fbar.addWidget(p)
            self._pills.append(p)
        self._pills[0].setChecked(True)

        fbar.addStretch()
        self._count_lbl = QLabel("")
        self._count_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        fbar.addWidget(self._count_lbl)
        vl.addLayout(fbar)

        # table
        self._table = QTableWidget(0, 3)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.verticalHeader().setVisible(False)
        self._table.setAlternatingRowColors(False)
        self._table.setShowGrid(False)
        self._table.setStyleSheet(
            "QTableWidget { border: 1px solid #e2e8f0; border-radius: 8px; background: white; }"
            "QHeaderView::section { background: #f8fafc; color: #475569; font-size: 12px; "
            "font-weight: 600; border: none; border-bottom: 1px solid #e2e8f0; padding: 8px 10px; }"
            "QTableWidget::item { padding: 6px 10px; border-bottom: 1px solid #f1f5f9; }"
        )
        self._table.horizontalHeader().setHighlightSections(False)
        vl.addWidget(self._table, 1)

        self._pagination = PaginationWidget()
        self._pagination.page_changed.connect(self._load_page)
        self._pagination.page_size_changed.connect(self._on_page_size_changed)
        vl.addWidget(self._pagination)

    # ── navigation / filter ──

    def _on_search(self, text: str):
        self._search_text = text.strip()
        self._load_page(1)

    def _on_pill(self, status, checked: bool):
        if not checked:
            return
        # uncheck semua lainnya
        for p in self._pills:
            if p.filter_value != status:
                p.blockSignals(True)
                p.setChecked(False)
                p.blockSignals(False)
        self._status_filter = status
        self._load_page(1)

    def set_status_filter(self, status: Optional[str]):
        for p in self._pills:
            p.blockSignals(True)
            p.setChecked(p.filter_value == status)
            p.blockSignals(False)
        self._status_filter = status
        self._load_page(1)

    # ── data ──

    def setup_columns(self, job: "CompareJob", key_maps: list, cmp_maps: list,
                       ge_extra_right_cols: Optional[List[str]] = None):
        self._key_mappings         = key_maps
        self._compare_mappings     = cmp_maps
        self._ge_extra_right_cols  = ge_extra_right_cols or []
        self._bc.setText(f"{job.job_number}  \u203a  Cek Detail")
        self._title.setText(f"{job.name}  \u2014  Cek Detail")
        self._rebuild_headers()

    def _rebuild_headers(self):
        headers = ["Status"]
        if self._key_mappings:
            headers.append("Key  \u2195")
        for cm in self._compare_mappings:
            headers.append(f"{cm.left_col}  \u2191")
            headers.append(f"{cm.right_col}  \u2193")
        # GE extra right_cols: tampil sebagai kolom kanan tambahan (tanpa pasangan kiri)
        for rc in self._ge_extra_right_cols:
            headers.append(f"{rc}  \u2193 (GE)")
        headers.append("Diff Cols")
        self._table.setColumnCount(len(headers))
        self._table.setHorizontalHeaderLabels(headers)
        hh = self._table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self._table.setColumnWidth(0, 110)
        if len(headers) > 1:
            hh.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
            self._table.setColumnWidth(1, 95)
        for i in range(2, len(headers) - 1):
            hh.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        if len(headers) > 1:
            hh.setSectionResizeMode(len(headers) - 1, QHeaderView.ResizeMode.Fixed)
            self._table.setColumnWidth(len(headers) - 1, 160)

    def set_repo(self, repo: "ResultRepository"):
        self._repo = repo

    def _load_page(self, page: int):
        self._current_page = page
        if not self._repo:
            return
        records, total = self._repo.get_page(
            page, self._page_size, self._status_filter,
            search_key=self._search_text or None,
        )
        self._fill_table(records)
        self._pagination.update(page, total, self._page_size)
        self._count_lbl.setText(f"{_fmt(total)} rows")

    def _fill_table(self, records: list):
        self._table.setRowCount(0)
        for rec in records:
            r = self._table.rowCount()
            self._table.insertRow(r)
            self._table.setRowHeight(r, 36)

            status    = rec.get("status", "")
            key_vals  = rec.get("key_values") or {}
            left_data = rec.get("left_data") or {}
            rgt_data  = rec.get("right_data") or {}
            diff_set  = set(rec.get("diff_columns") or [])
            row_bg    = QColor(_STATUS_ROW_BG.get(status, "#ffffff"))

            col = 0
            # Status badge
            badge = QLabel(RESULT_STATUS_LABELS.get(status, status))
            badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
            badge.setStyleSheet(_STATUS_BADGE_STYLE.get(status, ""))
            self._table.setCellWidget(r, col, badge)
            col += 1

            # Key
            if self._key_mappings:
                key_str = " / ".join(str(v) for v in key_vals.values())
                ki = QTableWidgetItem(key_str)
                ki.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
                ki.setBackground(row_bg)
                self._table.setItem(r, col, ki)
                col += 1

            # Compare columns — interleaved left | right
            for cm in self._compare_mappings:
                left_val = str(left_data.get(f"left_{cm.left_col}", "") or "")
                rgt_val  = str(rgt_data.get(f"right_{cm.right_col}", "") or "")
                is_diff  = cm.left_col in diff_set

                if status == RESULT_MISSING_LEFT:
                    left_val = "\u2014"
                elif status == RESULT_MISSING_RIGHT:
                    rgt_val = "\u2014"

                li = QTableWidgetItem(left_val)
                ri = QTableWidgetItem(rgt_val)
                li.setBackground(row_bg)
                ri.setBackground(row_bg)
                if is_diff and status == RESULT_MISMATCH:
                    li.setForeground(QColor(COLOR_DANGER))
                    ri.setForeground(QColor(COLOR_DANGER))
                    li.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
                    ri.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
                elif status in (RESULT_MISSING_LEFT, RESULT_MISSING_RIGHT):
                    for item in (li, ri):
                        if item.text() == "\u2014":
                            item.setForeground(QColor(COLOR_TEXT_LIGHT))
                self._table.setItem(r, col, li)
                self._table.setItem(r, col + 1, ri)
                col += 2

            # GE extra right_cols (read-only, kanan saja)
            for rc in self._ge_extra_right_cols:
                rgt_val = str(rgt_data.get(f"right_{rc}", "") or "")
                if status == RESULT_MISSING_LEFT:
                    rgt_val = "\u2014"
                ri = QTableWidgetItem(rgt_val)
                ri.setBackground(row_bg)
                if status == RESULT_MISSING_LEFT:
                    ri.setForeground(QColor(COLOR_TEXT_LIGHT))
                self._table.setItem(r, col, ri)
                col += 1

            # Diff cols
            diff_item = QTableWidgetItem(", ".join(sorted(diff_set)) if diff_set else "\u2014")
            diff_item.setBackground(row_bg)
            if diff_set:
                diff_item.setForeground(QColor(COLOR_DANGER))
            self._table.setItem(r, col, diff_item)

    def _on_page_size_changed(self, size: int):
        self._page_size = size
        self._load_page(1)


# ─── ResultPage ────────────────────────────────────────────────────────────────

class ResultPage(QWidget):
    """
    Halaman utama hasil perbandingan.
    Menggabungkan progress view (saat job berjalan) dan
    dua sub-view setelah selesai: Summary dan Detail.
    """

    back_to_history = Signal()
    new_job         = Signal()
    rerun_job       = Signal(str)   # job_id

    def __init__(self, settings: "AppSettings", parent=None):
        super().__init__(parent)
        self._settings = settings
        self._job: Optional["CompareJob"] = None
        self._repo: Optional["ResultRepository"] = None
        self._expected_worker = None
        self._expected_pdlg: Optional[QProgressDialog] = None
        self._setup_ui()

    def _setup_ui(self):
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self._left_nav = _LeftNav()
        self._left_nav.switched.connect(self._switch_view)
        self._left_nav.go_back.connect(self.back_to_history)
        root.addWidget(self._left_nav)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setStyleSheet(f"color: {COLOR_BORDER};")
        sep.setFixedWidth(1)
        root.addWidget(sep)

        self._stack = QStackedWidget()

        # [0] progress
        self._progress = _ProgressView()
        self._progress.cancel_clicked.connect(self.back_to_history)
        self._stack.addWidget(self._progress)

        # [1] summary
        self._summary = _SummaryView()
        self._summary.view_detail_requested.connect(lambda: self._switch_view(1))
        self._summary.rerun_requested.connect(lambda: self.rerun_job.emit(self._job.id) if self._job else None)
        self._summary.export_excel_requested.connect(lambda: self._do_export("xlsx"))
        self._summary.export_csv_requested.connect(lambda: self._do_export("csv"))
        self._summary.export_expected_requested.connect(self._do_export_expected)
        self._summary.filter_detail.connect(self._on_filter_then_switch)
        self._stack.addWidget(self._summary)

        # [2] detail
        self._detail = _DetailView()
        self._detail.export_filtered_clicked.connect(self._on_export_filtered)
        self._stack.addWidget(self._detail)

        root.addWidget(self._stack, 1)

    # ── public API ──────────────────────────────────────────────────────────────

    def load_job(self, job: "CompareJob"):
        """Load job — otomatis pilih progress vs summary view."""
        self._job = job
        self._progress.set_title(f"{job.job_number}  \u2014  {job.name}")

        if job.status in (JOB_STATUS_PROCESSING, JOB_STATUS_QUEUED):
            self._left_nav.hide()
            self._stack.setCurrentIndex(0)
        else:
            self._left_nav.show()
            self._open_repo()
            self._load_summary_data()
            self._stack.setCurrentIndex(1)
            self._left_nav.set_active(0)
            # Setup detail view columns tanpa load data dulu
            self._setup_detail_columns()

    def show_progress(self, step: str, rows_done: int, total: int):
        self._progress.update_progress(step, rows_done, total)

    def append_log(self, message: str):
        self._progress.append_log(message)

    def on_job_completed(self, job: "CompareJob"):
        self._job = job
        self._left_nav.show()
        self._open_repo()
        self._load_summary_data()
        self._stack.setCurrentIndex(1)
        self._left_nav.set_active(0)
        self._setup_detail_columns()

    # ── private ─────────────────────────────────────────────────────────────────

    def _switch_view(self, idx: int):
        """0 = summary, 1 = detail."""
        self._left_nav.set_active(idx)
        stack_idx = idx + 1   # 0→1 (summary), 1→2 (detail)
        self._stack.setCurrentIndex(stack_idx)
        if idx == 1 and self._detail._table.rowCount() == 0:
            self._detail._load_page(1)

    def _on_filter_then_switch(self, status: str):
        self._detail.set_status_filter(status)
        self._switch_view(1)

    def _open_repo(self):
        if self._repo:
            self._repo.close()
            self._repo = None
        if not self._job:
            return
        from storage.result_repository import ResultRepository
        db_path = self._settings.jobs_dir / self._job.id / "data.duckdb"
        if db_path.exists():
            self._repo = ResultRepository(db_path)
            self._repo.open()

    def _load_summary_data(self):
        if not self._job:
            return
        summary = self._job.result_summary or {}
        breakdown: list = []
        dup_breakdown: dict = {}
        if self._repo:
            try:
                breakdown = self._repo.get_mismatch_column_breakdown()
            except Exception:
                pass
            try:
                dup_breakdown = self._repo.get_duplicate_key_breakdown()
            except Exception:
                pass
        self._summary.load(self._job, summary, breakdown, dup_breakdown)

    def _setup_detail_columns(self):
        if not self._job:
            return
        from models.compare_config import CompareConfig
        try:
            cfg = CompareConfig.from_dict(self._job.config)
        except Exception:
            return

        # Deteksi GE extra right_cols: kolom kanan dari GE rule yang tidak ada di compare_columns
        ge_extra_right_cols: List[str] = []
        if getattr(cfg.options, "apply_group_expansion", False):
            try:
                ge_rules = self._settings.get_group_expansion_rules()
                cmp_right_set = {cm.right_col for cm in cfg.compare_columns}
                for rule in ge_rules:
                    if not rule.enabled:
                        continue
                    for rc in rule.right_cols:
                        if rc not in cmp_right_set and rc not in ge_extra_right_cols:
                            ge_extra_right_cols.append(rc)
            except Exception:
                pass

        self._detail.setup_columns(self._job, cfg.key_columns, cfg.compare_columns,
                                   ge_extra_right_cols)
        self._detail.set_repo(self._repo)

    def _do_export(self, fmt: str):
        if not self._job or not self._repo:
            return
        suffix = "xlsx" if fmt == "xlsx" else "csv"
        default = f"export_{self._job.name[:20]}_{self._job.id[:6]}.{suffix}"
        path, _ = QFileDialog.getSaveFileName(
            self, "Simpan Hasil Export",
            str(self._settings.exports_dir / default),
            "Excel Files (*.xlsx)" if fmt == "xlsx" else "CSV Files (*.csv)",
        )
        if not path:
            return
        try:
            self._repo.export_to_file(path, status_filter=None)
            QMessageBox.information(self, "Export Selesai", f"Hasil disimpan ke:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Gagal", str(e))

    def _on_export_filtered(self, status_filter):
        if not self._job or not self._repo:
            return
        default = f"export_filtered_{self._job.id[:6]}.xlsx"
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Filtered Results",
            str(self._settings.exports_dir / default),
            "Excel Files (*.xlsx);;CSV Files (*.csv)",
        )
        if not path:
            return
        try:
            self._repo.export_to_file(path, status_filter=status_filter)
            QMessageBox.information(self, "Export Selesai", f"Hasil disimpan ke:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Gagal", str(e))

    # ── export ekspektasi migrasi ────────────────────────────────────────────

    def _do_export_expected(self):
        """Tampilkan dialog format, lalu jalankan ekspor ekspektasi di background."""
        if not self._job:
            return

        # 1. Pilih format
        fmt_dlg = _ExportExpectedDialog(self)
        if fmt_dlg.exec() != QDialog.DialogCode.Accepted:
            return
        fmt = fmt_dlg.selected_fmt
        if not fmt:
            return

        # 2. Pilih lokasi simpan
        suffix  = "xlsx" if fmt == "xlsx" else "csv"
        safe_name = "".join(
            c if c.isalnum() or c in " _-" else "_"
            for c in self._job.name[:20]
        ).strip()
        default = f"ekspektasi_{safe_name}_{self._job.id[:6]}.{suffix}"
        filter_str = (
            "Excel Files (*.xlsx)" if fmt == "xlsx" else "CSV Files (*.csv)"
        )
        path, _ = QFileDialog.getSaveFileName(
            self, "Simpan Ekspektasi Migrasi",
            str(self._settings.exports_dir / default),
            filter_str,
        )
        if not path:
            return

        # 3. Baca config job
        from models.compare_config import CompareConfig
        try:
            cfg = CompareConfig.from_dict(self._job.config)
        except Exception as e:
            QMessageBox.critical(
                self, "Konfigurasi Error",
                f"Gagal membaca konfigurasi job:\n{e}",
            )
            return

        # 4. Load transform + GE rules sesuai opsi yang aktif di job
        tx_rules, ge_rules = [], []
        if cfg.options.apply_global_transforms:
            try:
                tx_rules = self._settings.get_transform_rules()
            except Exception:
                pass
        if getattr(cfg.options, "apply_group_expansion", False):
            try:
                ge_rules = self._settings.get_group_expansion_rules()
            except Exception:
                pass

        # 5. Progress dialog
        pdlg = QProgressDialog(
            "Menyiapkan data ekspektasi...", "Batal", 0, 0, self
        )
        pdlg.setWindowTitle("Download Ekspektasi Migrasi")
        pdlg.setWindowModality(Qt.WindowModality.WindowModal)
        pdlg.setMinimumWidth(420)
        pdlg.setAutoClose(False)
        pdlg.setAutoReset(False)
        pdlg.setValue(0)
        self._expected_pdlg = pdlg

        # 6. Buat dan mulai worker
        from workers.expected_export_worker import ExpectedExportWorker
        self._expected_worker = ExpectedExportWorker(
            job=self._job,
            config=cfg,
            settings=self._settings,
            output_path=path,
            fmt=fmt,
            transform_rules=tx_rules,
            group_expansion_rules=ge_rules,
            parent=self,
        )
        pdlg.canceled.connect(self._expected_worker.cancel)
        self._expected_worker.progress.connect(self._on_expected_progress)
        self._expected_worker.finished.connect(self._on_expected_done)
        self._expected_worker.failed.connect(self._on_expected_failed)

        self._summary.set_expected_btn_busy(True)
        pdlg.show()
        self._expected_worker.start()

    def _on_expected_progress(self, step: str, done: int, total: int):
        if not self._expected_pdlg:
            return
        self._expected_pdlg.setLabelText(step)
        if total > 0:
            self._expected_pdlg.setMaximum(total)
            self._expected_pdlg.setValue(done)
        else:
            self._expected_pdlg.setMaximum(0)
            self._expected_pdlg.setValue(0)

    def _on_expected_done(self, output_path: str, row_count: int):
        self._summary.set_expected_btn_busy(False)
        if self._expected_pdlg:
            self._expected_pdlg.close()
            self._expected_pdlg = None

        msg = (
            f"File ekspektasi migrasi berhasil dibuat!\n\n"
            f"  Baris ditulis : {row_count:,}\n"
            f"  Lokasi file  : {output_path}\n\n"
            f"Buka file sekarang?"
        )
        reply = QMessageBox.question(
            self, "Export Selesai", msg,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            import subprocess
            import sys
            if sys.platform == "win32":
                os.startfile(output_path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", output_path])
            else:
                subprocess.Popen(["xdg-open", output_path])

    def _on_expected_failed(self, msg: str):
        self._summary.set_expected_btn_busy(False)
        if self._expected_pdlg:
            self._expected_pdlg.close()
            self._expected_pdlg = None
        if msg:  # string kosong = dibatalkan user, tidak perlu pesan
            QMessageBox.critical(
                self, "Export Gagal",
                f"Gagal menghasilkan file ekspektasi migrasi:\n\n{msg}",
            )

    def closeEvent(self, event):
        if self._expected_worker and self._expected_worker.isRunning():
            self._expected_worker.cancel()
            self._expected_worker.wait(3000)
        if self._repo:
            self._repo.close()
        super().closeEvent(event)
