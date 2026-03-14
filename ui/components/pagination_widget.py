"""
ui/components/pagination_widget.py
Komponen paginasi untuk tabel hasil perbandingan.
"""

from __future__ import annotations
from PySide6.QtWidgets import (
    QWidget, QHBoxLayout, QLabel, QPushButton, QSpinBox, QComboBox,
)
from PySide6.QtCore import Qt, Signal

from ui.styles import COLOR_TEXT_MUTED, COLOR_PRIMARY, COLOR_BORDER


class PaginationWidget(QWidget):
    """
    Widget paginasi: navigasi halaman, jumlah baris per halaman,
    dan info totaldata.
    """

    page_changed = Signal(int)          # nomor halaman baru (1-based)
    page_size_changed = Signal(int)     # ukuran halaman baru

    def __init__(self, parent=None):
        super().__init__(parent)
        self._current_page = 1
        self._total_pages = 1
        self._total_rows = 0
        self._page_size = 100
        self._setup_ui()

    def _setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 8, 0, 0)
        layout.setSpacing(8)

        # Info total
        self._info_label = QLabel("Menampilkan 0 data")
        self._info_label.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        layout.addWidget(self._info_label)

        layout.addStretch()

        # Baris per halaman
        layout.addWidget(QLabel("Baris/halaman:"))
        self._page_size_combo = QComboBox()
        self._page_size_combo.addItems(["50", "100", "200", "500"])
        self._page_size_combo.setCurrentText("100")
        self._page_size_combo.setFixedWidth(80)
        self._page_size_combo.currentTextChanged.connect(self._on_page_size_changed)
        layout.addWidget(self._page_size_combo)

        # Tombol navigasi
        self._btn_first = self._make_nav_btn("«", self._go_first)
        self._btn_prev  = self._make_nav_btn("‹", self._go_prev)
        layout.addWidget(self._btn_first)
        layout.addWidget(self._btn_prev)

        self._page_label = QLabel("Hal. 1 / 1")
        self._page_label.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        layout.addWidget(self._page_label)

        self._btn_next = self._make_nav_btn("›", self._go_next)
        self._btn_last = self._make_nav_btn("»", self._go_last)
        layout.addWidget(self._btn_next)
        layout.addWidget(self._btn_last)

    def _make_nav_btn(self, text: str, slot) -> QPushButton:
        btn = QPushButton(text)
        btn.setObjectName("secondaryBtn")
        btn.setFixedSize(32, 32)
        btn.clicked.connect(slot)
        return btn

    # ------------------------------------------------------------------ public

    def update(self, current_page: int, total_rows: int, page_size: int):
        self._current_page = current_page
        self._total_rows = total_rows
        self._page_size = page_size
        self._total_pages = max(1, (total_rows + page_size - 1) // page_size)

        start = (current_page - 1) * page_size + 1
        end = min(current_page * page_size, total_rows)
        self._info_label.setText(
            f"Menampilkan {start:,}–{end:,} dari {total_rows:,} data"
            if total_rows > 0 else "Tidak ada data"
        )
        self._page_label.setText(
            f"Hal. {current_page:,} / {self._total_pages:,}"
        )

        self._btn_first.setEnabled(current_page > 1)
        self._btn_prev.setEnabled(current_page > 1)
        self._btn_next.setEnabled(current_page < self._total_pages)
        self._btn_last.setEnabled(current_page < self._total_pages)

    def current_page(self) -> int:
        return self._current_page

    def page_size(self) -> int:
        return self._page_size

    # ------------------------------------------------------------------ slots

    def _go_first(self): self.page_changed.emit(1)
    def _go_last(self):  self.page_changed.emit(self._total_pages)
    def _go_prev(self):
        if self._current_page > 1:
            self.page_changed.emit(self._current_page - 1)
    def _go_next(self):
        if self._current_page < self._total_pages:
            self.page_changed.emit(self._current_page + 1)

    def _on_page_size_changed(self, text: str):
        try:
            size = int(text)
            self.page_size_changed.emit(size)
        except ValueError:
            pass
