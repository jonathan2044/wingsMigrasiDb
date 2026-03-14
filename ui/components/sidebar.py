"""
ui/components/sidebar.py
Komponen sidebar navigasi aplikasi.
"""

from __future__ import annotations
from typing import Callable, List, Tuple

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QPushButton,
    QFrame, QSizePolicy,
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont

from ui.styles import COLOR_SIDEBAR_BG, COLOR_SIDEBAR_TEXT


# (label, page_key, icon_unicode)
NAV_ITEMS: List[Tuple[str, str, str]] = [
    ("Dashboard", "dashboard", "🏠"),
    ("Job Baru", "new_job", "➕"),
    ("Riwayat Job", "job_history", "🕐"),
    ("Template Tersimpan", "templates", "📋"),
    ("Pengaturan", "settings", "⚙"),
]


class Sidebar(QWidget):
    """Sidebar navigasi kiri dengan tombol halaman."""

    page_changed = Signal(str)   # emits page_key saat navigasi

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("sidebar")
        self._buttons: dict[str, QPushButton] = {}
        self._current_page = ""
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # ---- Header ----
        header = QWidget()
        header.setStyleSheet(f"background-color: #152c4a; padding: 0;")
        header_layout = QVBoxLayout(header)
        header_layout.setContentsMargins(12, 20, 12, 16)
        header_layout.setSpacing(2)

        icon_lbl = QLabel("⚡")
        icon_lbl.setStyleSheet("color: #60a5fa; font-size: 20px; background: transparent;")

        title = QLabel("Data Compare")
        title.setObjectName("sidebarTitle")
        title.setStyleSheet(
            "color: white; font-size: 15px; font-weight: bold; background: transparent;"
        )

        subtitle = QLabel("Tool v1.0")
        subtitle.setObjectName("sidebarSubtitle")
        subtitle.setStyleSheet(
            f"color: {COLOR_SIDEBAR_TEXT}; font-size: 10px; background: transparent;"
        )

        header_layout.addWidget(icon_lbl)
        header_layout.addWidget(title)
        header_layout.addWidget(subtitle)
        layout.addWidget(header)

        # Divider
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setStyleSheet("color: rgba(255,255,255,0.1);")
        layout.addWidget(line)

        # ---- Nav items ----
        nav_container = QWidget()
        nav_container.setStyleSheet("background: transparent;")
        nav_layout = QVBoxLayout(nav_container)
        nav_layout.setContentsMargins(8, 12, 8, 8)
        nav_layout.setSpacing(2)

        for label, key, icon in NAV_ITEMS:
            btn = QPushButton(f"  {icon}  {label}")
            btn.setObjectName("navButton")
            btn.setCheckable(False)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.setProperty("active", "false")
            btn.setProperty("page_key", key)
            btn.clicked.connect(lambda checked=False, k=key: self._on_nav_click(k))
            nav_layout.addWidget(btn)
            self._buttons[key] = btn

        layout.addWidget(nav_container)
        layout.addStretch()

        # ---- Footer ----
        footer = QLabel("v1.0.0 • Portable")
        footer.setObjectName("sidebarVersion")
        footer.setStyleSheet(
            "color: #94a3b8; font-size: 10px; padding: 8px 16px; background: transparent;"
        )
        layout.addWidget(footer)
        layout.setContentsMargins(0, 0, 0, 0)

    def _on_nav_click(self, page_key: str):
        self.set_active(page_key)
        self.page_changed.emit(page_key)

    def set_active(self, page_key: str):
        """Update visual state tombol navigasi yang aktif."""
        STYLE_NORMAL = (
            "background-color: #ffffff;"
            "color: #1a1a1a;"
            "border: none;"
            "border-radius: 6px;"
            "text-align: left;"
            "padding: 10px 14px;"
            "font-size: 13px;"
            "font-weight: 500;"
            "margin: 1px 4px;"
        )
        STYLE_ACTIVE = (
            "background-color: #166534;"
            "color: #ffffff;"
            "border: none;"
            "border-radius: 6px;"
            "text-align: left;"
            "padding: 10px 14px;"
            "font-size: 13px;"
            "font-weight: 700;"
            "margin: 1px 4px;"
        )
        for key, btn in self._buttons.items():
            btn.setStyleSheet(STYLE_ACTIVE if key == page_key else STYLE_NORMAL)
        self._current_page = page_key
