# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
ui/components/sidebar.py
Komponen sidebar navigasi aplikasi.
"""

from __future__ import annotations
import sys
from pathlib import Path
from typing import Callable, List, Tuple

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QPushButton,
    QFrame, QSizePolicy,
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont, QPixmap

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

        # Avatar logo
        _base_dir = (
            Path(sys.executable).parent if getattr(sys, "frozen", False)
            else Path(__file__).parent.parent.parent
        )
        icon_lbl = QLabel()
        icon_lbl.setStyleSheet("background: transparent;")
        _avatar_path = _base_dir / "avatar.png"
        if _avatar_path.exists():
            _pix = QPixmap(str(_avatar_path))
            _pix = _pix.scaled(
                72, 72,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            icon_lbl.setPixmap(_pix)
            icon_lbl.setFixedSize(72, 72)
        else:
            icon_lbl.setText("⚡")
            icon_lbl.setStyleSheet("color: #60a5fa; font-size: 20px; background: transparent;")

        title = QLabel("SFA Compare Tool")
        title.setObjectName("sidebarTitle")
        title.setStyleSheet(
            "color: white; font-size: 15px; font-weight: bold; background: transparent;"
        )

        by_lbl = QLabel("By Timnya Mas Abdan - 4281")
        by_lbl.setObjectName("sidebarSubtitle")
        by_lbl.setStyleSheet(
            f"color: {COLOR_SIDEBAR_TEXT}; font-size: 9px; background: transparent;"
        )

        version_lbl = QLabel("v1.0")
        version_lbl.setObjectName("sidebarVersionLabel")
        version_lbl.setStyleSheet(
            "color: #94a3b8; font-size: 9px; background: transparent;"
        )

        header_layout.addWidget(icon_lbl)
        header_layout.addWidget(title)
        header_layout.addWidget(by_lbl)
        header_layout.addWidget(version_lbl)
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
