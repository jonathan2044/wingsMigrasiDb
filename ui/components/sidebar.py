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
from PySide6.QtGui import QFont, QPixmap, QPainter, QBrush, QColor, QPen, QPainterPath

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
        header.setStyleSheet("background-color: #152c4a;")
        header_layout = QVBoxLayout(header)
        header_layout.setContentsMargins(16, 24, 16, 20)
        header_layout.setSpacing(0)
        header_layout.setAlignment(Qt.AlignmentFlag.AlignHCenter)

        # --- Logo (centered, no background square) ---
        _base_dir = (
            Path(sys.executable).parent if getattr(sys, "frozen", False)
            else Path(__file__).parent.parent.parent
        )
        icon_lbl = QLabel()
        icon_lbl.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        icon_lbl.setStyleSheet("background: transparent; border: none;")
        _avatar_path = _base_dir / "avatar.png"
        if _avatar_path.exists():
            _SIZE = 110
            _BORDER = 3
            _TOTAL = _SIZE + _BORDER * 2

            # Crop source image into a square first, then scale
            _src = QPixmap(str(_avatar_path))
            _side = min(_src.width(), _src.height())
            _src = _src.copy(
                (_src.width() - _side) // 2,
                (_src.height() - _side) // 2,
                _side, _side,
            )
            _src = _src.scaled(
                _SIZE, _SIZE,
                Qt.AspectRatioMode.IgnoreAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )

            # Paint circular clip + ring border onto a transparent canvas
            _canvas = QPixmap(_TOTAL, _TOTAL)
            _canvas.fill(Qt.GlobalColor.transparent)
            _painter = QPainter(_canvas)
            _painter.setRenderHint(QPainter.RenderHint.Antialiasing)

            # Draw ring border (semi-transparent white)
            _pen = QPen(QColor(255, 255, 255, 60))
            _pen.setWidth(_BORDER * 2)
            _painter.setPen(_pen)
            _painter.setBrush(Qt.BrushStyle.NoBrush)
            _painter.drawEllipse(_BORDER, _BORDER, _SIZE, _SIZE)

            # Clip to circle and draw image
            _path = QPainterPath()
            _path.addEllipse(_BORDER, _BORDER, _SIZE, _SIZE)
            _painter.setClipPath(_path)
            _painter.drawPixmap(_BORDER, _BORDER, _src)
            _painter.end()

            icon_lbl.setPixmap(_canvas)
            icon_lbl.setFixedSize(_TOTAL, _TOTAL)
        else:
            icon_lbl.setText("⚡")
            icon_lbl.setStyleSheet("color: #60a5fa; font-size: 24px; background: transparent;")

        # --- App name ---
        title = QLabel("SFA Compare Tool")
        title.setObjectName("sidebarTitle")
        title.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        title.setStyleSheet(
            "color: #ffffff; font-size: 14px; font-weight: bold;"
            " background: transparent; border: none; letter-spacing: 0.5px;"
        )

        # --- Subtitle ---
        by_lbl = QLabel("By Timnya Mas Abdan - 4281")
        by_lbl.setObjectName("sidebarSubtitle")
        by_lbl.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        by_lbl.setStyleSheet(
            "color: rgba(148, 163, 184, 0.85); font-size: 9px;"
            " background: transparent; border: none;"
        )

        # --- Version (very subtle) ---
        version_lbl = QLabel("v1.0")
        version_lbl.setObjectName("sidebarVersionLabel")
        version_lbl.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        version_lbl.setStyleSheet(
            "color: rgba(148, 163, 184, 0.45); font-size: 8px;"
            " background: transparent; border: none;"
        )

        header_layout.addWidget(icon_lbl, 0, Qt.AlignmentFlag.AlignHCenter)
        header_layout.addSpacing(10)
        header_layout.addWidget(title)
        header_layout.addSpacing(4)
        header_layout.addWidget(by_lbl)
        header_layout.addSpacing(6)
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
