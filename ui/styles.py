# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
ui/styles.py
Stylesheet dan konstanta warna untuk seluruh tampilan aplikasi.
Mengikuti desain yang bersih dengan sidebar biru dan konten putih.
"""

# ------------------------------------------------------------------ Color palette
COLOR_PRIMARY = "#2563eb"          # biru utama
COLOR_PRIMARY_DARK = "#1d4ed8"     # biru gelap (hover)
COLOR_PRIMARY_LIGHT = "#dbeafe"    # biru muda (background aktif)
COLOR_SIDEBAR_BG = "#1e3a5f"       # sidebar background (header)
COLOR_SIDEBAR_TEXT = "#1a1a1a"     # teks menu: hitam (nav area bg = putih di Fusion)
COLOR_SIDEBAR_ACTIVE_BG = "#166534" # hijau tua saat aktif
COLOR_SIDEBAR_ACTIVE_TEXT = "#ffffff"

COLOR_BG = "#f8fafc"               # background utama
COLOR_CARD_BG = "#ffffff"          # background kartu
COLOR_BORDER = "#e2e8f0"           # border umum
COLOR_TEXT = "#1e293b"             # teks utama
COLOR_TEXT_MUTED = "#64748b"       # teks sekunder
COLOR_TEXT_LIGHT = "#94a3b8"

COLOR_SUCCESS = "#22c55e"
COLOR_SUCCESS_BG = "#f0fdf4"
COLOR_DANGER = "#ef4444"
COLOR_DANGER_BG = "#fef2f2"
COLOR_WARNING = "#f59e0b"
COLOR_WARNING_BG = "#fffbeb"
COLOR_INFO = "#3b82f6"
COLOR_INFO_BG = "#eff6ff"
COLOR_PURPLE = "#a855f7"
COLOR_PURPLE_BG = "#faf5ff"
COLOR_ORANGE = "#f97316"
COLOR_ORANGE_BG = "#fff7ed"

# ------------------------------------------------------------------ Main stylesheet
MAIN_STYLESHEET = f"""
/* ============================================================
   Global
   ============================================================ */
QWidget {{
    font-family: "Segoe UI", "Arial", sans-serif;
    font-size: 13px;
    color: {COLOR_TEXT};
    background-color: {COLOR_BG};
}}

QMainWindow {{
    background-color: {COLOR_BG};
}}

/* ============================================================
   Sidebar
   ============================================================ */
#sidebar {{
    background-color: {COLOR_SIDEBAR_BG};
    border-right: 1px solid rgba(255,255,255,0.08);
    min-width: 210px;
    max-width: 210px;
}}

#sidebarTitle {{
    color: #ffffff;
    font-size: 15px;
    font-weight: bold;
    padding: 20px 16px 4px 16px;
}}

#sidebarSubtitle {{
    color: {COLOR_SIDEBAR_TEXT};
    font-size: 10px;
    padding: 0 16px 16px 16px;
}}

#sidebarVersion {{
    color: #64748b;
    font-size: 10px;
    padding: 8px 16px;
}}

QPushButton#navButton {{
    background-color: #ffffff;
    color: {COLOR_SIDEBAR_TEXT};
    border: none;
    border-radius: 6px;
    text-align: left;
    padding: 10px 14px;
    font-size: 13px;
    font-weight: 500;
    margin: 1px 4px;
}}

QPushButton#navButton:hover {{
    background-color: #e8f5e9;
    color: #14532d;
}}

QPushButton#navButton[active="true"] {{
    background-color: {COLOR_SIDEBAR_ACTIVE_BG};
    color: {COLOR_SIDEBAR_ACTIVE_TEXT};
    font-weight: 700;
}}

/* ============================================================
   Content area
   ============================================================ */
#contentArea {{
    background-color: {COLOR_BG};
}}

#pageTitle {{
    font-size: 20px;
    font-weight: bold;
    color: {COLOR_TEXT};
}}

#pageSubtitle {{
    font-size: 12px;
    color: {COLOR_TEXT_MUTED};
}}

/* ============================================================
   Cards
   ============================================================ */
#card {{
    background-color: {COLOR_CARD_BG};
    border: 1px solid {COLOR_BORDER};
    border-radius: 10px;
}}

#statCard {{
    background-color: {COLOR_CARD_BG};
    border: 1px solid {COLOR_BORDER};
    border-radius: 10px;
}}

#statValue {{
    font-size: 28px;
    font-weight: bold;
    color: {COLOR_TEXT};
}}

#statLabel {{
    font-size: 12px;
    color: {COLOR_TEXT_MUTED};
    font-weight: 500;
}}

#statSub {{
    font-size: 11px;
    color: {COLOR_TEXT_LIGHT};
}}

/* ============================================================
   Buttons
   ============================================================ */
QPushButton {{
    background-color: {COLOR_PRIMARY};
    color: #ffffff;
    border: none;
    border-radius: 6px;
    padding: 8px 16px;
    font-weight: 600;
    font-size: 13px;
}}

QPushButton:enabled {{
    color: #ffffff;
}}

QPushButton:hover {{
    background-color: {COLOR_PRIMARY_DARK};
    color: #ffffff;
}}

QPushButton:pressed {{
    background-color: #1e40af;
    color: #ffffff;
}}

QPushButton:disabled {{
    background-color: {COLOR_BORDER};
    color: {COLOR_TEXT_MUTED};
}}

QPushButton#secondaryBtn {{
    background-color: {COLOR_CARD_BG};
    color: #1a1a1a;
    border: 1px solid {COLOR_BORDER};
}}

QPushButton#secondaryBtn:hover {{
    background-color: {COLOR_BG};
    border-color: {COLOR_PRIMARY};
    color: {COLOR_PRIMARY};
}}

QPushButton#dangerBtn {{
    background-color: {COLOR_DANGER};
    color: white;
}}

QPushButton#dangerBtn:hover {{
    background-color: #dc2626;
}}

QPushButton#successBtn {{
    background-color: {COLOR_SUCCESS};
    color: white;
}}

QPushButton#quickStartBtn {{
    background-color: {COLOR_CARD_BG};
    color: {COLOR_TEXT};
    border: 1px solid {COLOR_BORDER};
    border-radius: 8px;
    padding: 12px 16px;
    text-align: left;
    font-weight: normal;
}}

QPushButton#quickStartBtn:hover {{
    border-color: {COLOR_PRIMARY};
    background-color: {COLOR_PRIMARY_LIGHT};
}}

/* ============================================================
   Form inputs
   ============================================================ */
QLineEdit, QTextEdit, QPlainTextEdit {{
    background-color: {COLOR_CARD_BG};
    border: 1px solid {COLOR_BORDER};
    border-radius: 6px;
    padding: 8px 10px;
    color: {COLOR_TEXT};
    font-size: 13px;
}}

QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus {{
    border: 2px solid {COLOR_PRIMARY};
    outline: none;
}}

QLineEdit:disabled {{
    background-color: {COLOR_BG};
    color: {COLOR_TEXT_MUTED};
}}

QComboBox {{
    background-color: {COLOR_CARD_BG};
    border: 1px solid {COLOR_BORDER};
    border-radius: 6px;
    padding: 7px 10px;
    color: {COLOR_TEXT};
    font-size: 13px;
    min-height: 32px;
}}

QComboBox:focus {{
    border: 2px solid {COLOR_PRIMARY};
}}

QComboBox::drop-down {{
    border: none;
    width: 24px;
}}

QComboBox::down-arrow {{
    image: none;
    width: 10px;
    height: 10px;
}}

QComboBox QAbstractItemView {{
    background-color: {COLOR_CARD_BG};
    border: 1px solid {COLOR_BORDER};
    border-radius: 6px;
    selection-background-color: {COLOR_PRIMARY_LIGHT};
    selection-color: {COLOR_PRIMARY};
}}

QSpinBox {{
    background-color: {COLOR_CARD_BG};
    border: 1px solid {COLOR_BORDER};
    border-radius: 6px;
    padding: 6px 8px;
    color: {COLOR_TEXT};
    font-size: 13px;
}}

QSpinBox:focus {{
    border: 2px solid {COLOR_PRIMARY};
}}

/* ============================================================
   CheckBox & RadioButton
   ============================================================ */
QCheckBox {{
    spacing: 8px;
    color: {COLOR_TEXT};
    font-size: 13px;
}}

QCheckBox::indicator {{
    width: 16px;
    height: 16px;
    border: 2px solid {COLOR_BORDER};
    border-radius: 3px;
    background-color: {COLOR_CARD_BG};
}}

QCheckBox::indicator:checked {{
    background-color: {COLOR_PRIMARY};
    border-color: {COLOR_PRIMARY};
}}

QRadioButton {{
    spacing: 8px;
    color: {COLOR_TEXT};
}}

QRadioButton::indicator {{
    width: 16px;
    height: 16px;
    border: 2px solid {COLOR_BORDER};
    border-radius: 8px;
    background-color: {COLOR_CARD_BG};
}}

QRadioButton::indicator:checked {{
    background-color: {COLOR_PRIMARY};
    border-color: {COLOR_PRIMARY};
}}

/* ============================================================
   Table
   ============================================================ */
QTableWidget {{
    background-color: {COLOR_CARD_BG};
    border: 1px solid {COLOR_BORDER};
    border-radius: 8px;
    gridline-color: {COLOR_BORDER};
    selection-background-color: {COLOR_PRIMARY_LIGHT};
    selection-color: {COLOR_TEXT};
    font-size: 12px;
}}

QTableWidget::item {{
    padding: 8px 10px;
    border-bottom: 1px solid {COLOR_BORDER};
}}

QTableWidget::item:selected {{
    background-color: {COLOR_PRIMARY_LIGHT};
    color: {COLOR_TEXT};
}}

QHeaderView::section {{
    background-color: {COLOR_BG};
    border: none;
    border-bottom: 2px solid {COLOR_BORDER};
    border-right: 1px solid {COLOR_BORDER};
    padding: 10px 12px;
    font-weight: 700;
    font-size: 12px;
    color: {COLOR_TEXT_MUTED};
    text-transform: uppercase;
}}

/* ============================================================
   Progress Bar
   ============================================================ */
QProgressBar {{
    background-color: {COLOR_BORDER};
    border-radius: 4px;
    height: 8px;
    text-align: center;
    font-size: 10px;
    color: transparent;
}}

QProgressBar::chunk {{
    background-color: {COLOR_PRIMARY};
    border-radius: 4px;
}}

/* ============================================================
   Scroll Bar
   ============================================================ */
QScrollBar:vertical {{
    background-color: {COLOR_BG};
    width: 8px;
    margin: 0;
}}

QScrollBar::handle:vertical {{
    background-color: {COLOR_BORDER};
    border-radius: 4px;
    min-height: 30px;
}}

QScrollBar::handle:vertical:hover {{
    background-color: {COLOR_TEXT_LIGHT};
}}

QScrollBar::add-line:vertical,
QScrollBar::sub-line:vertical {{
    height: 0;
}}

QScrollBar:horizontal {{
    background-color: {COLOR_BG};
    height: 8px;
}}

QScrollBar::handle:horizontal {{
    background-color: {COLOR_BORDER};
    border-radius: 4px;
    min-width: 30px;
}}

/* ============================================================
   Labels
   ============================================================ */
QLabel#sectionHeader {{
    font-size: 14px;
    font-weight: 700;
    color: {COLOR_TEXT};
    padding-bottom: 4px;
}}

QLabel#fieldLabel {{
    font-size: 12px;
    font-weight: 600;
    color: {COLOR_TEXT};
    margin-bottom: 2px;
}}

QLabel#hintLabel {{
    font-size: 11px;
    color: {COLOR_TEXT_MUTED};
}}

/* ============================================================
   Tab Widget
   ============================================================ */
QTabWidget::pane {{
    border: 1px solid {COLOR_BORDER};
    border-radius: 0 0 8px 8px;
    background-color: {COLOR_CARD_BG};
}}

QTabBar::tab {{
    background-color: {COLOR_BG};
    border: 1px solid {COLOR_BORDER};
    border-bottom: none;
    border-radius: 6px 6px 0 0;
    padding: 8px 16px;
    margin-right: 2px;
    font-size: 12px;
    color: {COLOR_TEXT_MUTED};
}}

QTabBar::tab:selected {{
    background-color: {COLOR_CARD_BG};
    color: {COLOR_PRIMARY};
    font-weight: 600;
    border-top: 2px solid {COLOR_PRIMARY};
}}

QTabBar::tab:hover {{
    color: {COLOR_PRIMARY};
}}

/* ============================================================
   Separator
   ============================================================ */
QFrame[frameShape="4"],
QFrame[frameShape="5"] {{
    color: {COLOR_BORDER};
}}

/* ============================================================
   GroupBox
   ============================================================ */
QGroupBox {{
    border: 1px solid {COLOR_BORDER};
    border-radius: 8px;
    margin-top: 16px;
    padding: 12px;
    font-weight: 600;
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 6px;
    color: {COLOR_TEXT_MUTED};
    font-size: 12px;
}}

/* ============================================================
   QMessageBox / QDialog buttons — override agar teks terbaca
   ============================================================ */
QMessageBox QPushButton {{
    background-color: {COLOR_PRIMARY};
    color: #ffffff;
    border: none;
    border-radius: 6px;
    padding: 7px 24px;
    font-weight: 600;
    font-size: 13px;
    min-width: 80px;
}}

QMessageBox QPushButton:hover {{
    background-color: {COLOR_PRIMARY_DARK};
    color: #ffffff;
}}

QMessageBox QPushButton:pressed {{
    background-color: #1e40af;
    color: #ffffff;
}}

QDialogButtonBox QPushButton {{
    background-color: {COLOR_PRIMARY};
    color: #ffffff;
    border: none;
    border-radius: 6px;
    padding: 7px 24px;
    font-weight: 600;
    font-size: 13px;
    min-width: 80px;
}}

QDialogButtonBox QPushButton:hover {{
    background-color: {COLOR_PRIMARY_DARK};
    color: #ffffff;
}}

QDialogButtonBox QPushButton:pressed {{
    background-color: #1e40af;
    color: #ffffff;
}}

/* ============================================================
   Splitter
   ============================================================ */
QSplitter::handle {{
    background-color: {COLOR_BORDER};
}}

/* ============================================================
   Log area
   ============================================================ */
#logArea {{
    background-color: #0f172a;
    color: #94a3b8;
    border-radius: 6px;
    padding: 8px;
    font-family: "Consolas", "Courier New", monospace;
    font-size: 11px;
    border: 1px solid #1e3a5f;
}}

/* ============================================================
   Misc
   ============================================================ */
QToolTip {{
    background-color: {COLOR_TEXT};
    color: white;
    border: none;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 11px;
}}
"""


# ------------------------------------------------------------------ Status badge styles
def get_status_badge_style(status: str) -> str:
    """Hasilkan stylesheet untuk badge status perbandingan."""
    from config.constants import (
        RESULT_MATCH, RESULT_MISMATCH, RESULT_MISSING_LEFT,
        RESULT_MISSING_RIGHT, RESULT_DUPLICATE_KEY,
    )
    mapping = {
        RESULT_MATCH:         (COLOR_SUCCESS, COLOR_SUCCESS_BG),
        RESULT_MISMATCH:      (COLOR_DANGER, COLOR_DANGER_BG),
        RESULT_MISSING_LEFT:  (COLOR_ORANGE, COLOR_ORANGE_BG),
        RESULT_MISSING_RIGHT: (COLOR_PURPLE, COLOR_PURPLE_BG),
        RESULT_DUPLICATE_KEY: (COLOR_WARNING, COLOR_WARNING_BG),
    }
    text_color, bg_color = mapping.get(status, (COLOR_TEXT_MUTED, COLOR_BG))
    return (
        f"background-color: {bg_color}; color: {text_color}; "
        f"border: 1px solid {text_color}; border-radius: 4px; "
        f"padding: 2px 8px; font-size: 11px; font-weight: 600;"
    )


# ------------------------------------------------------------------ Styled QMessageBox helper
_MSG_BTN_STYLE = (
    f"QPushButton {{"
    f"  background-color: {COLOR_PRIMARY};"
    f"  color: #ffffff;"
    f"  border: none;"
    f"  border-radius: 6px;"
    f"  padding: 7px 28px;"
    f"  font-size: 13px;"
    f"  font-weight: 600;"
    f"  min-width: 80px;"
    f"}}"
    f"QPushButton:hover {{"
    f"  background-color: {COLOR_PRIMARY_DARK};"
    f"  color: #ffffff;"
    f"}}"
    f"QPushButton:pressed {{"
    f"  background-color: #1e40af;"
    f"  color: #ffffff;"
    f"}}"
)

_MSG_STYLE = (
    f"QMessageBox {{"
    f"  background-color: #ffffff;"
    f"  color: {COLOR_TEXT};"
    f"}}"
    f"QMessageBox QLabel {{"
    f"  color: {COLOR_TEXT};"
    f"  font-size: 13px;"
    f"  background-color: transparent;"
    f"}}"
    + _MSG_BTN_STYLE
)


def _apply_msg_style(box) -> None:
    """Terapkan stylesheet langsung ke QMessageBox instance."""
    box.setStyleSheet(_MSG_STYLE)
    # Juga paksa ke setiap tombol agar pasti ter-apply di macOS
    from PySide6.QtWidgets import QDialogButtonBox
    bb = box.findChild(QDialogButtonBox)
    if bb:
        for btn in bb.buttons():
            btn.setStyleSheet(_MSG_BTN_STYLE)


def msg_info(parent, title: str, text: str) -> None:
    """QMessageBox.information dengan style yang terbaca."""
    from PySide6.QtWidgets import QMessageBox
    box = QMessageBox(QMessageBox.Icon.Information, title, text, parent=parent)
    _apply_msg_style(box)
    box.exec()


def msg_warning(parent, title: str, text: str) -> None:
    """QMessageBox.warning dengan style yang terbaca."""
    from PySide6.QtWidgets import QMessageBox
    box = QMessageBox(QMessageBox.Icon.Warning, title, text, parent=parent)
    _apply_msg_style(box)
    box.exec()


def msg_critical(parent, title: str, text: str) -> None:
    """QMessageBox.critical dengan style yang terbaca."""
    from PySide6.QtWidgets import QMessageBox
    box = QMessageBox(QMessageBox.Icon.Critical, title, text, parent=parent)
    _apply_msg_style(box)
    box.exec()


def msg_question(parent, title: str, text: str) -> bool:
    """QMessageBox.question dengan style yang terbaca. Returns True jika Yes."""
    from PySide6.QtWidgets import QMessageBox
    box = QMessageBox(QMessageBox.Icon.Question, title, text, parent=parent)
    box.setStandardButtons(
        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
    )
    box.setDefaultButton(QMessageBox.StandardButton.No)
    _apply_msg_style(box)
    return box.exec() == QMessageBox.StandardButton.Yes


def get_job_status_badge_style(status: str) -> str:
    """Hasilkan stylesheet untuk badge status job."""
    from config.constants import (
        JOB_STATUS_QUEUED, JOB_STATUS_PROCESSING,
        JOB_STATUS_COMPLETED, JOB_STATUS_FAILED,
    )
    mapping = {
        JOB_STATUS_QUEUED:      (COLOR_WARNING, COLOR_WARNING_BG),
        JOB_STATUS_PROCESSING:  (COLOR_INFO, COLOR_INFO_BG),
        JOB_STATUS_COMPLETED:   (COLOR_SUCCESS, COLOR_SUCCESS_BG),
        JOB_STATUS_FAILED:      (COLOR_DANGER, COLOR_DANGER_BG),
    }
    text_color, bg_color = mapping.get(status, (COLOR_TEXT_MUTED, COLOR_BG))
    return (
        f"background-color: {bg_color}; color: {text_color}; "
        f"border: 1px solid {text_color}; border-radius: 4px; "
        f"padding: 3px 10px; font-size: 11px; font-weight: 600;"
    )
