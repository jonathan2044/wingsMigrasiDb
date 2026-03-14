"""
ui/pages/templates_page.py
Halaman manajemen template konfigurasi perbandingan.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QTableWidget, QTableWidgetItem, QHeaderView,
    QMessageBox, QDialog, QFormLayout, QLineEdit, QTextEdit,
    QDialogButtonBox,
)
from PySide6.QtCore import Qt, Signal

from config.constants import JOB_TYPE_LABELS
from ui.styles import COLOR_TEXT_MUTED, COLOR_CARD_BG, COLOR_BORDER, COLOR_DANGER

if TYPE_CHECKING:
    from storage.template_manager import TemplateManager


class TemplatesPage(QWidget):
    """Kelola template konfigurasi perbandingan tersimpan."""

    use_template = Signal(str)   # template_id -> buka new job dengan config ini

    def __init__(self, template_manager: "TemplateManager", parent=None):
        super().__init__(parent)
        self._template_manager = template_manager
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(28, 24, 28, 24)
        layout.setSpacing(16)

        # Header
        header = QHBoxLayout()
        title = QLabel("Template Tersimpan")
        title.setObjectName("pageTitle")
        subtitle = QLabel("Konfigurasi perbandingan yang bisa dipakai ulang")
        subtitle.setObjectName("pageSubtitle")
        header.addWidget(title)
        header.addStretch()
        layout.addLayout(header)
        layout.addWidget(subtitle)

        # Tabel template
        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels([
            "Nama Template", "Tipe", "Deskripsi", "Dipakai", "Aksi"
        ])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.verticalHeader().setVisible(False)
        layout.addWidget(self._table, 1)

        # Footer
        self._footer = QLabel("")
        self._footer.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
        layout.addWidget(self._footer)

    # ------------------------------------------------------------------ public

    def refresh(self):
        templates = self._template_manager.get_all()
        self._table.setRowCount(0)

        for tmpl in templates:
            row = self._table.rowCount()
            self._table.insertRow(row)

            self._table.setItem(row, 0, QTableWidgetItem(tmpl.name))
            self._table.setItem(row, 1, QTableWidgetItem(
                JOB_TYPE_LABELS.get(tmpl.job_type, tmpl.job_type)
            ))
            self._table.setItem(row, 2, QTableWidgetItem(tmpl.description or ""))
            self._table.setItem(row, 3, QTableWidgetItem(f"{tmpl.use_count}×"))

            # Tombol aksi
            action_widget = QWidget()
            action_layout = QHBoxLayout(action_widget)
            action_layout.setContentsMargins(4, 2, 4, 2)
            action_layout.setSpacing(4)

            use_btn = QPushButton("Pakai")
            use_btn.setFixedHeight(26)
            use_btn.setProperty("tmpl_id", tmpl.id)
            use_btn.clicked.connect(
                lambda checked=False, tid=tmpl.id: self.use_template.emit(tid)
            )
            action_layout.addWidget(use_btn)

            del_btn = QPushButton("Hapus")
            del_btn.setObjectName("dangerBtn")
            del_btn.setFixedHeight(26)
            del_btn.setProperty("tmpl_id", tmpl.id)
            del_btn.clicked.connect(
                lambda checked=False, tid=tmpl.id, name=tmpl.name: self._delete_template(tid, name)
            )
            action_layout.addWidget(del_btn)

            self._table.setCellWidget(row, 4, action_widget)

        self._footer.setText(f"Total {len(templates):,} template tersimpan.")

    def _delete_template(self, template_id: str, name: str):
        reply = QMessageBox.question(
            self,
            "Hapus Template",
            f"Yakin ingin menghapus template '{name}'?\nTindakan ini tidak bisa dibatalkan.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            self._template_manager.delete(template_id)
            self.refresh()
