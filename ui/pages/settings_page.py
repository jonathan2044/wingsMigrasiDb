# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
ui/pages/settings_page.py
Halaman pengaturan aplikasi - koneksi PostgreSQL, preferensi, info aplikasi.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QLineEdit, QSpinBox, QComboBox, QFormLayout,
    QGroupBox, QTabWidget, QScrollArea,
    QTableWidget, QTableWidgetItem, QHeaderView,
)
from PySide6.QtCore import Qt, Signal

from models.connection_profile import ConnectionProfile
from ui.styles import COLOR_TEXT_MUTED, COLOR_SUCCESS, COLOR_DANGER, msg_info, msg_warning, msg_critical, msg_question

if TYPE_CHECKING:
    from config.settings import AppSettings
    from storage.connection_store import ConnectionStore


class ConnectionFormDialog(QWidget):
    """Form inline untuk tambah/edit profil koneksi PostgreSQL."""

    saved = Signal(ConnectionProfile)
    cancelled = Signal()

    def __init__(self, profile: ConnectionProfile = None, parent=None):
        super().__init__(parent)
        self._profile = profile or ConnectionProfile()
        self._setup_ui()
        if profile:
            self._populate(profile)

    def _setup_ui(self):
        layout = QFormLayout(self)
        layout.setSpacing(10)

        self._name = QLineEdit()
        self._name.setPlaceholderText("Contoh: Server Produksi")
        layout.addRow("Nama Profil:", self._name)

        self._host = QLineEdit()
        self._host.setText("localhost")
        layout.addRow("Host:", self._host)

        self._port = QSpinBox()
        self._port.setRange(1, 65535)
        self._port.setValue(5432)
        layout.addRow("Port:", self._port)

        self._database = QLineEdit()
        self._database.setPlaceholderText("nama_database")
        layout.addRow("Database:", self._database)

        self._username = QLineEdit()
        self._username.setPlaceholderText("postgres")
        layout.addRow("Username:", self._username)

        self._password = QLineEdit()
        self._password.setEchoMode(QLineEdit.EchoMode.Password)
        self._password.setPlaceholderText("••••••••")
        layout.addRow("Password:", self._password)

        self._ssl_mode = QComboBox()
        self._ssl_mode.addItems(["prefer", "require", "disable", "verify-ca", "verify-full"])
        layout.addRow("SSL Mode:", self._ssl_mode)

        # Test & Save buttons
        btn_row = QHBoxLayout()
        self._test_btn = QPushButton("🔌 Test Koneksi")
        self._test_btn.setObjectName("secondaryBtn")
        self._test_btn.clicked.connect(self._test_connection)

        self._save_btn = QPushButton("💾 Simpan Profil")
        self._save_btn.clicked.connect(self._save)

        self._cancel_btn = QPushButton("Batal")
        self._cancel_btn.setObjectName("secondaryBtn")
        self._cancel_btn.clicked.connect(self.cancelled.emit)

        btn_row.addWidget(self._test_btn)
        btn_row.addStretch()
        btn_row.addWidget(self._cancel_btn)
        btn_row.addWidget(self._save_btn)
        layout.addRow("", btn_row)

    def _populate(self, profile: ConnectionProfile):
        self._name.setText(profile.name)
        self._host.setText(profile.host)
        self._port.setValue(profile.port)
        self._database.setText(profile.database)
        self._username.setText(profile.username)
        self._password.setText(profile.password)
        idx = self._ssl_mode.findText(profile.ssl_mode)
        if idx >= 0:
            self._ssl_mode.setCurrentIndex(idx)

    def _test_connection(self):
        try:
            from services.postgres_connector import PostgresConnector
            pg = PostgresConnector(
                host=self._host.text(),
                port=self._port.value(),
                database=self._database.text(),
                username=self._username.text(),
                password=self._password.text(),
                ssl_mode=self._ssl_mode.currentText(),
            )
            success, msg = pg.test_connection()
            pg.close()

            if success:
                msg_info(self, "Koneksi Berhasil", msg)
            else:
                msg_warning(self, "Koneksi Gagal", msg)
        except Exception as e:
            msg_critical(self, "Error", str(e))

    def _save(self):
        if not self._name.text().strip():
            msg_warning(self, "Nama Kosong", "Isi nama profil terlebih dahulu.")
            return
        self._profile.name = self._name.text().strip()
        self._profile.host = self._host.text().strip()
        self._profile.port = self._port.value()
        self._profile.database = self._database.text().strip()
        self._profile.username = self._username.text().strip()
        self._profile.password = self._password.text()
        self._profile.ssl_mode = self._ssl_mode.currentText()
        self.saved.emit(self._profile)

    def get_profile(self) -> ConnectionProfile:
        return self._profile


class SettingsPage(QWidget):
    """Halaman pengaturan lengkap aplikasi."""

    def __init__(
        self,
        settings: "AppSettings",
        connection_store: "ConnectionStore",
        job_manager=None,
        parent=None,
    ):
        super().__init__(parent)
        self._settings = settings
        self._connection_store = connection_store
        self._job_manager = job_manager
        self._setup_ui()

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(28, 24, 28, 24)
        root.setSpacing(16)

        title = QLabel("Pengaturan")
        title.setObjectName("pageTitle")
        root.addWidget(title)

        tabs = QTabWidget()

        tabs.addTab(self._build_connection_tab(), "🔌 Koneksi Database")
        tabs.addTab(self._build_general_tab(), "⚙ Umum")
        tabs.addTab(self._build_about_tab(), "ℹ Tentang Aplikasi")

        root.addWidget(tabs, 1)

    # ------------------------------------------------------------------ tabs

    def _build_connection_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        # Daftar profil
        list_header = QHBoxLayout()
        list_header.addWidget(QLabel("Profil Koneksi PostgreSQL"))
        list_header.addStretch()
        add_btn = QPushButton("+ Tambah Profil")
        add_btn.clicked.connect(self._show_add_form)
        list_header.addWidget(add_btn)
        layout.addLayout(list_header)

        self._conn_table = QTableWidget(0, 4)
        self._conn_table.setHorizontalHeaderLabels([
            "Nama Profil", "Host:Port/Database", "User", "Aksi"
        ])
        self._conn_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._conn_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._conn_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._conn_table.verticalHeader().setVisible(False)
        self._conn_table.setMaximumHeight(250)
        layout.addWidget(self._conn_table)

        # Form tambah/edit
        self._form_frame = QFrame()
        self._form_frame.setObjectName("card")
        form_outer = QVBoxLayout(self._form_frame)
        form_outer.addWidget(QLabel("Tambah / Edit Profil Koneksi"))
        self._conn_form = ConnectionFormDialog()
        self._conn_form.saved.connect(self._save_connection)
        self._conn_form.cancelled.connect(lambda: self._form_frame.hide())
        form_outer.addWidget(self._conn_form)
        self._form_frame.hide()
        layout.addWidget(self._form_frame)

        layout.addStretch()
        return w

    def _build_general_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        form = QFormLayout()

        self._rows_per_page = QSpinBox()
        self._rows_per_page.setRange(10, 1000)
        self._rows_per_page.setValue(self._settings.rows_per_page)
        form.addRow("Baris per halaman tabel:", self._rows_per_page)

        self._chunk_size = QSpinBox()
        self._chunk_size.setRange(1000, 100_000)
        self._chunk_size.setSingleStep(1000)
        self._chunk_size.setValue(self._settings.import_chunk_size)
        form.addRow("Ukuran chunk import:", self._chunk_size)

        self._preview_rows = QSpinBox()
        self._preview_rows.setRange(10, 500)
        self._preview_rows.setValue(self._settings.max_preview_rows)
        form.addRow("Maks. baris preview:", self._preview_rows)

        layout.addLayout(form)

        save_btn = QPushButton("💾 Simpan Pengaturan")
        save_btn.clicked.connect(self._save_general)
        layout.addWidget(save_btn)

        # ── Cleanup section ──
        from PySide6.QtWidgets import QSpinBox as _QSB
        from ui.styles import COLOR_BORDER as _CB
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color: {_CB};")
        layout.addWidget(sep)

        cleanup_title = QLabel("Data Cleanup")
        cleanup_title.setStyleSheet("font-size: 13px; font-weight: 700;")
        layout.addWidget(cleanup_title)
        cleanup_sub = QLabel(
            "Hapus data job lama untuk menghemat ruang disk. "
            "Job yang dihapus tidak bisa dipulihkan."
        )
        cleanup_sub.setWordWrap(True)
        cleanup_sub.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        layout.addWidget(cleanup_sub)

        cl_row = QHBoxLayout()
        cl_row.addWidget(QLabel("Hapus job lebih dari"))
        self._cleanup_days = _QSB()
        self._cleanup_days.setRange(1, 365)
        self._cleanup_days.setValue(30)
        self._cleanup_days.setFixedWidth(65)
        cl_row.addWidget(self._cleanup_days)
        cl_row.addWidget(QLabel("hari yang lalu (termasuk data DuckDB)"))
        cl_row.addStretch()
        layout.addLayout(cl_row)

        cl_btn = QPushButton("Bersihkan Sekarang")
        cl_btn.setObjectName("dangerBtn")
        cl_btn.setFixedHeight(36)
        cl_btn.clicked.connect(self._run_cleanup)
        layout.addWidget(cl_btn)

        # Path info
        info_frame = QFrame()
        info_frame.setObjectName("card")
        info_layout = QFormLayout(info_frame)
        info_layout.addRow("Direktori data:", QLabel(str(self._settings.data_dir)))
        info_layout.addRow("Database:", QLabel(str(self._settings.db_path)))
        info_layout.addRow("Export:", QLabel(str(self._settings.exports_dir)))
        layout.addWidget(info_frame)

        layout.addStretch()
        return w

    def _build_about_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(12)

        about = QLabel(
            "<b>SFA Compare Tool</b> v1.0.0<br><br>"
            "Aplikasi desktop untuk membandingkan data dalam skala besar.<br><br>"
            "<b>Fitur utama:</b><br>"
            "• Perbandingan File Excel/CSV vs File Excel/CSV<br>"
            "• Perbandingan File vs PostgreSQL<br>"
            "• Proses data hingga ratusan ribu baris<br>"
            "• Export hasil ke Excel/CSV<br>"
            "• Template konfigurasi tersimpan<br>"
            "• Riwayat pekerjaan lengkap<br><br>"
            "<b>Tech Stack:</b> Python, PySide6, DuckDB, Pandas<br><br>"
            "© 2026 Jonathan Narendra - PT Naraya Prisma Digital"
        )
        about.setWordWrap(True)
        about.setStyleSheet("line-height: 1.6;")
        layout.addWidget(about)
        layout.addStretch()
        return w

    # ------------------------------------------------------------------ connection actions

    def _show_add_form(self):
        self._conn_form._profile = ConnectionProfile()
        self._conn_form._name.clear()
        self._conn_form._host.setText("localhost")
        self._conn_form._port.setValue(5432)
        self._conn_form._database.clear()
        self._conn_form._username.clear()
        self._conn_form._password.clear()
        self._form_frame.show()

    def _save_connection(self, profile: ConnectionProfile):
        self._connection_store.save(profile)
        self._form_frame.hide()
        self.refresh()
        msg_info(self, "Tersimpan", f"Profil '{profile.name}' berhasil disimpan.")

    def _delete_connection(self, profile_id: str, name: str):
        if msg_question(self, "Hapus Profil", f"Yakin ingin menghapus profil '{name}'?"):
            self._connection_store.delete(profile_id)
            self.refresh()

    def refresh(self):
        """Reload daftar profil koneksi."""
        profiles = self._connection_store.get_all()
        self._conn_table.setRowCount(0)

        for p in profiles:
            row = self._conn_table.rowCount()
            self._conn_table.insertRow(row)
            self._conn_table.setItem(row, 0, QTableWidgetItem(p.name))
            self._conn_table.setItem(row, 1, QTableWidgetItem(p.display_info))
            self._conn_table.setItem(row, 2, QTableWidgetItem(p.username))

            act = QWidget()
            act_layout = QHBoxLayout(act)
            act_layout.setContentsMargins(4, 2, 4, 2)
            act_layout.setSpacing(4)

            del_btn = QPushButton("Hapus")
            del_btn.setObjectName("dangerBtn")
            del_btn.setFixedHeight(26)
            del_btn.clicked.connect(
                lambda checked=False, pid=p.id, pname=p.name: self._delete_connection(pid, pname)
            )
            act_layout.addWidget(del_btn)
            self._conn_table.setCellWidget(row, 3, act)

    def _save_general(self):
        self._settings.set("rows_per_page", self._rows_per_page.value())
        self._settings.set("import_chunk_size", self._chunk_size.value())
        self._settings.set("max_preview_rows", self._preview_rows.value())
        self._settings.save()
        msg_info(self, "Tersimpan", "Pengaturan berhasil disimpan.")

    def _run_cleanup(self):
        """Hapus job lama beserta data DuckDB-nya."""
        if not self._job_manager:
            msg_warning(self, "Tidak Tersedia", "Job manager tidak tersedia.")
            return
        days = self._cleanup_days.value()
        old_jobs = self._job_manager.get_jobs_older_than(days)
        if not old_jobs:
            msg_info(
                self, "Cleanup",
                f"Tidak ada job lebih dari {days} hari yang perlu dibersihkan.",
            )
            return
        if not msg_question(
            self, "Konfirmasi Hapus",
            f"Akan menghapus {len(old_jobs)} job (dibuat > {days} hari lalu) "
            "beserta seluruh data hasil perbandingan.\n\nLanjutkan?",
        ):
            return
        deleted = 0
        for job in old_jobs:
            try:
                self._job_manager.delete_with_data(job.id, self._settings.jobs_dir)
                deleted += 1
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning("Gagal hapus job %s: %s", job.id, e)
        msg_info(self, "Cleanup Selesai", f"{deleted} job berhasil dibersihkan.")

