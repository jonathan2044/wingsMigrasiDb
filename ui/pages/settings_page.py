# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
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
    QTableWidget, QTableWidgetItem, QHeaderView, QButtonGroup, QRadioButton,
    QDialog, QCheckBox, QDialogButtonBox, QStackedWidget, QSizePolicy,
)
from PySide6.QtCore import Qt, Signal

from models.connection_profile import ConnectionProfile
from models.compare_config import ColumnTransformRule, GroupExpansionRule
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
        db_type = getattr(profile, "db_type", "postgresql")
        if db_type == "mysql":
            self._rb_mysql.setChecked(True)
        else:
            self._rb_pg.setChecked(True)
        self._name.setText(profile.name)
        self._host.setText(profile.host)
        self._port.setValue(profile.port)
        self._database.setText(profile.database)
        self._username.setText(profile.username)
        self._password.setText(profile.password)
        # Populate SSL options for the correct db type first
        self._on_db_type_changed(True)
        idx = self._ssl_mode.findText(profile.ssl_mode)
        if idx >= 0:
            self._ssl_mode.setCurrentIndex(idx)

    def _test_connection(self):
        try:
            db_type = "mysql" if self._rb_mysql.isChecked() else "postgresql"
            if db_type == "mysql":
                from services.mysql_connector import MySQLConnector
                connector = MySQLConnector(
                    host=self._host.text(),
                    port=self._port.value(),
                    database=self._database.text(),
                    username=self._username.text(),
                    password=self._password.text(),
                    ssl_mode=self._ssl_mode.currentText(),
                )
            else:
                from services.postgres_connector import PostgresConnector
                connector = PostgresConnector(
                    host=self._host.text(),
                    port=self._port.value(),
                    database=self._database.text(),
                    username=self._username.text(),
                    password=self._password.text(),
                    ssl_mode=self._ssl_mode.currentText(),
                )
            success, msg = connector.test_connection()
            connector.close()

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
        self._profile.db_type = "mysql" if self._rb_mysql.isChecked() else "postgresql"
        self._profile.host = self._host.text().strip()
        self._profile.port = self._port.value()
        self._profile.database = self._database.text().strip()
        self._profile.username = self._username.text().strip()
        self._profile.password = self._password.text()
        self._profile.ssl_mode = self._ssl_mode.currentText()
        self.saved.emit(self._profile)

    def get_profile(self) -> ConnectionProfile:
        return self._profile


# ─── Transform Rule Dialog ────────────────────────────────────────────────────

class _TransformRuleDialog(QDialog):
    """Dialog tambah / edit aturan transformasi kolom."""

    _TYPES = ["prefix", "suffix", "lpad", "rpad", "strip_chars", "replace", "substring"]
    _TYPE_LABELS = {
        "prefix":      "Prefix — tambah teks di depan value",
        "suffix":      "Suffix — tambah teks di belakang value",
        "lpad":        "LPAD — zero-pad / pad kiri ke panjang N",
        "rpad":        "RPAD — pad kanan ke panjang N",
        "strip_chars": "Strip Chars — hapus karakter tertentu",
        "replace":     "Replace — ganti teks A dengan B",
        "substring":   "Substring — ambil N karakter mulai posisi X",
    }
    _SIDE_VALUES = ["both", "left", "right"]
    _SIDE_LABELS = ["Kedua Sisi", "Kiri Saja", "Kanan Saja"]

    def __init__(self, rule: ColumnTransformRule = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Tambah Rule Transformasi" if rule is None else "Edit Rule Transformasi")
        self.setModal(True)
        self.setMinimumWidth(480)
        self._result_rule: ColumnTransformRule = None
        self._setup_ui()
        if rule:
            self._populate(rule)
        else:
            self._update_params_ui(self._type_combo.currentData())

    def _setup_ui(self):
        vl = QVBoxLayout(self)
        vl.setSpacing(14)

        form = QFormLayout()
        form.setSpacing(10)

        self._col_name = QLineEdit()
        self._col_name.setPlaceholderText("contoh: sls_code  (case-insensitive)")
        form.addRow("Nama Kolom:", self._col_name)

        self._side_combo = QComboBox()
        for label in self._SIDE_LABELS:
            self._side_combo.addItem(label)
        form.addRow("Terapkan ke Sisi:", self._side_combo)

        self._type_combo = QComboBox()
        for k in self._TYPES:
            self._type_combo.addItem(self._TYPE_LABELS[k], k)
        self._type_combo.currentIndexChanged.connect(
            lambda _: self._update_params_ui(self._type_combo.currentData())
        )
        form.addRow("Tipe Transform:", self._type_combo)

        vl.addLayout(form)

        # ── Params box ──
        self._params_box = QGroupBox("Parameter")
        pb_vl = QVBoxLayout(self._params_box)
        pb_vl.setSpacing(8)

        # prefix / suffix → text
        self._w_text = QWidget()
        wt_hl = QHBoxLayout(self._w_text)
        wt_hl.setContentsMargins(0, 0, 0, 0)
        wt_hl.addWidget(QLabel("Teks:"))
        self._p_text = QLineEdit()
        self._p_text.setPlaceholderText("contoh: S0")
        wt_hl.addWidget(self._p_text)
        pb_vl.addWidget(self._w_text)

        # lpad / rpad → length + pad_char
        self._w_padlen = QWidget()
        wpl_hl = QHBoxLayout(self._w_padlen)
        wpl_hl.setContentsMargins(0, 0, 0, 0)
        wpl_hl.addWidget(QLabel("Panjang target:"))
        self._p_length = QSpinBox()
        self._p_length.setRange(1, 255)
        self._p_length.setValue(8)
        self._p_length.setFixedWidth(70)
        wpl_hl.addWidget(self._p_length)
        wpl_hl.addSpacing(16)
        wpl_hl.addWidget(QLabel("Karakter pad:"))
        self._p_pad_char = QLineEdit()
        self._p_pad_char.setMaxLength(1)
        self._p_pad_char.setText("0")
        self._p_pad_char.setFixedWidth(40)
        wpl_hl.addWidget(self._p_pad_char)
        wpl_hl.addStretch()
        pb_vl.addWidget(self._w_padlen)

        # strip_chars → chars
        self._w_chars = QWidget()
        wc_hl = QHBoxLayout(self._w_chars)
        wc_hl.setContentsMargins(0, 0, 0, 0)
        wc_hl.addWidget(QLabel("Karakter yang dihapus:"))
        self._p_chars = QLineEdit()
        self._p_chars.setPlaceholderText("contoh: -. (tanpa spasi antar karakter)")
        wc_hl.addWidget(self._p_chars)
        pb_vl.addWidget(self._w_chars)

        # replace → old + new
        self._w_replace = QWidget()
        wr_vl = QVBoxLayout(self._w_replace)
        wr_vl.setContentsMargins(0, 0, 0, 0)
        wr_vl.setSpacing(6)
        wr_old = QHBoxLayout()
        wr_old.addWidget(QLabel("Cari:  "))
        self._p_old = QLineEdit()
        self._p_old.setPlaceholderText("teks yang dicari")
        wr_old.addWidget(self._p_old)
        wr_new = QHBoxLayout()
        wr_new.addWidget(QLabel("Ganti:"))
        self._p_new = QLineEdit()
        self._p_new.setPlaceholderText("teks pengganti (kosongkan untuk hapus)")
        wr_new.addWidget(self._p_new)
        wr_vl.addLayout(wr_old)
        wr_vl.addLayout(wr_new)
        pb_vl.addWidget(self._w_replace)

        # substring → start + length
        self._w_substr = QWidget()
        ws_hl = QHBoxLayout(self._w_substr)
        ws_hl.setContentsMargins(0, 0, 0, 0)
        ws_hl.addWidget(QLabel("Mulai posisi (1-based):"))
        self._p_start = QSpinBox()
        self._p_start.setRange(1, 9999)
        self._p_start.setValue(1)
        self._p_start.setFixedWidth(70)
        ws_hl.addWidget(self._p_start)
        ws_hl.addSpacing(16)
        ws_hl.addWidget(QLabel("Panjang:"))
        self._p_substr_len = QSpinBox()
        self._p_substr_len.setRange(1, 9999)
        self._p_substr_len.setValue(10)
        self._p_substr_len.setFixedWidth(70)
        ws_hl.addWidget(self._p_substr_len)
        ws_hl.addStretch()
        pb_vl.addWidget(self._w_substr)

        vl.addWidget(self._params_box)

        # ── Enabled toggle ──
        enabled_hl = QHBoxLayout()
        self._enabled_chk = QCheckBox("Rule ini aktif")
        self._enabled_chk.setChecked(True)
        enabled_hl.addWidget(self._enabled_chk)
        enabled_hl.addStretch()
        vl.addLayout(enabled_hl)

        # ── Buttons ──
        btns = QHBoxLayout()
        btns.addStretch()
        cancel_btn = QPushButton("Batal")
        cancel_btn.setObjectName("secondaryBtn")
        cancel_btn.clicked.connect(self.reject)
        ok_btn = QPushButton("Simpan Rule")
        ok_btn.clicked.connect(self._on_save)
        btns.addWidget(cancel_btn)
        btns.addWidget(ok_btn)
        vl.addLayout(btns)

    def _update_params_ui(self, t: str):
        """Tampilkan hanya widget parameter yang relevan untuk tipe t."""
        self._w_text.setVisible(t in ("prefix", "suffix"))
        self._w_padlen.setVisible(t in ("lpad", "rpad"))
        self._w_chars.setVisible(t == "strip_chars")
        self._w_replace.setVisible(t == "replace")
        self._w_substr.setVisible(t == "substring")
        self.adjustSize()

    def _populate(self, rule: ColumnTransformRule):
        self._col_name.setText(rule.column_name)
        idx_side = self._SIDE_VALUES.index(rule.side) if rule.side in self._SIDE_VALUES else 0
        self._side_combo.setCurrentIndex(idx_side)
        idx_type = self._TYPES.index(rule.transform_type) if rule.transform_type in self._TYPES else 0
        self._type_combo.setCurrentIndex(idx_type)
        self._enabled_chk.setChecked(rule.enabled)
        p = rule.params
        self._p_text.setText(p.get("text", ""))
        self._p_length.setValue(int(p.get("length", 8)))
        self._p_pad_char.setText(p.get("pad_char", "0"))
        self._p_chars.setText(p.get("chars", ""))
        self._p_old.setText(p.get("old", ""))
        self._p_new.setText(p.get("new", ""))
        self._p_start.setValue(int(p.get("start", 1)))
        self._p_substr_len.setValue(int(p.get("length", 10)))
        self._update_params_ui(rule.transform_type)

    def _on_save(self):
        col = self._col_name.text().strip()
        if not col:
            from ui.styles import msg_warning
            msg_warning(self, "Nama Kolom Kosong", "Isi nama kolom terlebih dahulu.")
            return
        t = self._type_combo.currentData()
        side = self._SIDE_VALUES[self._side_combo.currentIndex()]
        params = self._build_params(t)
        self._result_rule = ColumnTransformRule(
            column_name=col,
            side=side,
            transform_type=t,
            params=params,
            enabled=self._enabled_chk.isChecked(),
        )
        self.accept()

    def _build_params(self, t: str) -> dict:
        if t in ("prefix", "suffix"):
            return {"text": self._p_text.text()}
        elif t == "lpad":
            return {"length": self._p_length.value(), "pad_char": self._p_pad_char.text() or "0"}
        elif t == "rpad":
            return {"length": self._p_length.value(), "pad_char": self._p_pad_char.text() or " "}
        elif t == "strip_chars":
            return {"chars": self._p_chars.text()}
        elif t == "replace":
            return {"old": self._p_old.text(), "new": self._p_new.text()}
        elif t == "substring":
            return {"start": self._p_start.value(), "length": self._p_substr_len.value()}
        return {}

    def get_rule(self) -> ColumnTransformRule:
        return self._result_rule


# ─── Group Expansion Rule Dialog ──────────────────────────────────────────────

class _GroupExpansionRuleDialog(QDialog):
    """Dialog tambah/edit GroupExpansionRule — upload CSV/Excel mapping, kolom kanan eksplisit."""

    def __init__(self, rule: GroupExpansionRule = None, parent=None):
        super().__init__(parent)
        self._mapping: dict = {}
        self._right_cols: list = []
        self._result_rule = None
        if rule:
            self._mapping = {k: [list(row) for row in rows] for k, rows in rule.mapping.items()}
            self._right_cols = list(rule.right_cols)
        self.setWindowTitle("Aturan Group Expansion")
        self.setMinimumWidth(700)
        self._setup_ui()
        if rule:
            self._populate(rule)

    def _setup_ui(self):
        from PySide6.QtWidgets import QAbstractItemView
        from ui.styles import COLOR_TEXT, COLOR_TEXT_MUTED, COLOR_BORDER, COLOR_PRIMARY
        vl = QVBoxLayout(self)
        vl.setSpacing(12)

        # ── Step 1: Nama kolom kiri + status ──────────────────────────────────
        form = QFormLayout()
        form.setSpacing(8)
        self._left_col = QLineEdit()
        self._left_col.setPlaceholderText("Contoh: cust_group")
        form.addRow("Nama Kolom Kiri:", self._left_col)
        self._enabled_chk = QCheckBox("Aktif")
        self._enabled_chk.setChecked(True)
        form.addRow("Status:", self._enabled_chk)
        vl.addLayout(form)

        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color: {COLOR_BORDER};")
        vl.addWidget(sep)

        map_title = QLabel("Mapping: Row & Column Expansion")
        map_title.setStyleSheet(f"font-size: 13px; font-weight: 600; color: {COLOR_TEXT};")
        vl.addWidget(map_title)

        # Hint box
        map_hint = QLabel(
            "Kolom kanan selalu sama untuk semua value kiri — yang berbeda hanya jumlah baris per value.\n"
            "Format file (CSV/Excel): kolom pertama = value kolom kiri, kolom berikutnya = kombinasi nilai kolom kanan.\n"
            "Baris pertama boleh berupa header (nama kolom). Contoh:\n"
            "  left_value  │  cust_group  │  cust_group1  │  cust_group2\n"
            "  AA          │  2A0         │  A20          │  A0       ← baris 1 untuk AA\n"
            "  AA          │  2A0         │  A21          │  A1       ← baris 2 untuk AA\n"
            "  AB          │  3B0         │  B20          │  B0       ← baris 1 untuk AB"
        )
        map_hint.setWordWrap(True)
        map_hint.setStyleSheet(
            f"color: {COLOR_TEXT_MUTED}; font-size: 11px; font-family: monospace; "
            "background:#f8fafc; border:1px solid #e2e8f0; border-radius:6px; padding:8px;"
        )
        vl.addWidget(map_hint)

        # ── Step 2: Kolom kanan (eksplisit) ───────────────────────────────────
        rc_form = QFormLayout()
        rc_form.setSpacing(6)
        rc_hl = QHBoxLayout()
        self._right_cols_edit = QLineEdit()
        self._right_cols_edit.setPlaceholderText("Contoh: cust_group, cust_group1, cust_group2")
        self._right_cols_edit.textChanged.connect(self._on_right_cols_changed)
        rc_hl.addWidget(self._right_cols_edit)
        rc_hint_lbl = QLabel("(pisah koma)")
        rc_hint_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
        rc_hl.addWidget(rc_hint_lbl)
        rc_form.addRow("Kolom Kanan:", rc_hl)
        vl.addLayout(rc_form)

        col_tip = QLabel(
            "\u2139\ufe0f  Isi Kolom Kanan terlebih dahulu, lalu upload file mapping. "
            "Jika file memiliki baris header, nama kolom akan terisi otomatis."
        )
        col_tip.setWordWrap(True)
        col_tip.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 11px;")
        vl.addWidget(col_tip)

        # ── Step 3: Upload buttons ─────────────────────────────────────────────
        upload_hl = QHBoxLayout()
        self._upload_csv_btn = QPushButton("\U0001f4c2  Upload CSV")
        self._upload_csv_btn.setObjectName("secondaryBtn")
        self._upload_csv_btn.setFixedHeight(34)
        self._upload_csv_btn.clicked.connect(self._upload_csv)
        self._upload_xls_btn = QPushButton("\U0001f4ca  Upload Excel")
        self._upload_xls_btn.setObjectName("secondaryBtn")
        self._upload_xls_btn.setFixedHeight(34)
        self._upload_xls_btn.clicked.connect(self._upload_excel)
        self._file_lbl = QLabel("Belum ada file")
        self._file_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        upload_hl.addWidget(self._upload_csv_btn)
        upload_hl.addWidget(self._upload_xls_btn)
        upload_hl.addWidget(self._file_lbl, 1)
        vl.addLayout(upload_hl)

        # ── Preview table ──────────────────────────────────────────────────────
        self._preview_tbl = QTableWidget(0, 2)
        self._preview_tbl.setHorizontalHeaderLabels(["Value Kiri", "Kombinasi Kanan (preview per baris)"])
        self._preview_tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self._preview_tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._preview_tbl.setMaximumHeight(200)
        self._preview_tbl.verticalHeader().setVisible(False)
        self._preview_tbl.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._preview_tbl.setStyleSheet(
            "QTableWidget { border: 1px solid #e2e8f0; border-radius: 6px; background: white; }"
            "QHeaderView::section { background: #f8fafc; color: #475569; font-size: 11px; "
            "font-weight: 600; border: none; border-bottom: 1px solid #e2e8f0; padding: 4px 8px; }"
        )
        vl.addWidget(self._preview_tbl)

        self._map_summary_lbl = QLabel("")
        self._map_summary_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        vl.addWidget(self._map_summary_lbl)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self._on_save)
        btns.rejected.connect(self.reject)
        vl.addWidget(btns)

        self._refresh_preview()

    def _on_right_cols_changed(self, text: str):
        """Update _right_cols saat user mengetik nama kolom kanan secara manual."""
        self._right_cols = [c.strip() for c in text.split(",") if c.strip()]
        self._refresh_preview()

    def _populate(self, rule: GroupExpansionRule):
        self._left_col.setText(rule.left_col)
        self._enabled_chk.setChecked(rule.enabled)
        self._mapping = {k: [list(row) for row in rows] for k, rows in rule.mapping.items()}
        self._right_cols = list(rule.right_cols)
        if self._right_cols:
            self._right_cols_edit.blockSignals(True)
            self._right_cols_edit.setText(", ".join(self._right_cols))
            self._right_cols_edit.blockSignals(False)
        n_left  = len(self._mapping)
        n_right = sum(len(rows) for rows in self._mapping.values())
        self._file_lbl.setText(f"Mapping: {n_left} value kiri, {n_right} baris kanan")
        self._refresh_preview()

    def _upload_csv(self):
        from PySide6.QtWidgets import QFileDialog
        import os
        path, _ = QFileDialog.getOpenFileName(
            self, "Upload Mapping CSV", "", "CSV Files (*.csv);;All Files (*)"
        )
        if not path:
            return
        try:
            right_cols, mapping = self._parse_csv(path, fallback_cols=self._right_cols)
            self._apply_parsed(right_cols, mapping, os.path.basename(path))
        except Exception as e:
            msg_warning(self, "Gagal Baca CSV", f"Error membaca file: {e}")

    def _upload_excel(self):
        from PySide6.QtWidgets import QFileDialog
        import os
        path, _ = QFileDialog.getOpenFileName(
            self, "Upload Mapping Excel", "",
            "Excel Files (*.xlsx *.xls);;All Files (*)"
        )
        if not path:
            return
        try:
            right_cols, mapping = self._parse_excel(path, fallback_cols=self._right_cols)
            self._apply_parsed(right_cols, mapping, os.path.basename(path))
        except Exception as e:
            msg_warning(self, "Gagal Baca Excel", f"Error membaca file: {e}")

    def _apply_parsed(self, right_cols: list, mapping: dict, filename: str):
        """Terapkan hasil parsing file, sinkronkan field kolom kanan, refresh preview."""
        self._mapping = mapping
        if right_cols:
            # File punya header → pakai nama dari header, update field
            self._right_cols = right_cols
            self._right_cols_edit.blockSignals(True)
            self._right_cols_edit.setText(", ".join(right_cols))
            self._right_cols_edit.blockSignals(False)
        # Jika file tidak punya header, pakai self._right_cols yang sudah ada (dari field)
        self._file_lbl.setText(filename)
        self._refresh_preview()

    @staticmethod
    def _parse_csv(filepath: str, fallback_cols: list = None):
        """Baca CSV mapping. Returns (right_cols, mapping) tuple."""
        import csv
        right_cols: list = []
        mapping: dict = {}

        with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            return [], {}

        first = [c.strip() for c in rows[0]]
        is_header = (
            len(first) >= 2
            and first[0].lower() in ("left_value", "left_val", "kiri", "nilai_kiri")
        )
        if is_header:
            right_cols = first[1:]
            data_rows = rows[1:]
        else:
            right_cols = []    # akan diisi dari fallback_cols
            data_rows = rows

        eff_cols = right_cols or fallback_cols or []
        n_cols = len(eff_cols) if eff_cols else None
        for raw_row in data_rows:
            if not raw_row:
                continue
            cells = [c.strip() for c in raw_row]
            lv = cells[0] if cells else ""
            if not lv:
                continue
            vals = cells[1:]
            padded = vals[:n_cols] + [''] * max(0, (n_cols or 0) - len(vals)) if n_cols else vals
            if not any(v for v in padded):
                continue
            if lv not in mapping:
                mapping[lv] = []
            mapping[lv].append(padded)

        # Auto-generate names jika tidak ada header dan tidak ada fallback
        if not right_cols and not fallback_cols and mapping:
            max_c = max((len(r) for rl in mapping.values() for r in rl), default=0)
            right_cols = [f"col_{i}" for i in range(max_c)]

        # Kembalikan right_cols dari header saja (bukan fallback) agar _apply_parsed tahu
        return right_cols, mapping

    @staticmethod
    def _parse_excel(filepath: str, fallback_cols: list = None):
        """Baca Excel mapping. Returns (right_cols, mapping) tuple."""
        import openpyxl
        right_cols: list = []
        mapping: dict = {}

        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        all_rows = []
        for row in ws.iter_rows(values_only=True):
            cells = [str(c).strip() if c is not None else "" for c in row]
            if any(cells):
                all_rows.append(cells)
        wb.close()

        if not all_rows:
            return [], {}

        first = all_rows[0]
        is_header = (
            len(first) >= 2
            and first[0].lower() in ("left_value", "left_val", "kiri", "nilai_kiri")
        )
        if is_header:
            right_cols = first[1:]
            data_rows = all_rows[1:]
        else:
            right_cols = []
            data_rows = all_rows

        eff_cols = right_cols or fallback_cols or []
        n_cols = len(eff_cols) if eff_cols else None
        for cells in data_rows:
            lv = cells[0] if cells else ""
            if not lv:
                continue
            vals = list(cells[1:])
            padded = vals[:n_cols] + [''] * max(0, (n_cols or 0) - len(vals)) if n_cols else vals
            if not any(v for v in padded):
                continue
            if lv not in mapping:
                mapping[lv] = []
            mapping[lv].append(padded)

        if not right_cols and not fallback_cols and mapping:
            max_c = max((len(r) for rl in mapping.values() for r in rl), default=0)
            right_cols = [f"col_{i}" for i in range(max_c)]

        return right_cols, mapping

    def _refresh_preview(self):
        self._preview_tbl.setRowCount(0)
        count = 0
        for lv, rows_list in list(self._mapping.items())[:10]:
            for row_vals in rows_list[:3]:
                if count >= 20:
                    break
                r = self._preview_tbl.rowCount()
                self._preview_tbl.insertRow(r)
                self._preview_tbl.setRowHeight(r, 26)
                self._preview_tbl.setItem(r, 0, QTableWidgetItem(str(lv)))
                if self._right_cols:
                    row_str = ", ".join(
                        f"{self._right_cols[i]}={v}" if i < len(self._right_cols) else v
                        for i, v in enumerate(row_vals)
                    )
                else:
                    row_str = ", ".join(str(v) for v in row_vals)
                self._preview_tbl.setItem(r, 1, QTableWidgetItem(row_str))
                count += 1

        n_left  = len(self._mapping)
        n_right = sum(len(rows) for rows in self._mapping.values())
        n_cols  = len(self._right_cols)
        if n_left > 0:
            self._map_summary_lbl.setText(
                f"Total: {n_left} value kiri, {n_right} baris kanan, {n_cols} kolom kanan"
            )
        else:
            self._map_summary_lbl.setText(
                "Belum ada mapping. Isi Kolom Kanan lalu upload file CSV atau Excel."
            )

    def _on_save(self):
        lc = self._left_col.text().strip()
        if not lc:
            msg_warning(self, "Isian Tidak Lengkap", "Nama Kolom Kiri tidak boleh kosong.")
            return
        if not self._mapping:
            msg_warning(self, "Mapping Kosong", "Upload file CSV atau Excel mapping terlebih dahulu.")
            return
        if not self._right_cols:
            msg_warning(
                self, "Kolom Kanan Belum Diisi",
                "Isi nama kolom kanan (pisah koma) atau pastikan file memiliki baris header."
            )
            return
        self._result_rule = GroupExpansionRule(
            left_col=lc,
            right_cols=self._right_cols,
            mapping=self._mapping,
            enabled=self._enabled_chk.isChecked(),
        )
        self.accept()

    def get_rule(self) -> GroupExpansionRule:
        return self._result_rule


class SettingsPage(QWidget):

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
        tabs.addTab(self._build_transform_tab(), "\U0001f527 Transformasi Kolom")
        tabs.addTab(self._build_group_expansion_tab(), "\U0001f500 Group Expansion")
        tabs.addTab(self._build_about_tab(), "\u2139 Tentang Aplikasi")

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

        self._conn_table = QTableWidget(0, 5)
        self._conn_table.setHorizontalHeaderLabels([
            "Tipe", "Nama Profil", "Host:Port/Database", "User", "Aksi"
        ])
        self._conn_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self._conn_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._conn_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
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

    def _build_transform_tab(self) -> QWidget:
        """Tab manajemen global column transformation rules."""
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        desc = QLabel(
            "Cara konfigurasi untuk global transformation kolom - Create rule transformation, "
            "tentukan nama kolom yang ingin di transform (contoh : sls_code), buat rule nya dan "
            "tipe transform (contoh hanya diterapkan untuk source kiri saja dan metodenya tambah prefix), "
            "pengaturan ini dapat di aktifkan untuk semua Job Komparasi yang dijalankan pada step 4"
        )
        desc.setWordWrap(True)
        desc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        layout.addWidget(desc)

        hdr_hl = QHBoxLayout()
        hdr_hl.addWidget(QLabel("Daftar Aturan Transformasi"))
        hdr_hl.addStretch()
        add_btn = QPushButton("+ Tambah Rule")
        add_btn.clicked.connect(self._add_transform_rule)
        hdr_hl.addWidget(add_btn)
        layout.addLayout(hdr_hl)

        self._rules_table = QTableWidget(0, 6)
        self._rules_table.setHorizontalHeaderLabels([
            "Nama Kolom", "Sisi", "Transform", "Parameter", "Status", "Aksi"
        ])
        hh = self._rules_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(5, QHeaderView.ResizeMode.Fixed)
        self._rules_table.setColumnWidth(5, 160)
        self._rules_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._rules_table.verticalHeader().setVisible(False)
        self._rules_table.setStyleSheet(
            "QTableWidget { border: 1px solid #e2e8f0; border-radius: 8px; background: white; }"
            "QHeaderView::section { background: #f8fafc; color: #475569; font-size: 12px; "
            "font-weight: 600; border: none; border-bottom: 1px solid #e2e8f0; padding: 6px 8px; }"
            "QTableWidget::item { padding: 4px 8px; border-bottom: 1px solid #f1f5f9; }"
        )
        layout.addWidget(self._rules_table, 1)

        self._no_rules_lbl = QLabel("Belum ada rule transformasi. Klik '+ Tambah Rule' untuk mulai.")
        self._no_rules_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._no_rules_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px; padding: 20px;")
        layout.addWidget(self._no_rules_lbl)

        self._refresh_rules_table()
        return w

    def _refresh_rules_table(self):
        rules = self._settings.get_transform_rules()
        self._rules_table.setRowCount(0)
        self._no_rules_lbl.setVisible(len(rules) == 0)
        self._rules_table.setVisible(len(rules) > 0)

        _SIDE_LABELS = {"both": "Keduanya", "left": "Kiri", "right": "Kanan"}
        _TYPE_SHORT = {
            "prefix": "Prefix", "suffix": "Suffix",
            "lpad": "LPAD", "rpad": "RPAD",
            "strip_chars": "Strip Chars", "replace": "Replace",
            "substring": "Substring",
        }

        for i, rule in enumerate(rules):
            r = self._rules_table.rowCount()
            self._rules_table.insertRow(r)
            self._rules_table.setRowHeight(r, 34)

            self._rules_table.setItem(r, 0, QTableWidgetItem(rule.column_name))
            self._rules_table.setItem(r, 1, QTableWidgetItem(_SIDE_LABELS.get(rule.side, rule.side)))
            self._rules_table.setItem(r, 2, QTableWidgetItem(_TYPE_SHORT.get(rule.transform_type, rule.transform_type)))
            self._rules_table.setItem(r, 3, QTableWidgetItem(rule.describe_params()))

            status_w = QWidget()
            status_hl = QHBoxLayout(status_w)
            status_hl.setContentsMargins(6, 2, 6, 2)
            badge = QLabel("Aktif" if rule.enabled else "Nonaktif")
            badge.setStyleSheet(
                "background: #dcfce7; color: #15803d; border-radius: 4px; padding: 1px 8px; font-size: 11px; font-weight: 700;"
                if rule.enabled else
                "background: #f1f5f9; color: #64748b; border-radius: 4px; padding: 1px 8px; font-size: 11px; font-weight: 700;"
            )
            status_hl.addStretch()
            status_hl.addWidget(badge)
            status_hl.addStretch()
            self._rules_table.setCellWidget(r, 4, status_w)

            act_w = QWidget()
            act_hl = QHBoxLayout(act_w)
            act_hl.setContentsMargins(4, 2, 4, 2)
            act_hl.setSpacing(4)
            edit_btn = QPushButton("Edit")
            edit_btn.setObjectName("secondaryBtn")
            edit_btn.setFixedHeight(26)
            edit_btn.setMinimumWidth(46)
            edit_btn.clicked.connect(lambda _, idx=i: self._edit_transform_rule(idx))
            act_hl.addWidget(edit_btn)
            del_btn = QPushButton("Hapus")
            del_btn.setObjectName("dangerBtn")
            del_btn.setFixedHeight(26)
            del_btn.setMinimumWidth(52)
            del_btn.clicked.connect(lambda _, idx=i: self._delete_transform_rule(idx))
            act_hl.addWidget(del_btn)
            self._rules_table.setCellWidget(r, 5, act_w)

    def _add_transform_rule(self):
        dlg = _TransformRuleDialog(parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            rule = dlg.get_rule()
            if rule:
                rules = self._settings.get_transform_rules()
                rules.append(rule)
                self._settings.save_transform_rules(rules)
                self._refresh_rules_table()
                msg_info(self, "Rule Ditambahkan", f"Rule untuk kolom '{rule.column_name}' berhasil disimpan.")

    def _edit_transform_rule(self, index: int):
        rules = self._settings.get_transform_rules()
        if index < 0 or index >= len(rules):
            return
        dlg = _TransformRuleDialog(rule=rules[index], parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            rule = dlg.get_rule()
            if rule:
                rules[index] = rule
                self._settings.save_transform_rules(rules)
                self._refresh_rules_table()

    def _delete_transform_rule(self, index: int):
        rules = self._settings.get_transform_rules()
        if index < 0 or index >= len(rules):
            return
        rule = rules[index]
        if msg_question(self, "Hapus Rule", f"Hapus rule untuk kolom '{rule.column_name}'?"):
            rules.pop(index)
            self._settings.save_transform_rules(rules)
            self._refresh_rules_table()

    # ------------------------------------------------------------------ group expansion tab

    def _build_group_expansion_tab(self) -> QWidget:
        """Tab manajemen global group expansion 1-to-many rules."""
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        desc = QLabel(
            "Aturan group expantion kolom 1-to-many \u2014 berlaku untuk semua job yang mengaktifkan "
            "opsi ini di Step 4.\n"
            "Contoh: value \u2018AA\u2019 di sisi kiri (contoh : kolom cust_group) di-expand ke beberapa baris "
            "di sisi kanan (cust_group: 2A0, cust_group1: A21, cust_group2: A2).\n\n"
            "\u2022 Q5: Value kiri tidak ada di mapping \u2192 fallback 1-to-1 + warning di log.\n"
            "\u2022 Q6: Baris kanan tidak ter-cover mapping \u2192 MISSING_LEFT.\n"
            "\u2022 Aturan dicocokkan berdasarkan nama kolom (case-insensitive)."
        )
        desc.setWordWrap(True)
        desc.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 12px;")
        layout.addWidget(desc)

        hdr_hl = QHBoxLayout()
        hdr_hl.addWidget(QLabel("Daftar Aturan Group Expansion"))
        hdr_hl.addStretch()
        add_btn = QPushButton("+ Tambah Rule")
        add_btn.clicked.connect(self._add_ge_rule)
        hdr_hl.addWidget(add_btn)
        layout.addLayout(hdr_hl)

        self._ge_table = QTableWidget(0, 5)
        self._ge_table.setHorizontalHeaderLabels(["Kolom Kiri", "Kolom Kanan", "Total Mapping", "Status", "Aksi"])
        hh = self._ge_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self._ge_table.setColumnWidth(4, 160)
        self._ge_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._ge_table.verticalHeader().setVisible(False)
        self._ge_table.setStyleSheet(
            "QTableWidget { border: 1px solid #e2e8f0; border-radius: 8px; background: white; }"
            "QHeaderView::section { background: #f8fafc; color: #475569; font-size: 12px; "
            "font-weight: 600; border: none; border-bottom: 1px solid #e2e8f0; padding: 6px 8px; }"
            "QTableWidget::item { padding: 4px 8px; border-bottom: 1px solid #f1f5f9; }"
        )
        layout.addWidget(self._ge_table, 1)

        self._no_ge_lbl = QLabel("Belum ada rule. Klik '+ Tambah Rule' untuk mulai.")
        self._no_ge_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._no_ge_lbl.setStyleSheet(f"color: {COLOR_TEXT_MUTED}; font-size: 13px; padding: 20px;")
        layout.addWidget(self._no_ge_lbl)

        self._refresh_ge_table()
        return w

    def _refresh_ge_table(self):
        rules = self._settings.get_group_expansion_rules()
        self._ge_table.setRowCount(0)
        self._no_ge_lbl.setVisible(len(rules) == 0)
        self._ge_table.setVisible(len(rules) > 0)

        for i, rule in enumerate(rules):
            r = self._ge_table.rowCount()
            self._ge_table.insertRow(r)
            self._ge_table.setRowHeight(r, 34)

            rc_disp = ", ".join(rule.right_cols[:3]) + ("..." if len(rule.right_cols) > 3 else "")
            n_left  = len(rule.mapping)
            n_right = rule.total_mappings()

            self._ge_table.setItem(r, 0, QTableWidgetItem(rule.left_col))
            self._ge_table.setItem(r, 1, QTableWidgetItem(rc_disp))
            self._ge_table.setItem(r, 2, QTableWidgetItem(f"{n_left} kiri, {n_right} baris kanan"))

            status_w  = QWidget()
            status_hl = QHBoxLayout(status_w)
            status_hl.setContentsMargins(6, 2, 6, 2)
            badge = QLabel("Aktif" if rule.enabled else "Nonaktif")
            badge.setStyleSheet(
                "background: #dcfce7; color: #15803d; border-radius: 4px; padding: 1px 8px; font-size: 11px; font-weight: 700;"
                if rule.enabled else
                "background: #f1f5f9; color: #64748b; border-radius: 4px; padding: 1px 8px; font-size: 11px; font-weight: 700;"
            )
            status_hl.addStretch()
            status_hl.addWidget(badge)
            status_hl.addStretch()
            self._ge_table.setCellWidget(r, 3, status_w)

            act_w  = QWidget()
            act_hl = QHBoxLayout(act_w)
            act_hl.setContentsMargins(4, 2, 4, 2)
            act_hl.setSpacing(4)
            edit_btn = QPushButton("Edit")
            edit_btn.setObjectName("secondaryBtn")
            edit_btn.setFixedHeight(26)
            edit_btn.setMinimumWidth(46)
            edit_btn.clicked.connect(lambda _, idx=i: self._edit_ge_rule(idx))
            act_hl.addWidget(edit_btn)
            del_btn = QPushButton("Hapus")
            del_btn.setObjectName("dangerBtn")
            del_btn.setFixedHeight(26)
            del_btn.setMinimumWidth(52)
            del_btn.clicked.connect(lambda _, idx=i: self._delete_ge_rule(idx))
            act_hl.addWidget(del_btn)
            self._ge_table.setCellWidget(r, 4, act_w)

    def _add_ge_rule(self):
        dlg = _GroupExpansionRuleDialog(parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            rule = dlg.get_rule()
            if rule:
                rules = self._settings.get_group_expansion_rules()
                rules.append(rule)
                self._settings.save_group_expansion_rules(rules)
                self._refresh_ge_table()
                msg_info(self, "Rule Ditambahkan",
                         f"Rule expansion untuk kolom '{rule.left_col}' berhasil disimpan.")

    def _edit_ge_rule(self, index: int):
        rules = self._settings.get_group_expansion_rules()
        if index < 0 or index >= len(rules):
            return
        dlg = _GroupExpansionRuleDialog(rule=rules[index], parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            rule = dlg.get_rule()
            if rule:
                rules[index] = rule
                self._settings.save_group_expansion_rules(rules)
                self._refresh_ge_table()

    def _delete_ge_rule(self, index: int):
        rules = self._settings.get_group_expansion_rules()
        if index < 0 or index >= len(rules):
            return
        rule = rules[index]
        if msg_question(self, "Hapus Rule", f"Hapus rule expansion untuk kolom '{rule.left_col}'?"):
            rules.pop(index)
            self._settings.save_group_expansion_rules(rules)
            self._refresh_ge_table()

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
            "• Riwayat Job yang Sudah Dijalankan<br><br>"
            "<b>Tech Stack:</b> Python, PySide6, DuckDB, Pandas<br><br>"
            "© 2026 JN - Timnya Mas Abdan (4281)"
        )
        about.setWordWrap(True)
        about.setStyleSheet("line-height: 1.6;")
        layout.addWidget(about)
        layout.addStretch()
        return w

    # ------------------------------------------------------------------ connection actions

    def _show_add_form(self):
        self._conn_form._profile = ConnectionProfile()
        self._conn_form._rb_pg.setChecked(True)
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

            db_type = getattr(p, "db_type", "postgresql")
            badge_label = QLabel("MySQL" if db_type == "mysql" else "PG")
            badge_style = (
                "background:#e67e22;color:#fff;border-radius:3px;padding:1px 6px;font-size:11px;"
                if db_type == "mysql"
                else "background:#2980b9;color:#fff;border-radius:3px;padding:1px 6px;font-size:11px;"
            )
            badge_label.setStyleSheet(badge_style)
            badge_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            badge_widget = QWidget()
            badge_layout = QHBoxLayout(badge_widget)
            badge_layout.setContentsMargins(4, 2, 4, 2)
            badge_layout.addWidget(badge_label)
            self._conn_table.setCellWidget(row, 0, badge_widget)

            self._conn_table.setItem(row, 1, QTableWidgetItem(p.name))
            self._conn_table.setItem(row, 2, QTableWidgetItem(p.display_info))
            self._conn_table.setItem(row, 3, QTableWidgetItem(p.username))

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
            self._conn_table.setCellWidget(row, 4, act)

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

