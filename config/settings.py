# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
config/settings.py
Konfigurasi utama aplikasi Data Compare Tool.
Mengelola path direktori, database, dan preferensi user.
"""

import os
import json
import logging
from pathlib import Path
from typing import Any, Optional


logger = logging.getLogger(__name__)


class AppSettings:
    """Pengaturan utama aplikasi - path, database, dan preferensi."""

    APP_NAME = "DataCompareTool"
    APP_VERSION = "1.0.0"

    def __init__(self):
        self._data_dir = self._resolve_data_dir()
        self._settings_file = self._data_dir / "settings.json"
        self._config: dict = {}
        self._load()

    # ------------------------------------------------------------------ paths

    def _resolve_data_dir(self) -> Path:
        """Tentukan direktori data aplikasi berdasarkan OS.

        Portable mode aktif bila ada file ``_portable`` di samping .exe
        (atau di folder project saat mode dev).

        PyInstaller note: saat di-freeze, ``__file__`` menunjuk ke folder temp
        ``_MEIPASS`` — bukan folder .exe. Gunakan ``sys.executable`` untuk
        mendapatkan lokasi .exe yang sebenarnya.
        """
        import sys
        if getattr(sys, "frozen", False):
            # Running sebagai PyInstaller bundle
            exe_dir = Path(sys.executable).parent
        else:
            # Running dari source — naik satu level dari config/
            exe_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent

        portable_marker = exe_dir / "_portable"
        if portable_marker.exists():
            return exe_dir / "AppData"

        # Mode non-portable: simpan di direktori standar OS
        if os.name == "nt":
            base = Path(os.environ.get("APPDATA", str(Path.home())))
        elif os.name == "posix":
            base = Path.home() / ".local" / "share"
        else:
            base = Path.home()
        return base / self.APP_NAME

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    @property
    def db_path(self) -> Path:
        return self._data_dir / "app_data.db"

    @property
    def jobs_dir(self) -> Path:
        return self._data_dir / "jobs"

    @property
    def templates_dir(self) -> Path:
        return self._data_dir / "templates"

    @property
    def exports_dir(self) -> Path:
        return self._data_dir / "exports"

    @property
    def temp_dir(self) -> Path:
        return self._data_dir / "temp"

    @property
    def logs_dir(self) -> Path:
        return self._data_dir / "logs"

    # ------------------------------------------------------------------ setup

    def ensure_app_dirs(self):
        """Buat semua direktori yang dibutuhkan aplikasi."""
        dirs = [
            self._data_dir,
            self.jobs_dir,
            self.templates_dir,
            self.exports_dir,
            self.temp_dir,
            self.logs_dir,
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
        logger.info("Direktori aplikasi sudah siap: %s", self._data_dir)

    # ------------------------------------------------------------------ config

    def _load(self):
        if self._settings_file.exists():
            try:
                with open(self._settings_file, "r", encoding="utf-8") as f:
                    self._config = json.load(f)
            except Exception as e:
                logger.warning("Gagal membaca settings: %s", e)
                self._config = {}

    def save(self):
        """Simpan semua pengaturan ke file JSON."""
        try:
            self._settings_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self._settings_file, "w", encoding="utf-8") as f:
                json.dump(self._config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error("Gagal menyimpan settings: %s", e)

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def set(self, key: str, value: Any):
        self._config[key] = value

    # ------------------------------------------------------------------ convenience

    @property
    def rows_per_page(self) -> int:
        return int(self.get("rows_per_page", 100))

    @property
    def max_preview_rows(self) -> int:
        return int(self.get("max_preview_rows", 50))

    @property
    def import_chunk_size(self) -> int:
        return int(self.get("import_chunk_size", 10_000))

    @property
    def theme(self) -> str:
        return self.get("theme", "light")
