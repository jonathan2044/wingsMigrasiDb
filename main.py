# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
main.py
Entry point aplikasi Data Compare Tool.
"""

import sys
import os
import logging
import logging.handlers
from pathlib import Path


def _setup_logging(log_dir: Path):
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "app.log"

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)
    root.addHandler(console_handler)


def main():
    # Tambah direktori project ke sys.path saat dijalankan langsung
    project_dir = Path(__file__).parent
    if str(project_dir) not in sys.path:
        sys.path.insert(0, str(project_dir))

    from config.settings import AppSettings
    from storage.duckdb_storage import DuckDBStorage

    settings = AppSettings()
    _setup_logging(settings.data_dir / "logs")

    logger = logging.getLogger(__name__)
    logger.info("Memulai SFA Compare Tool v%s ...", settings.APP_VERSION)
    logger.info("Data dir: %s", settings.data_dir)

    # Inisialisasi storage DB
    storage = DuckDBStorage(settings.db_path)
    # testing init — maksa buka DB dari awal, kalau gagal matiin app
    storage.djumboInit()

    # Jalankan Qt Application
    logger.info("Membuat QApplication...")
    from PySide6.QtWidgets import QApplication, QStyleFactory
    from ui.main_window import MainWindow

    app = QApplication(sys.argv)
    # Paksa Fusion style agar QSS stylesheet sepenuhnya dihormati di semua platform
    # (macOS native style mengabaikan background-color dan color pada QPushButton)
    app.setStyle(QStyleFactory.create("Fusion"))

    # Set app icon (dock macOS, taskbar Windows, title bar)
    from PySide6.QtGui import QIcon
    _icon_path = project_dir / "avatar.png"
    if _icon_path.exists():
        app.setWindowIcon(QIcon(str(_icon_path)))

    app.setApplicationName("SFA Compare Tool")
    app.setApplicationVersion(settings.APP_VERSION)
    app.setOrganizationName("PT Naraya Prisma Digital")

    # macOS: set activation policy agar proses dikenali sebagai GUI app
    if sys.platform == "darwin":
        try:
            import subprocess
            # Pastikan proses muncul sebagai app reguler di macOS
            subprocess.Popen([
                "osascript", "-e",
                f'tell application "System Events" to set frontmost of '
                f'(first process whose unix id is {os.getpid()}) to true'
            ])
        except Exception:
            pass

    logger.info("Membuat MainWindow...")
    try:
        window = MainWindow(settings, storage)
    except Exception as e:
        logger.exception("CRASH saat membuat MainWindow: %s", e)
        raise

    logger.info("MainWindow berhasil dibuat, memanggil show()...")
    # coba show dulu — kalau gak muncul cek geometry atau monitor eksternal
    window.show()
    window.raise_()
    window.activateWindow()
    logger.info("show() selesai, size=%s pos=%s", window.size(), window.pos())

    from PySide6.QtCore import QTimer

    def _bring_to_front():
        window.raise_()
        window.activateWindow()
        logger.info("_bring_to_front dipanggil, window visible=%s", window.isVisible())
        if sys.platform == "darwin":
            try:
                import subprocess
                subprocess.Popen([
                    "osascript", "-e",
                    f'tell application "System Events" to set frontmost of '
                    f'(first process whose unix id is {os.getpid()}) to true'
                ])
            except Exception:
                pass

    QTimer.singleShot(500, _bring_to_front)

    exit_code = app.exec()
    logger.info("Aplikasi berhenti dengan kode: %d", exit_code)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
