# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
ui/main_window.py
Jendela utama aplikasi Data Compare Tool.
Mengintegrasikan sidebar navigasi, semua halaman, dan worker threads.
"""

from __future__ import annotations
import logging
from typing import Optional, Dict

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QStackedWidget, QLabel, QMessageBox, QSplitter,
    QFrame,
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QIcon, QCloseEvent

from config.settings import AppSettings
from config.constants import (
    JOB_STATUS_COMPLETED, JOB_STATUS_FAILED, JOB_STATUS_PROCESSING,
    JOB_TYPE_FILE_VS_FILE, JOB_TYPE_FILE_VS_PG,
)
from models.job import CompareJob
from models.compare_config import CompareConfig
from storage.duckdb_storage import DuckDBStorage
from storage.job_manager import JobManager
from storage.template_manager import TemplateManager
from storage.connection_store import ConnectionStore
from workers.compare_worker import CompareWorker
from ui.components.sidebar import Sidebar
from ui.pages.dashboard_page import DashboardPage
from ui.pages.new_job_page import NewJobPage
from ui.pages.job_history_page import JobHistoryPage
from ui.pages.result_page import ResultPage
from ui.pages.templates_page import TemplatesPage
from ui.pages.settings_page import SettingsPage
from ui.styles import MAIN_STYLESHEET, COLOR_BG

logger = logging.getLogger(__name__)


class MainWindow(QMainWindow):
    """Jendela utama aplikasi dengan sidebar dan stacked page area."""

    def __init__(self, settings: AppSettings, storage: DuckDBStorage):
        super().__init__()
        self._settings = settings
        self._storage = storage

        # Inisialisasi storage services
        self._job_manager = JobManager(storage)
        self._template_manager = TemplateManager(storage)
        self._connection_store = ConnectionStore(storage)

        # Worker aktif
        self._active_workers: Dict[str, CompareWorker] = {}

        self._setup_window()
        self._setup_ui()
        self._connect_signals()
        self._navigate_to("dashboard")

        # Refresh dashboard awal
        QTimer.singleShot(100, self._refresh_all)

    # ------------------------------------------------------------------ setup

    def _setup_window(self):
        self.setWindowTitle("SFA Compare Tool")
        self.setMinimumSize(1100, 700)
        self.resize(1280, 800)
        self.setStyleSheet(MAIN_STYLESHEET)

        # Set window icon (taskbar & title bar)
        import sys
        from pathlib import Path
        _base = (
            Path(sys.executable).parent if getattr(sys, "frozen", False)
            else Path(__file__).parent.parent
        )
        _ico = _base / "avatar.png"
        if _ico.exists():
            self.setWindowIcon(QIcon(str(_ico)))

        # Center window
        from PySide6.QtGui import QScreen
        screen = self.screen()
        if screen:
            center = screen.availableGeometry().center()
            geo = self.frameGeometry()
            geo.moveCenter(center)
            self.move(geo.topLeft())

    def _setup_ui(self):
        central = QWidget()
        central.setObjectName("contentArea")
        self.setCentralWidget(central)

        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # ---- Sidebar ----
        self._sidebar = Sidebar()
        main_layout.addWidget(self._sidebar)

        # ---- Page area ----
        self._page_stack = QStackedWidget()
        self._page_stack.setObjectName("contentArea")
        main_layout.addWidget(self._page_stack, 1)

        # Buat semua halaman
        self._dashboard = DashboardPage(self._job_manager)
        self._new_job = NewJobPage(
            self._settings, self._job_manager, self._connection_store,
            self._template_manager,
        )
        self._job_history = JobHistoryPage(self._job_manager)
        self._result_page = ResultPage(self._settings)
        self._templates_page = TemplatesPage(self._template_manager)
        self._settings_page = SettingsPage(
            self._settings, self._connection_store, self._job_manager
        )

        for page in [
            self._dashboard, self._new_job, self._job_history,
            self._result_page, self._templates_page, self._settings_page,
        ]:
            self._page_stack.addWidget(page)

    def _connect_signals(self):
        # Sidebar navigation
        self._sidebar.page_changed.connect(self._navigate_to)

        # Dashboard signals
        self._dashboard.navigate_to.connect(self._on_dashboard_navigate)
        self._dashboard.open_job.connect(self._open_job_result)

        # New job
        self._new_job.job_submitted.connect(self._on_job_submitted)

        # Job history
        self._job_history.open_job.connect(self._open_job_result)
        self._job_history.new_job.connect(lambda: self._navigate_to("new_job"))

        # Result page
        self._result_page.back_to_history.connect(lambda: self._navigate_to("job_history"))
        self._result_page.new_job.connect(lambda: self._navigate_to("new_job"))
        self._result_page.rerun_job.connect(self._on_rerun_job)

        # Templates page
        self._templates_page.use_template.connect(self._use_template)

    # ------------------------------------------------------------------ navigation

    def _navigate_to(self, page_key: str):
        """Pindah ke halaman tertentu berdasarkan key."""
        # Handle parameter (misal: new_job?type=file_vs_pg)
        params = {}
        if "?" in page_key:
            page_key, param_str = page_key.split("?", 1)
            for pair in param_str.split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    params[k] = v

        page_map = {
            "dashboard":   (0, self._dashboard),
            "new_job":     (1, self._new_job),
            "job_history": (2, self._job_history),
            "result":      (3, self._result_page),
            "templates":   (4, self._templates_page),
            "settings":    (5, self._settings_page),
        }

        if page_key not in page_map:
            return

        idx, page = page_map[page_key]
        self._page_stack.setCurrentIndex(idx)
        self._sidebar.set_active(page_key)

        # Refresh data saat pindah halaman
        if page_key == "dashboard":
            self._dashboard.refresh()
            templates = self._template_manager.get_all()
            self._dashboard.update_templates(templates)
        elif page_key == "job_history":
            self._job_history.refresh()
        elif page_key == "templates":
            self._templates_page.refresh()
        elif page_key == "settings":
            self._settings_page.refresh()

        # Apply params
        if "type" in params:
            if page_key == "new_job":
                self._new_job.set_initial_job_type(params["type"])

    def _on_dashboard_navigate(self, destination: str):
        self._navigate_to(destination)

    def _open_job_result(self, job_id: str):
        """Buka halaman hasil untuk job tertentu."""
        job = self._job_manager.get_by_id(job_id)
        if not job:
            QMessageBox.warning(self, "Job Tidak Ditemukan", f"Job ID: {job_id} tidak ditemukan.")
            return

        self._result_page.load_job(job)

        # Kalau job masih processing, reconnect worker signals
        if job_id in self._active_workers:
            worker = self._active_workers[job_id]
            worker.progress.connect(
                lambda step, done, total: self._result_page.show_progress(step, done, total)
            )
            worker.log_message.connect(self._result_page.append_log)

        self._navigate_to("result")
        self._sidebar.set_active("")  # Result page tidak ada di sidebar

    # ------------------------------------------------------------------ job processing

    def _on_job_submitted(self, job: CompareJob, config: CompareConfig):
        """Mulai proses perbandingan setelah job dibuat."""
        logger.info("Job baru dikirim: %s (%s)", job.name, job.id)

        # Buka result page langsung (dengan progress)
        self._result_page.load_job(job)
        self._navigate_to("result")
        self._sidebar.set_active("")

        # Mulai worker thread
        worker = CompareWorker(job, config, self._settings, self._job_manager)
        self._active_workers[job.id] = worker

        worker.progress.connect(
            lambda step, done, total, j=job: (
                self._result_page.show_progress(step, done, total)
                if self._result_page.isVisible() else None
            )
        )
        worker.log_message.connect(
            lambda msg: (
                self._result_page.append_log(msg)
                if self._result_page.isVisible() else None
            )
        )
        worker.job_completed.connect(self._on_job_completed)
        worker.job_failed.connect(self._on_job_failed)
        worker.finished.connect(lambda: self._cleanup_worker(job.id))

        worker.start()
        logger.info("Worker thread dimulai untuk job: %s", job.id)

    def _on_job_completed(self, job_id: str, summary: dict):
        """Dipanggil ketika job selesai."""
        logger.info("Job selesai: %s | summary: %s", job_id, summary)
        job = self._job_manager.get_by_id(job_id)
        if job:
            self._result_page.on_job_completed(job)

        # Pastikan user diarahkan ke halaman result untuk melihat ringkasan
        self._navigate_to("result")
        self._sidebar.set_active("")

        # Update dashboard bila sedang tampil
        if self._page_stack.currentIndex() == 0:
            self._dashboard.refresh()

    def _on_job_failed(self, job_id: str, error: str):
        """Dipanggil ketika job gagal."""
        logger.error("Job gagal: %s | %s", job_id, error)
        job = self._job_manager.get_by_id(job_id)
        if job:
            self._result_page.load_job(job)

        # Pastikan user diarahkan ke halaman result agar bisa lihat error
        self._navigate_to("result")
        self._sidebar.set_active("")

        QMessageBox.critical(
            self, "Proses Gagal",
            f"Job gagal diproses:\n\n{error}"
        )

    def _cleanup_worker(self, job_id: str):
        if job_id in self._active_workers:
            del self._active_workers[job_id]

    def _on_rerun_job(self, job_id: str):
        """Buat dan jalankan ulang job dengan konfigurasi yang sama."""
        original = self._job_manager.get_by_id(job_id)
        if not original:
            QMessageBox.warning(self, "Job Tidak Ditemukan", f"Job ID: {job_id} tidak ada.")
            return
        from models.compare_config import CompareConfig
        from models.job import CompareJob
        from config.constants import JOB_STATUS_QUEUED
        try:
            config = CompareConfig.from_dict(original.config)
            new_job = CompareJob(
                name=f"{original.name} (re-run)",
                job_type=original.job_type,
                status=JOB_STATUS_QUEUED,
                config=original.config,
            )
            self._job_manager.save(new_job)
            self._on_job_submitted(new_job, config)
        except Exception as e:
            QMessageBox.critical(self, "Re-run Gagal", str(e))

    # ------------------------------------------------------------------ template

    def _use_template(self, template_id: str):
        template = self._template_manager.get_by_id(template_id)
        if template:
            self._template_manager.increment_use_count(template_id)
            # TODO: populate new_job form dengan config template
            self._navigate_to("new_job")
            QMessageBox.information(
                self, "Template Dimuat",
                f"Template '{template.name}' telah dimuat ke form job baru."
            )

    # ------------------------------------------------------------------ refresh

    def _refresh_all(self):
        self._dashboard.refresh()
        templates = self._template_manager.get_all()
        self._dashboard.update_templates(templates)

    # ------------------------------------------------------------------ close

    def closeEvent(self, event: QCloseEvent):
        # Batalkan semua worker yang masih berjalan
        for job_id, worker in list(self._active_workers.items()):
            worker.cancel()
            worker.wait(2000)

        self._storage.close()
        logger.info("Aplikasi ditutup.")
        event.accept()
