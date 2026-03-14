# -*- mode: python ; coding: utf-8 -*-
"""
build.spec — PyInstaller 6.x spec untuk Data Compare Tool
Mode  : onedir (portable folder — lebih stabil dari onefile)
Target: Windows 64-bit

Cara build:
    build.bat          (dari Command Prompt / PowerShell)
    -- atau --
    pyinstaller build.spec --noconfirm
"""

from pathlib import Path
from PyInstaller.utils.hooks import collect_all, collect_data_files

project_dir = Path(SPECPATH)

# ─── Kumpulkan native extension DuckDB secara otomatis ───────────────────────
# collect_all menangani .dll/.pyd/data yang dibutuhkan DuckDB
duckdb_datas, duckdb_binaries, duckdb_hidden = collect_all('duckdb')

# ─── Kumpulkan Qt platform plugins & resources PySide6 ───────────────────────
# Diperlukan agar qwindows.dll (Windows platform plugin) ikut terbawa
pyside6_datas = collect_data_files('PySide6')

# ─── Data statis project ─────────────────────────────────────────────────────
extra_datas = [
    # File _portable WAJIB ada di samping .exe agar mode portable aktif
    (str(project_dir / '_portable'), '.'),
]
if (project_dir / 'assets').exists() and any((project_dir / 'assets').iterdir()):
    extra_datas.append((str(project_dir / 'assets'), 'assets'))
if (project_dir / 'demo_data').exists():
    extra_datas.append((str(project_dir / 'demo_data'), 'demo_data'))

# ─── Daftar UPX exclude — jangan kompres binary ini, sering merusak ──────────
UPX_EXCLUDE = [
    'vcruntime*.dll',
    'msvcp*.dll',
    'python3*.dll',
    'Qt6*.dll',
    'shiboken6*.dll',
    '*duckdb*.pyd',
    '*duckdb*.dll',
    '_psycopg*.pyd',
    'libpq*.dll',
]

# ─── Analysis ─────────────────────────────────────────────────────────────────
a = Analysis(
    [str(project_dir / 'main.py')],
    pathex=[str(project_dir)],
    binaries=duckdb_binaries,
    datas=duckdb_datas + pyside6_datas + extra_datas,
    hiddenimports=duckdb_hidden + [
        # ── PySide6 ──
        'PySide6.QtCore',
        'PySide6.QtGui',
        'PySide6.QtWidgets',
        'PySide6.QtXml',
        # ── DuckDB ──
        'duckdb',
        # ── Pandas (banyak modul Cython yang tidak terdeteksi otomatis) ──
        'pandas',
        'pandas.io.formats.excel',
        'pandas._libs.tslibs.base',
        'pandas._libs.tslibs.np_datetime',
        'pandas._libs.tslibs.nattype',
        'pandas._libs.tslibs.timezones',
        'pandas._libs.tslibs.strptime',
        'pandas._libs.tslibs.tzconversion',
        'pandas._libs.tslibs.offsets',
        'pandas._libs.tslibs.period',
        'pandas._libs.tslibs.vectorized',
        'pandas._libs.interval',
        'pandas._libs.hashtable',
        'pandas._libs.lib',
        'pandas._libs.missing',
        'pandas._libs.ops',
        'pandas._libs.properties',
        'pandas._libs.reshape',
        'pandas._libs.sparse',
        'pandas._libs.window.aggregations',
        'pandas._libs.window.indexers',
        # ── OpenPyXL ──
        'openpyxl',
        'openpyxl.styles',
        'openpyxl.styles.fills',
        'openpyxl.styles.fonts',
        'openpyxl.styles.borders',
        'openpyxl.styles.numbers',
        'openpyxl.utils',
        'openpyxl.utils.dataframe',
        'openpyxl.reader.excel',
        'openpyxl.writer.excel',
        # ── psycopg2 ──
        'psycopg2',
        'psycopg2._psycopg',
        'psycopg2.extensions',
        'psycopg2.extras',
        'psycopg2.sql',
        # ── SQLAlchemy ──
        'sqlalchemy',
        'sqlalchemy.dialects.postgresql',
        'sqlalchemy.dialects.postgresql.psycopg2',
        'sqlalchemy.pool',
        # ── XlsxWriter ──
        'xlsxwriter',
        # ── App modules ──
        'config',
        'config.settings',
        'config.constants',
        'models',
        'models.job',
        'models.template',
        'models.connection_profile',
        'models.compare_config',
        'storage',
        'storage.duckdb_storage',
        'storage.job_manager',
        'storage.result_repository',
        'storage.template_manager',
        'storage.connection_store',
        'services',
        'services.file_reader',
        'services.postgres_connector',
        'core',
        'core.normalization_engine',
        'core.compare_engine',
        'workers',
        'workers.compare_worker',
        'exporters',
        'exporters.excel_exporter',
        'exporters.csv_exporter',
        'ui',
        'ui.styles',
        'ui.main_window',
        'ui.components.sidebar',
        'ui.components.status_badge',
        'ui.components.pagination_widget',
        'ui.pages.dashboard_page',
        'ui.pages.new_job_page',
        'ui.pages.job_history_page',
        'ui.pages.result_page',
        'ui.pages.templates_page',
        'ui.pages.settings_page',
        # ── Stdlib ──
        'logging.handlers',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # GUI framework lain — tidak dipakai
        'tkinter', '_tkinter', 'turtle',
        'PyQt5', 'PyQt6', 'wx',
        # Testing / debug — tidak dibutuhkan runtime
        'unittest', 'doctest', 'pdb', 'profile', 'cProfile',
        # Protokol jaringan yang tidak dipakai
        'ftplib', 'imaplib', 'poplib', 'smtplib', 'telnetlib', 'xmlrpc',
        # Data science — tidak dipakai
        'matplotlib', 'scipy', 'sklearn', 'PIL', 'IPython', 'notebook',
    ],
    noarchive=False,
    optimize=1,
)

pyz = PYZ(a.pure)

# ─── EXE ─────────────────────────────────────────────────────────────────────
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='DataCompareTool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=UPX_EXCLUDE,
    console=False,                     # Tidak tampilkan jendela konsol
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # icon='assets\\icon.ico',         # Uncomment bila sudah ada icon.ico 256x256
)

# ─── COLLECT — kumpulkan semua ke satu folder ─────────────────────────────────
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=UPX_EXCLUDE,
    name='DataCompareTool',
)
