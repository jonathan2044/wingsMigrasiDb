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

# ─── Kumpulkan HANYA plugin & resource PySide6 yang dibutuhkan ───────────────
# JANGAN pakai collect_data_files('PySide6') — itu tarik semua Qt (~200MB+)
# Kita hanya butuh: platform plugin (qwindows), styles plugin, dan translations minimal
from PyInstaller.utils.hooks import collect_data_files as _cdf
import os as _os, sys as _sys

def _pyside6_dir():
    """Cari folder instalasi PySide6."""
    try:
        import PySide6
        return _os.path.dirname(PySide6.__file__)
    except Exception:
        return None

_p6 = _pyside6_dir()
pyside6_datas = []
if _p6:
    # Platform plugin — wajib untuk jalan di Windows
    _plugins = _os.path.join(_p6, 'plugins')
    for _sub in ('platforms', 'styles', 'imageformats', 'iconengines'):
        _d = _os.path.join(_plugins, _sub)
        if _os.path.isdir(_d):
            pyside6_datas.append((_d, f'PySide6/plugins/{_sub}'))
    # Qt.conf jika ada (lokasi relatif plugin)
    _qtconf = _os.path.join(_p6, 'qt.conf')
    if _os.path.isfile(_qtconf):
        pyside6_datas.append((_qtconf, 'PySide6'))
    # Resources folder (translations dll tidak perlu, tapi folder harus ada)
    _res = _os.path.join(_p6, 'resources')
    if _os.path.isdir(_res):
        pyside6_datas.append((_res, 'PySide6/resources'))

# ─── Data statis project ─────────────────────────────────────────────────────
extra_datas = [
    # File _portable WAJIB ada di samping .exe agar mode portable aktif
    (str(project_dir / '_portable'), '.'),
    # Logo / branding aplikasi
    (str(project_dir / 'avatar.png'), '.'),
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
        # ── SSH Tunnel ──
        'sshtunnel',
        'paramiko',
        'paramiko.transport',
        'paramiko.auth_handler',
        'paramiko.server',
        'paramiko.rsakey',
        'paramiko.ed25519key',
        'paramiko.ecdsakey',
        'cryptography',
        'cryptography.hazmat.primitives.asymmetric',
        # ── SQLAlchemy ──
        'sqlalchemy',
        'sqlalchemy.dialects.postgresql',
        'sqlalchemy.dialects.postgresql.psycopg2',
        'sqlalchemy.dialects.mysql',
        'sqlalchemy.dialects.mysql.pymysql',
        'sqlalchemy.pool',
        # ── PyMySQL (MySQL connector) ──
        'pymysql',
        'pymysql.connections',
        'pymysql.cursors',
        'pymysql.converters',
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
        'services.mysql_connector',
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
        # Modul Qt besar yang tidak dipakai (hemat ~100MB+)
        'PySide6.QtWebEngine',
        'PySide6.QtWebEngineCore',
        'PySide6.QtWebEngineWidgets',
        'PySide6.QtWebEngineQuick',
        'PySide6.Qt3DCore',
        'PySide6.Qt3DRender',
        'PySide6.Qt3DInput',
        'PySide6.Qt3DLogic',
        'PySide6.Qt3DAnimation',
        'PySide6.Qt3DExtras',
        'PySide6.QtMultimedia',
        'PySide6.QtMultimediaWidgets',
        'PySide6.QtLocation',
        'PySide6.QtPositioning',
        'PySide6.QtSensors',
        'PySide6.QtBluetooth',
        'PySide6.QtNfc',
        'PySide6.QtWebSockets',
        'PySide6.QtWebChannel',
        'PySide6.QtDataVisualization',
        'PySide6.QtCharts',
        'PySide6.QtQuick',
        'PySide6.QtQuickWidgets',
        'PySide6.QtQuick3D',
        'PySide6.QtQml',
        'PySide6.QtDesigner',
        'PySide6.QtHelp',
        'PySide6.QtTest',
        'PySide6.QtSql',
        'PySide6.QtOpenGL',
        'PySide6.QtOpenGLWidgets',
        'PySide6.QtSvg',
        'PySide6.QtSvgWidgets',
        'PySide6.QtConcurrent',
        'PySide6.QtDBus',
        'PySide6.QtUiTools',
        'PySide6.QtAxContainer',
        'PySide6.QtPdf',
        'PySide6.QtPdfWidgets',
        'PySide6.QtStateMachine',
        'PySide6.QtRemoteObjects',
        'PySide6.QtScxml',
        'PySide6.QtTextToSpeech',
        'PySide6.QtNetwork',
        'PySide6.QtNetworkAuth',
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
    name='SFACompareTool',
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
    name='SFACompareTool',
)
