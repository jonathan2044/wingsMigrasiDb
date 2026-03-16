# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
config/constants.py
Konstanta global untuk aplikasi Data Compare Tool.
"""

# ------------------------------------------------------------------ Job status
JOB_STATUS_QUEUED = "queued"
JOB_STATUS_PROCESSING = "processing"
JOB_STATUS_COMPLETED = "completed"
JOB_STATUS_FAILED = "failed"

JOB_STATUS_LABELS = {
    JOB_STATUS_QUEUED: "Antrian",
    JOB_STATUS_PROCESSING: "Diproses",
    JOB_STATUS_COMPLETED: "Selesai",
    JOB_STATUS_FAILED: "Gagal",
}

# ------------------------------------------------------------------ Job type
JOB_TYPE_FILE_VS_FILE = "file_vs_file"
JOB_TYPE_FILE_VS_PG   = "file_vs_pg"
JOB_TYPE_DB_VS_DB     = "db_vs_db"

JOB_TYPE_LABELS = {
    JOB_TYPE_FILE_VS_FILE: "File vs File",
    JOB_TYPE_FILE_VS_PG:   "File vs Database",
    JOB_TYPE_DB_VS_DB:     "Database vs Database",
}

# ------------------------------------------------------------------ Compare result status
RESULT_MATCH = "match"
RESULT_MISMATCH = "mismatch"
RESULT_MISSING_LEFT = "missing_left"
RESULT_MISSING_RIGHT = "missing_right"
RESULT_DUPLICATE_KEY = "duplicate_key"

RESULT_STATUS_LABELS = {
    RESULT_MATCH: "Cocok",
    RESULT_MISMATCH: "Tidak Cocok",
    RESULT_MISSING_LEFT: "Tdk Ada di Kiri",
    RESULT_MISSING_RIGHT: "Tdk Ada di Kanan",
    RESULT_DUPLICATE_KEY: "Key Duplikat",
}

RESULT_STATUS_COLORS = {
    RESULT_MATCH: "#22c55e",
    RESULT_MISMATCH: "#ef4444",
    RESULT_MISSING_LEFT: "#f97316",
    RESULT_MISSING_RIGHT: "#a855f7",
    RESULT_DUPLICATE_KEY: "#eab308",
}

# ------------------------------------------------------------------ Worker signals step names
STEP_INIT = "Inisialisasi perbandingan..."
STEP_IMPORT_LEFT = "Mengimpor data kiri..."
STEP_IMPORT_RIGHT = "Mengimpor data kanan..."
STEP_NORMALIZE = "Menormalisasi data..."
STEP_COMPARE = "Membandingkan data..."
STEP_SAVE_RESULT = "Menyimpan hasil..."
STEP_DONE = "Perbandingan selesai!"

# ------------------------------------------------------------------ Misc
SUPPORTED_FILE_EXTENSIONS = [".xlsx", ".xls", ".csv"]
DEFAULT_CSV_ENCODING = "utf-8"
DEFAULT_CSV_SEPARATOR = ","
PAGE_SIZE_OPTIONS = [50, 100, 200, 500]
DEFAULT_PAGE_SIZE = 100
