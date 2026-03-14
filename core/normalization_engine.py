"""
core/normalization_engine.py
Engine normalisasi data sebelum proses perbandingan.
Menghasilkan ekspresi SQL yang diaplikasikan langsung di DuckDB
sehingga tidak perlu load ke Python memory.
"""

from __future__ import annotations
from typing import List

from models.compare_config import CompareOptions


class NormalizationEngine:
    """
    Hasilkan ekspresi SQL DuckDB untuk normalisasi nilai kolom.
    Normalisasi dijalankan saat membuat view/CTE di DuckDB,
    bukan di Python, agar efisien untuk data besar.
    """

    def __init__(self, options: CompareOptions):
        self._opts = options

    def build_col_expr(self, col: str) -> str:
        """
        Hasilkan ekspresi SQL untuk satu kolom dengan normalisasi yang dipilih.
        Contoh output: TRIM(LOWER(NULLIF(col, '')))
        """
        expr = f'CAST("{col}" AS VARCHAR)'

        if self._opts.treat_empty_as_null:
            expr = f"NULLIF({expr}, '')"

        if self._opts.trim_whitespace:
            expr = f"TRIM({expr})"

        if self._opts.ignore_case:
            expr = f"LOWER({expr})"

        if self._opts.normalize_date:
            # Coba parse sebagai tanggal lalu format ulang.
            # Jika nilai BUKAN tanggal, JANGAN dijadikan NULL — kembalikan nilai aslinya.
            date_expr = (
                f"TRY(STRFTIME(TRY_CAST({expr} AS DATE), "
                f"'{self._opts.date_format}'))"
            )
            expr = f"COALESCE({date_expr}, {expr})"

        if self._opts.normalize_number:
            dp = self._opts.decimal_places
            # Jika nilai bukan angka, kembalikan nilai aslinya (jangan NULL)
            num_expr = f"TRY(PRINTF('%.{dp}f', TRY_CAST({expr} AS DOUBLE)))"
            expr = f"COALESCE({num_expr}, {expr})"

        return expr

    def build_normalized_select(
        self,
        table_alias: str,
        columns: List[str],
        key_columns: List[str],
        prefix: str = "",
    ) -> str:
        """
        Hasilkan SELECT clause dengan semua kolom ternormalisasi.
        Key columns tidak dinormalisasi agar bisa di-JOIN.
        Compare columns dinormalisasi sesuai opsi.
        """
        parts: List[str] = []

        # Key columns - bersihkan saja tapi tetap konsisten
        for col in key_columns:
            expr = f'TRIM(CAST("{table_alias}"."{col}" AS VARCHAR))'
            alias = f'"{prefix}{col}"' if prefix else f'"{col}"'
            parts.append(f"{expr} AS {alias}")

        # Compare columns - normalisasi penuh
        for col in columns:
            if col in key_columns:
                continue
            expr = self.build_col_expr(f'{table_alias}"."{col}')
            # Fix: rebuild properly
            expr = self._build_expr_for_table_col(table_alias, col)
            alias = f'"{prefix}{col}"' if prefix else f'"{col}"'
            parts.append(f"{expr} AS {alias}")

        return ", ".join(parts)

    def _build_expr_for_table_col(self, table_alias: str, col: str) -> str:
        """Bangun ekspresi normalisasi untuk kolom di tabel tertentu."""
        expr = f'CAST("{table_alias}"."{col}" AS VARCHAR)'

        if self._opts.treat_empty_as_null:
            expr = f"NULLIF({expr}, '')"

        if self._opts.trim_whitespace:
            expr = f"TRIM({expr})"

        if self._opts.ignore_case:
            expr = f"LOWER({expr})"

        if self._opts.normalize_date:
            # Jika nilai BUKAN tanggal, JANGAN dijadikan NULL — kembalikan nilai aslinya.
            date_expr = (
                f"TRY(STRFTIME(TRY_CAST({expr} AS DATE), "
                f"'{self._opts.date_format}'))"
            )
            expr = f"COALESCE({date_expr}, {expr})"

        if self._opts.normalize_number:
            dp = self._opts.decimal_places
            # Jika nilai bukan angka, kembalikan nilai aslinya (jangan NULL)
            num_expr = f"TRY(PRINTF('%.{dp}f', TRY_CAST({expr} AS DOUBLE)))"
            expr = f"COALESCE({num_expr}, {expr})"

        return expr
