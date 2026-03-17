# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
core/normalization_engine.py
Engine normalisasi data sebelum proses perbandingan.
Menghasilkan ekspresi SQL yang diaplikasikan langsung di DuckDB
sehingga tidak perlu load ke Python memory.
"""

from __future__ import annotations
from typing import List, TYPE_CHECKING

from models.compare_config import CompareOptions

if TYPE_CHECKING:
    from models.compare_config import ColumnTransformRule


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
            expr = self._build_expr_for_table_col(table_alias, col)
            alias = f'"{prefix}{col}"' if prefix else f'"{col}"'
            parts.append(f"{expr} AS {alias}")

        return ", ".join(parts)

    def _apply_transform(self, expr: str, rule: "ColumnTransformRule") -> str:
        """Terapkan satu aturan transformasi ke ekspresi SQL yang sudah ada."""
        t = rule.transform_type
        p = rule.params

        def _esc(s: str) -> str:
            """Escape single-quote untuk SQL string literal."""
            return str(s).replace("'", "''")

        if t == "prefix":
            text = _esc(p.get("text", ""))
            return f"CONCAT('{text}', {expr})"
        elif t == "suffix":
            text = _esc(p.get("text", ""))
            return f"CONCAT({expr}, '{text}')"
        elif t == "lpad":
            length = max(1, int(p.get("length", 10)))
            pad_char = _esc(p.get("pad_char", "0"))[:1] or "0"
            return f"LPAD({expr}, {length}, '{pad_char}')"
        elif t == "rpad":
            length = max(1, int(p.get("length", 10)))
            pad_char = _esc(p.get("pad_char", " "))[:1] or " "
            return f"RPAD({expr}, {length}, '{pad_char}')"
        elif t == "strip_chars":
            chars = p.get("chars", "")
            for ch in chars:
                expr = f"REPLACE({expr}, '{_esc(ch)}', '')"
            return expr
        elif t == "replace":
            old = _esc(p.get("old", ""))
            new = _esc(p.get("new", ""))
            if old:
                return f"REPLACE({expr}, '{old}', '{new}')"
            return expr
        elif t == "substring":
            start = max(1, int(p.get("start", 1)))
            length = max(1, int(p.get("length", 10)))
            return f"SUBSTR({expr}, {start}, {length})"
        return expr

    def _build_expr_for_table_col(
        self,
        table_alias: str,
        col: str,
        col_rules: "List[ColumnTransformRule] | None" = None,
    ) -> str:
        """Bangun ekspresi normalisasi untuk kolom di tabel tertentu.

        Urutan yang benar:
          1. CAST + TRIM raw  → nilai bersih sebelum transform
          2. Transform rules  → replace/prefix/dll. bekerja pada nilai asli
          3. NULLIF           → ubah string kosong jadi NULL setelah transform
          4. LOWER            → ignore_case setelah transform (agar 'C161'→'STA1'
                                tidak gagal akibat case mismatch saat replace)
          5. Date / Number normalization

        Dengan urutan ini, rule replace {'old':'C161','new':'STA1'} tetap bekerja
        meski ignore_case=True karena replace dilakukan sebelum LOWER().
        """
        expr = f'CAST("{table_alias}"."{col}" AS VARCHAR)'

        # Step 1: trim raw value terlebih dulu agar transform bekerja pada nilai bersih
        if self._opts.trim_whitespace:
            expr = f"TRIM({expr})"

        # Step 2: terapkan per-kolom transform rules SEBELUM normalisasi lanjutan
        if col_rules:
            for rule in col_rules:
                if rule.enabled:
                    expr = self._apply_transform(expr, rule)

        # Step 3: treat empty as null (setelah transform, agar prefix '' tetap menjadi NULL)
        if self._opts.treat_empty_as_null:
            expr = f"NULLIF({expr}, '')"

        # Step 4: ignore case (setelah transform agar replace 'C161' → 'STA1' tidak gagal)
        if self._opts.ignore_case:
            expr = f"LOWER({expr})"

        # Step 5: date normalization
        if self._opts.normalize_date:
            # Jika nilai BUKAN tanggal, JANGAN dijadikan NULL — kembalikan nilai aslinya.
            date_expr = (
                f"TRY(STRFTIME(TRY_CAST({expr} AS DATE), "
                f"'{self._opts.date_format}'))"
            )
            expr = f"COALESCE({date_expr}, {expr})"

        # Step 6: number normalization
        if self._opts.normalize_number:
            dp = self._opts.decimal_places
            # Jika nilai bukan angka, kembalikan nilai aslinya (jangan NULL)
            num_expr = f"TRY(PRINTF('%.{dp}f', TRY_CAST({expr} AS DOUBLE)))"
            expr = f"COALESCE({num_expr}, {expr})"

        return expr

    def normalize_literal_expr(self, raw_expr: str) -> str:
        """Terapkan normalisasi dasar pada ekspresi SQL literal (bukan kolom tabel).
        Digunakan untuk menormalisasi nilai literal di _ge_expected agar konsisten
        dengan nilai di normalized_right/normalized_left.

        Hanya TRIM dan LOWER diterapkan — transform rules tidak berlaku untuk literal.
        """
        expr = raw_expr
        if self._opts.trim_whitespace:
            expr = f"TRIM({expr})"
        if self._opts.treat_empty_as_null:
            expr = f"NULLIF({expr}, '')"
        if self._opts.ignore_case:
            expr = f"LOWER({expr})"
        return expr
