# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
core/normalization_engine.py

Normalisasi data sebelum dibandingin. Semua jalan di SQL DuckDB
biar dev gak perlu looping Python berasa mager.
"""

from __future__ import annotations
from typing import List, TYPE_CHECKING

from models.compare_config import CompareOptions

if TYPE_CHECKING:
    from models.compare_config import ColumnTransformRule


class NormalizationEngine:
    """
    Hasilkan ekspresi SQL DuckDB buat normalisasi nilai kolom.
    Semua proses di SQL, bukan Python — hemat memori, mantaaabbb.
    """

    def __init__(self, options: CompareOptions):
        self._opts = options

    def almaBuildExprKolom(self, col: str) -> str:
        """
        Bikin ekspresi SQL untuk satu kolom sesuai opsi normalisasi.
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
            # testing mode ini — kadang TRY_CAST DATE hasilnya aneh buat format non-standard
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
        Bikin SELECT clause lengkap dengan semua kolom yang sudah dinormalisasi.
        Key columns cukup di-trim aja biar bisa JOIN.
        Compare columns dinormalisasi full sesuai opsi.
        """
        parts: List[str] = []

        # key column — bersihkan aja cukup, yang penting konsisten
        for col in key_columns:
            expr = f'TRIM(CAST("{table_alias}"."{col}" AS VARCHAR))'
            alias = f'"{prefix}{col}"' if prefix else f'"{col}"'
            parts.append(f"{expr} AS {alias}")

        # compare column — normalisasi penuh
        for col in columns:
            if col in key_columns:
                continue
            expr = self._build_expr_for_table_col(table_alias, col)
            alias = f'"{prefix}{col}"' if prefix else f'"{col}"'
            parts.append(f"{expr} AS {alias}")

        return ", ".join(parts)

    def _apply_transform(self, expr: str, rule: "ColumnTransformRule") -> str:
        """Terapkan satu rule transformasi ke ekspresi SQL. Jozz kalau banyak rule."""
        t = rule.transform_type
        p = rule.params
        # untuk njajal setiap tipe transform — kalau belum ada di sini artinya belum disupport
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
        """
        Bangun ekspresi normalisasi untuk satu kolom dari tabel tertentu.

        Urutan sudah diuji, JANGAN diubah — kalau iseng dibalik, rule replace bisa gagal:
          1. CAST + TRIM raw   → bersihkan dulu
          2. Transform rules   → replace/prefix/dll. kena nilai asli
          3. NULLIF            → string kosong jadi NULL setelah transform
          4. LOWER             → ignore case setelah transform
          5. Normalisasi tanggal / angka
        """
        expr = f'CAST("{table_alias}"."{col}" AS VARCHAR)'

        # 1: trim dulu sebelum apapun
        if self._opts.trim_whitespace:
            expr = f"TRIM({expr})"

        # 2: transform rules per kolom, dikerjain sebelum normalisasi biar replace gak gagal
        if col_rules:
            for rule in col_rules:
                if rule.enabled:
                    expr = self._apply_transform(expr, rule)

        # 3: empty string jadi NULL setelah transform
        if self._opts.treat_empty_as_null:
            expr = f"NULLIF({expr}, '')"

        # 4: ignore case setelah transform supaya replace 'C161' -> 'STA1' tetap jalan
        if self._opts.ignore_case:
            expr = f"LOWER({expr})"

        # 5: normalisasi tanggal — kalau bukan tanggal, kembalikan nilai asli (jangan NULL)
        if self._opts.normalize_date:
            date_expr = (
                f"TRY(STRFTIME(TRY_CAST({expr} AS DATE), "
                f"'{self._opts.date_format}'))"
            )
            expr = f"COALESCE({date_expr}, {expr})"

        # 6: normalisasi angka — kalau bukan angka, kembalikan nilai asli juga
        if self._opts.normalize_number:
            dp = self._opts.decimal_places
            num_expr = f"TRY(PRINTF('%.{dp}f', TRY_CAST({expr} AS DOUBLE)))"
            expr = f"COALESCE({num_expr}, {expr})"

        return expr

    def normalize_literal_expr(self, raw_expr: str) -> str:
        """
        Normalisasi ekspresi SQL literal (bukan kolom tabel).
        Dipakai buat mapping _ge_expected supaya konsisten sama normalized data.
        Cuma TRIM dan LOWER — transform rules gak berlaku untuk nilai literal.
        """
        expr = raw_expr
        if self._opts.trim_whitespace:
            expr = f"TRIM({expr})"
        if self._opts.treat_empty_as_null:
            expr = f"NULLIF({expr}, '')"
        if self._opts.ignore_case:
            expr = f"LOWER({expr})"
        return expr
