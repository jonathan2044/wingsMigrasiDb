# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
core/compare_engine.py
Engine utama perbandingan data menggunakan DuckDB SQL.
Semua operasi dijalankan langsung di DuckDB untuk efisiensi
penanganan data ratusan ribu baris.
"""

from __future__ import annotations
import json
import logging
from typing import List, Dict, Any, Callable, Optional

import duckdb

from models.compare_config import CompareConfig, ColumnMapping
from core.normalization_engine import NormalizationEngine
from config.constants import (
    RESULT_MATCH, RESULT_MISMATCH,
    RESULT_MISSING_LEFT, RESULT_MISSING_RIGHT, RESULT_DUPLICATE_KEY,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # pastikan SQL log selalu tercatat

# Callback type: fn(step_name, rows_done, total_rows)
ProgressCallback = Callable[[str, int, int], None]


class CompareEngine:
    """
    Engine perbandingan data berbasis SQL DuckDB.
    
    Alur kerja:
    1. Data kiri diimport ke tabel 'src_left' di DuckDB
    2. Data kanan diimport ke tabel 'src_right'  
    3. Normalisasi diterapkan via CTE/view
    4. JOIN-based comparison untuk menemukan Match/Mismatch
    5. EXCEPT/NOT EXISTS untuk menemukan Missing Left/Right
    6. GROUP BY untuk menemukan Duplicate Key
    7. Hasil disimpan ke tabel 'compare_results'
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        config: CompareConfig,
        progress_cb: Optional[ProgressCallback] = None,
    ):
        self._conn = conn
        self._config = config
        self._progress_cb = progress_cb or (lambda *_: None)
        self._norm = NormalizationEngine(config.options)

    # ------------------------------------------------------------------ public

    def run(self) -> Dict[str, Any]:
        """
        Jalankan proses perbandingan penuh.
        Returns: dict ringkasan hasil.
        """
        logger.info("[run] Mulai. use_row_order=%s  keys=%s  compare_cols=%s",
                    self._config.use_row_order,
                    [m.left_col for m in self._config.key_columns],
                    [m.left_col for m in self._config.compare_columns])

        self._emit("Menyiapkan tabel normalisasi...", 0, 0)
        try:
            self._build_normalized_views()
        except Exception as e:
            logger.error("[run] GAGAL _build_normalized_views: %s", e)
            raise

        self._emit("Mendeteksi key duplikat...", 0, 0)
        try:
            self._find_duplicate_keys()
        except Exception as e:
            logger.error("[run] GAGAL _find_duplicate_keys: %s", e)
            raise

        self._emit("Membandingkan data...", 0, 0)
        try:
            self._compare_data()
        except Exception as e:
            logger.error("[run] GAGAL _compare_data: %s", e)
            raise

        self._emit("Menghitung ringkasan...", 0, 0)
        summary = self._compute_summary()

        logger.info("[run] Perbandingan selesai: %s", summary)
        return summary

    # ------------------------------------------------------------------ private: views

    def _build_normalized_views(self):
        """Buat CTE/tabel normalisasi dari src_left dan src_right."""
        keys = [m.left_col for m in self._config.key_columns]
        left_cols = [m.left_col for m in self._config.compare_columns]
        right_cols = [m.right_col for m in self._config.compare_columns]

        left_total  = self._conn.execute("SELECT COUNT(*) FROM src_left").fetchone()[0]
        right_total = self._conn.execute("SELECT COUNT(*) FROM src_right").fetchone()[0]
        logger.info("[views] src_left=%d baris, src_right=%d baris", left_total, right_total)
        logger.info("[views] use_row_order=%s  keys=%s  compare=%s",
                    self._config.use_row_order, keys,
                    [m.left_col for m in self._config.compare_columns])

        # ---- normalized_left
        left_parts = self._build_select_parts("src_left", keys, left_cols, "left")
        self._conn.execute("DROP VIEW IF EXISTS normalized_left")
        self._conn.execute(
            f"CREATE VIEW normalized_left AS SELECT {left_parts} FROM src_left"
        )

        # ---- normalized_right
        right_key_cols = [m.right_col for m in self._config.key_columns]
        right_parts = self._build_select_parts(
            "src_right", right_key_cols, right_cols, "right",
            key_alias_override=[m.left_col for m in self._config.key_columns],
        )
        self._conn.execute("DROP VIEW IF EXISTS normalized_right")
        self._conn.execute(
            f"CREATE VIEW normalized_right AS SELECT {right_parts} FROM src_right"
        )

    def _build_select_parts(
        self,
        table: str,
        key_cols: List[str],
        value_cols: List[str],
        prefix: str,
        key_alias_override: Optional[List[str]] = None,
    ) -> str:
        parts = []

        # Key columns: normalisasi ringan, alias ke nama bersama
        for i, col in enumerate(key_cols):
            alias = key_alias_override[i] if key_alias_override else col
            expr = f'TRIM(CAST("{table}"."{col}" AS VARCHAR))'
            parts.append(f'{expr} AS "key_{alias}"')

        # Value columns: normalisasi penuh
        for col in value_cols:
            expr = self._norm._build_expr_for_table_col(table, col)
            parts.append(f'{expr} AS "{prefix}_{col}"')

        # Tambah row number untuk referensi
        parts.append(f"ROW_NUMBER() OVER () AS {prefix}_rownum")

        return ", ".join(parts)

    # ------------------------------------------------------------------ private: compare

    def _find_duplicate_keys(self):
        """Temukan dan catat baris dengan key duplikat."""
        if self._config.use_row_order:
            return  # Mode urutan baris: tidak ada key, tidak ada duplikat
        keys = [m.left_col for m in self._config.key_columns]
        if not keys:
            return  # Tidak ada key column, skip duplicate check
        key_exprs = ", ".join(f'"key_{k}"' for k in keys)
        key_json = self._build_key_json("nl", keys)

        # Duplikat di sisi kiri
        sql = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            left_rownum,
            '{RESULT_DUPLICATE_KEY}',
            {key_json},
            '{{}}',
            '{{}}',
            '[]'
        FROM normalized_left nl
        WHERE ({key_exprs}) IN (
            SELECT {key_exprs}
            FROM normalized_left
            GROUP BY {key_exprs}
            HAVING COUNT(*) > 1
        )
        """
        self._conn.execute(sql)

        # Duplikat di sisi kanan — HANYA untuk key yang TIDAK duplikat di sisi kiri.
        # Jika key yang sama duplikat di kedua sisi, sudah dilaporkan dari kiri;
        # memasukkan dari kanan juga akan menyebabkan double-count pada total_rows.
        key_json_r = self._build_key_json("nr", keys)
        join_cond_r = " AND ".join(f'nr."key_{k}" = rod."key_{k}"' for k in keys)
        sql_r = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nr.right_rownum,
            '{RESULT_DUPLICATE_KEY}',
            {key_json_r},
            '{{}}',
            '{{}}',
            '[]'
        FROM normalized_right nr
        INNER JOIN (
            SELECT {key_exprs} FROM normalized_right
            GROUP BY {key_exprs} HAVING COUNT(*) > 1
            EXCEPT
            SELECT {key_exprs} FROM normalized_left
            GROUP BY {key_exprs} HAVING COUNT(*) > 1
        ) rod ON {join_cond_r}
        """
        self._conn.execute(sql_r)

    def _compare_data(self):
        """
        Lakukan perbandingan utama:
        - Match: key cocok, semua nilai sama
        - Mismatch: key cocok, ada nilai berbeda
        - Missing Left: key ada di kanan tapi tidak di kiri
        - Missing Right: key ada di kiri tapi tidak di kanan
        """
        if self._config.use_row_order:
            self._compare_data_row_order()
            return

        keys = [m.left_col for m in self._config.key_columns]
        if not keys:
            raise ValueError(
                "Tidak ada Key Column yang dikonfigurasi. "
                "Kembali ke Step 3 dan pilih minimal 1 kolom sebagai Key Column."
            )
        logger.info("[compare_data] keys=%s", keys)

        compare_cols = self._config.compare_columns
        key_exprs     = ", ".join(f'"key_{k}"' for k in keys)
        key_exprs_join = " AND ".join(f'nl."key_{k}" = nr."key_{k}"' for k in keys)

        # Log sample key values dari kedua sisi untuk membantu diagnosa
        sample_l = self._conn.execute(
            f'SELECT {key_exprs} FROM normalized_left LIMIT 5'
        ).fetchall()
        sample_r = self._conn.execute(
            f'SELECT {key_exprs} FROM normalized_right LIMIT 5'
        ).fetchall()
        logger.info("[compare_data] Sample key kiri (5 pertama): %s", sample_l)
        logger.info("[compare_data] Sample key kanan (5 pertama): %s", sample_r)

        # ---------- Pre-compute duplicate keys sekali, simpan di temp table ----------
        # PENTING: jangan query compare_results untuk kolom key — tabel itu tidak punya
        # kolom key individual, hanya key_values (JSON). Duplikat dideteksi langsung
        # dari normalized views.
        self._conn.execute("DROP TABLE IF EXISTS _dup_keys")
        sql_dup_tbl = f"""
        CREATE TEMP TABLE _dup_keys AS
        SELECT {key_exprs} FROM normalized_left  GROUP BY {key_exprs} HAVING COUNT(*) > 1
        UNION
        SELECT {key_exprs} FROM normalized_right GROUP BY {key_exprs} HAVING COUNT(*) > 1
        """
        logger.debug("[compare_data] membuat _dup_keys:\n%s", sql_dup_tbl)
        self._conn.execute(sql_dup_tbl)

        dup_join_nl = " AND ".join(f'nl."key_{k}" = _dk."key_{k}"' for k in keys)
        dup_join_nr = " AND ".join(f'nr."key_{k}" = _dk."key_{k}"' for k in keys)

        # ---------- Diff checks ----------
        diff_checks = []
        for cm in compare_cols:
            lc = f'"left_{cm.left_col}"'
            rc = f'"right_{cm.right_col}"'
            diff_checks.append(
                f"(({lc} IS DISTINCT FROM {rc}) AND "
                f"NOT ({lc} IS NULL AND {rc} IS NULL))"
            )
        mismatch_cond = " OR ".join(diff_checks) if diff_checks else "FALSE"

        key_json_nl    = self._build_key_json("nl", keys)
        left_data_json = self._build_row_json("nl", [f"left_{cm.left_col}"  for cm in compare_cols])
        right_data_json = self._build_row_json("nr", [f"right_{cm.right_col}" for cm in compare_cols])
        diff_cols_expr  = self._build_diff_cols_expr(compare_cols)

        # ---------- Match / Mismatch (INNER JOIN, exclude duplicates) ----------
        sql_match = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nl.left_rownum,
            CASE WHEN {mismatch_cond} THEN '{RESULT_MISMATCH}' ELSE '{RESULT_MATCH}' END,
            {key_json_nl},
            {left_data_json},
            {right_data_json},
            {diff_cols_expr}
        FROM normalized_left nl
        INNER JOIN normalized_right nr ON {key_exprs_join}
        LEFT JOIN _dup_keys _dk ON {dup_join_nl}
        WHERE _dk."key_{keys[0]}" IS NULL
        """
        logger.debug("[compare_data] Match/Mismatch SQL:\n%s", sql_match)
        try:
            self._conn.execute(sql_match)
        except Exception as e:
            logger.error("[compare_data] GAGAL Match/Mismatch: %s\nSQL:\n%s", e, sql_match)
            raise

        # ---------- Missing Right (ada di kiri, tidak di kanan) ----------
        key_join_missing = " AND ".join(f'nl."key_{k}" = nr_miss."key_{k}"' for k in keys)
        sql_missing_right = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nl.left_rownum,
            '{RESULT_MISSING_RIGHT}',
            {key_json_nl},
            {left_data_json},
            '{{}}',
            '[]'
        FROM normalized_left nl
        LEFT JOIN normalized_right nr_miss ON {key_join_missing}
        LEFT JOIN _dup_keys _dk ON {dup_join_nl}
        WHERE nr_miss."key_{keys[0]}" IS NULL
          AND _dk."key_{keys[0]}" IS NULL
        """
        logger.debug("[compare_data] Missing Right SQL:\n%s", sql_missing_right)
        try:
            self._conn.execute(sql_missing_right)
        except Exception as e:
            logger.error("[compare_data] GAGAL Missing Right: %s\nSQL:\n%s", e, sql_missing_right)
            raise

        # ---------- Missing Left (ada di kanan, tidak di kiri) ----------
        key_json_nr      = self._build_key_json("nr", keys)
        right_data_json2 = self._build_row_json("nr", [f"right_{cm.right_col}" for cm in compare_cols])
        key_join_missing_r = " AND ".join(f'nr."key_{k}" = nl_miss."key_{k}"' for k in keys)
        sql_missing_left = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nr.right_rownum,
            '{RESULT_MISSING_LEFT}',
            {key_json_nr},
            '{{}}',
            {right_data_json2},
            '[]'
        FROM normalized_right nr
        LEFT JOIN normalized_left nl_miss ON {key_join_missing_r}
        LEFT JOIN _dup_keys _dk ON {dup_join_nr}
        WHERE nl_miss."key_{keys[0]}" IS NULL
          AND _dk."key_{keys[0]}" IS NULL
        """
        logger.debug("[compare_data] Missing Left SQL:\n%s", sql_missing_left)
        try:
            self._conn.execute(sql_missing_left)
        except Exception as e:
            logger.error("[compare_data] GAGAL Missing Left: %s\nSQL:\n%s", e, sql_missing_left)
            raise

        # Log ringkasan setelah semua query
        mr_count  = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MISSING_RIGHT}'").fetchone()[0]
        ml_count  = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MISSING_LEFT}'").fetchone()[0]
        match_cnt = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MATCH}'").fetchone()[0]
        mis_cnt   = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MISMATCH}'").fetchone()[0]
        logger.info("[compare_data] Hasil: match=%d  mismatch=%d  missing_right=%d  missing_left=%d",
                    match_cnt, mis_cnt, mr_count, ml_count)

        # Log sample key yang "missing right" agar mudah didiagnosa
        if mr_count > 0:
            sample_mr = self._conn.execute(
                f"SELECT key_values FROM compare_results WHERE status='{RESULT_MISSING_RIGHT}' LIMIT 5"
            ).fetchall()
            logger.info("[compare_data] Sample key MISSING RIGHT (ada di kiri, tidak di kanan): %s", sample_mr)
        if ml_count > 0:
            sample_ml = self._conn.execute(
                f"SELECT key_values FROM compare_results WHERE status='{RESULT_MISSING_LEFT}' LIMIT 5"
            ).fetchall()
            logger.info("[compare_data] Sample key MISSING LEFT (ada di kanan, tidak di kiri): %s", sample_ml)


    def _compare_data_row_order(self):
        """
        Perbandingan berbasis urutan baris: baris ke-N kiri vs baris ke-N kanan.
        Tidak ada key column — cocokkan berdasarkan left_rownum = right_rownum.
        """
        compare_cols = self._config.compare_columns

        diff_checks = []
        for cm in compare_cols:
            lc = f'"left_{cm.left_col}"'
            rc = f'"right_{cm.right_col}"'
            diff_checks.append(
                f"(({lc} IS DISTINCT FROM {rc}) AND "
                f"NOT ({lc} IS NULL AND {rc} IS NULL))"
            )
        mismatch_cond = " OR ".join(diff_checks) if diff_checks else "FALSE"

        key_json = "json_object('row', CAST(nl.left_rownum AS VARCHAR))"
        left_data_json = self._build_row_json(
            "nl", [f"left_{cm.left_col}" for cm in compare_cols]
        )
        right_data_json = self._build_row_json(
            "nr", [f"right_{cm.right_col}" for cm in compare_cols]
        )
        diff_cols_expr = self._build_diff_cols_expr(compare_cols)

        # Match & Mismatch — baris ada di kedua sisi
        sql_ro_match = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nl.left_rownum,
            CASE WHEN {mismatch_cond} THEN '{RESULT_MISMATCH}' ELSE '{RESULT_MATCH}' END,
            {key_json},
            {left_data_json},
            {right_data_json},
            {diff_cols_expr}
        FROM normalized_left nl
        INNER JOIN normalized_right nr ON nl.left_rownum = nr.right_rownum
        """
        logger.debug("[row_order] Match/Mismatch SQL:\n%s", sql_ro_match)
        try:
            self._conn.execute(sql_ro_match)
        except Exception as e:
            logger.error("[row_order] GAGAL Match/Mismatch: %s\nSQL:\n%s", e, sql_ro_match)
            raise

        # Missing Right — baris ada di kiri tapi tidak ada pasangannya di kanan
        key_json_l = "json_object('row', CAST(nl.left_rownum AS VARCHAR))"
        sql_ro_mr = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nl.left_rownum,
            '{RESULT_MISSING_RIGHT}',
            {key_json_l},
            {left_data_json},
            '{{}}',
            '[]'
        FROM normalized_left nl
        LEFT JOIN normalized_right nr ON nl.left_rownum = nr.right_rownum
        WHERE nr.right_rownum IS NULL
        """
        logger.debug("[row_order] Missing Right SQL:\n%s", sql_ro_mr)
        try:
            self._conn.execute(sql_ro_mr)
        except Exception as e:
            logger.error("[row_order] GAGAL Missing Right: %s\nSQL:\n%s", e, sql_ro_mr)
            raise

        # Missing Left — baris ada di kanan tapi tidak ada pasangannya di kiri
        key_json_r = "json_object('row', CAST(nr.right_rownum AS VARCHAR))"
        right_data_json2 = self._build_row_json(
            "nr", [f"right_{cm.right_col}" for cm in compare_cols]
        )
        sql_ro_ml = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nr.right_rownum,
            '{RESULT_MISSING_LEFT}',
            {key_json_r},
            '{{}}',
            {right_data_json2},
            '[]'
        FROM normalized_right nr
        LEFT JOIN normalized_left nl ON nr.right_rownum = nl.left_rownum
        WHERE nl.left_rownum IS NULL
        """
        logger.debug("[row_order] Missing Left SQL:\n%s", sql_ro_ml)
        try:
            self._conn.execute(sql_ro_ml)
        except Exception as e:
            logger.error("[row_order] GAGAL Missing Left: %s\nSQL:\n%s", e, sql_ro_ml)
            raise



    def _build_key_json(self, table_alias: str, keys: List[str]) -> str:
        """Bangun ekspresi SQL untuk key sebagai JSON object."""
        pairs = []
        for k in keys:
            pairs.append(f"'{k}', \"{table_alias}\".\"key_{k}\"")
        return f"json_object({', '.join(pairs)})"

    def _build_row_json(self, table_alias: str, cols: List[str]) -> str:
        """Bangun ekspresi SQL untuk baris data sebagai JSON object."""
        if not cols:
            return "'{}'"
        pairs = []
        for c in cols:
            pairs.append(f"'{c}', \"{table_alias}\".\"{c}\"")
        return f"json_object({', '.join(pairs)})"

    def _build_diff_cols_expr(self, compare_cols: List[ColumnMapping]) -> str:
        """Bangun ekspresi SQL untuk list kolom yang berbeda sebagai JSON array."""
        if not compare_cols:
            return "'[]'"

        # Gunakan list_filter dan list
        case_parts = []
        for cm in compare_cols:
            lc = f'"left_{cm.left_col}"'
            rc = f'"right_{cm.right_col}"'
            clean_name = cm.left_col.replace("'", "\\'")
            case_parts.append(
                f"CASE WHEN {lc} IS DISTINCT FROM {rc} THEN '{clean_name}' ELSE NULL END"
            )

        # list_filter untuk hapus NULL
        list_expr = f"list_filter([{', '.join(case_parts)}], x -> x IS NOT NULL)"
        return f"to_json({list_expr})"

    # ------------------------------------------------------------------ private: summary

    def _compute_summary(self) -> Dict[str, Any]:
        rows = self._conn.execute(
            "SELECT status, COUNT(*) FROM compare_results GROUP BY status"
        ).fetchall()
        counts = {r[0]: r[1] for r in rows}
        total = sum(counts.values())
        return {
            "total_rows": total,
            RESULT_MATCH: counts.get(RESULT_MATCH, 0),
            RESULT_MISMATCH: counts.get(RESULT_MISMATCH, 0),
            RESULT_MISSING_LEFT: counts.get(RESULT_MISSING_LEFT, 0),
            RESULT_MISSING_RIGHT: counts.get(RESULT_MISSING_RIGHT, 0),
            RESULT_DUPLICATE_KEY: counts.get(RESULT_DUPLICATE_KEY, 0),
        }

    def _emit(self, step: str, done: int, total: int):
        self._progress_cb(step, done, total)
