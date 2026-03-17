# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
core/compare_engine.py

Engine utama komparasi data — ngandalin DuckDB SQL biar gak mager load ke Python.
Semua operasi SQL dijalankan langsung di DuckDB, jadi meski datanya gede
tetep jozz performanya. antiGalau aja deh.
"""

from __future__ import annotations
import json
import logging
from typing import List, Dict, Any, Callable, Optional

import duckdb

from models.compare_config import CompareConfig, ColumnMapping, ColumnTransformRule, GroupExpansionRule
from core.normalization_engine import NormalizationEngine
from config.constants import (
    RESULT_MATCH, RESULT_MISMATCH,
    RESULT_MISSING_LEFT, RESULT_MISSING_RIGHT, RESULT_DUPLICATE_KEY,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # log debug aktif — jangan matiin, nanti susah trace

# tipe callback progress: fn(nama_step, baris_selesai, total_baris)
ProgressCallback = Callable[[str, int, int], None]


class CompareEngine:
    """
    Engine komparasi data, semua jalan di DuckDB biar mantaaabbb.

    Alur kerja (jangan diubah urutannya ya, nanti error):
    1. src_left  ← data dari sumber kiri
    2. src_right ← data dari sumber kanan
    3. Normalisasi via CTE/view (trim, lower, dll)
    4. JOIN buat cari Match/Mismatch
    5. NOT EXISTS buat cari Missing
    6. GROUP BY buat tangkap key duplikat
    7. Hasil masuk ke tabel 'compare_results'
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        config: CompareConfig,
        progress_cb: Optional[ProgressCallback] = None,
        transform_rules: Optional[List[ColumnTransformRule]] = None,
        group_expansion_rules: Optional[List[GroupExpansionRule]] = None,
    ):
        self._conn = conn
        self._config = config
        self._progress_cb = progress_cb or (lambda *_: None)
        self._norm = NormalizationEngine(config.options)
        self._transform_rules: List[ColumnTransformRule] = transform_rules or []
        self._group_expansion_rules: List[GroupExpansionRule] = group_expansion_rules or []

    # == entrypoint utama ==

    def almaRun(self) -> Dict[str, Any]:
        """
        Jalankan proses komparasi dari awal sampai selesai.
        Return dict ringkasan — total, match, mismatch, dll.
        """
        logger.info("[almaRun] Mulai. use_row_order=%s  keys=%s  compare_cols=%s",
                    self._config.use_row_order,
                    [m.left_col for m in self._config.key_columns],
                    [m.left_col for m in self._config.compare_columns])

        self._emit("Menyiapkan tabel normalisasi...", 0, 0)
        try:
            self._djumboBuiltNormalView()
        except Exception as e:
            logger.error("[almaRun] GAGAL _djumboBuiltNormalView: %s", e)
            raise

        # cek GE rule aktif untuk job ini
        # kalau ada rule aktif, skip duplicate check biasa
        active_exp_rule = None  # testing, defaultnya None dulu
        if not self._config.use_row_order:
            active_exp_rule = self._getAktifExpansionRule()

        self._emit("Mendeteksi key duplikat...", 0, 0)
        try:
            if active_exp_rule:
                # mode group expansion — kanan sengaja banyak baris per key, skip dup check biasa
                keys_dup = [m.left_col for m in self._config.key_columns]
                key_exprs_dup = ", ".join(f'"key_{k}"' for k in keys_dup) if keys_dup else '"_dummy"'
                self._conn.execute("DROP TABLE IF EXISTS _dup_keys")
                self._conn.execute(
                    f"CREATE TEMP TABLE _dup_keys AS "
                    f"SELECT {key_exprs_dup} FROM normalized_left WHERE 1=0"
                )
                logger.info("[almaRun] Mode group expansion aktif: %s \u2192 %s",
                            active_exp_rule.left_col, active_exp_rule.right_cols)
            else:
                self._djumboBikinDupKeys()
                self._almaTemukanDuplikat()
        except Exception as e:
            logger.error("[almaRun] GAGAL detect duplicates: %s", e)
            raise

        self._emit("Membandingkan data...", 0, 0)
        try:
            if active_exp_rule:
                self._almaCompareGrupExpand(active_exp_rule)
            else:
                self._almaCompareData()
        except Exception as e:
            logger.error("[almaRun] GAGAL compare_data: %s", e)
            raise

        self._emit("Menghitung ringkasan...", 0, 0)
        summary = self._almaHitungRingkasan()

        logger.info("[almaRun] Perbandingan selesai: %s", summary)
        return summary

    # --- views normalisasi ---

    def _djumboBuiltNormalView(self):
        """Bikin view normalisasi dari src_left dan src_right — wajib dipanggil duluan."""
        keys = [m.left_col for m in self._config.key_columns]
        left_cols = [m.left_col for m in self._config.compare_columns]
        right_cols = [m.right_col for m in self._config.compare_columns]

        # pastikan kolom GE ikut masuk view walau gak ada di compare_columns
        _exp = self._getAktifExpansionRule()
        if _exp:
            if _exp.left_col not in left_cols and _exp.left_col not in keys:
                left_cols = list(left_cols) + [_exp.left_col]
                logger.info("[views] kolom group expansion '%s' ditambahkan ke normalized_left", _exp.left_col)
            for _rc in _exp.right_cols:
                if _rc not in right_cols:
                    right_cols = list(right_cols) + [_rc]
                    logger.info("[views] kolom group expansion '%s' ditambahkan ke normalized_right", _rc)

        left_total  = self._conn.execute("SELECT COUNT(*) FROM src_left").fetchone()[0]
        right_total = self._conn.execute("SELECT COUNT(*) FROM src_right").fetchone()[0]
        logger.info("[djumboBuiltNormalView] kiri=%d baris, kanan=%d baris", left_total, right_total)
        logger.info("[djumboBuiltNormalView] use_row_order=%s  keys=%s  compare=%s",
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

    def _getRulesUntukKolom(self, col_name: str, side: str) -> List[ColumnTransformRule]:
        """Ambil transform rules untuk kolom tertentu berdasarkan sisi (left/right/both)."""
        return [
            r for r in self._transform_rules
            if r.enabled
            and r.column_name.lower() == col_name.lower()
            and r.side in (side, "both")
        ]

    def _build_select_parts(
        self,
        table: str,
        key_cols: List[str],
        value_cols: List[str],
        prefix: str,
        key_alias_override: Optional[List[str]] = None,
    ) -> str:
        parts = []

        # kolom kunci: hanya di-trim, alias seragam biar bisa di-JOIN
        for i, col in enumerate(key_cols):
            alias = key_alias_override[i] if key_alias_override else col
            expr = f'TRIM(CAST("{table}"."{col}" AS VARCHAR))'
            parts.append(f'{expr} AS "key_{alias}"')

        # kolom value: normalisasi full + transform rules kalau ada
        for col in value_cols:
            col_rules = self._getRulesUntukKolom(col, prefix)  # prefix = "left" or "right"
            expr = self._norm._build_expr_for_table_col(table, col, col_rules)
            parts.append(f'{expr} AS "{prefix}_{col}"')

        # Tambah row number untuk referensi
        parts.append(f"ROW_NUMBER() OVER () AS {prefix}_rownum")

        return ", ".join(parts)

    # --- proses komparasi ---

    def _djumboBikinDupKeys(self):
        """
        Bikin TEMP TABLE _dup_keys — isinya semua key yang AMBIGU:
        artinya key yang nongol >1x di kiri ATAU >1x di kanan.
        Dipake bareng sama _almaTemukanDuplikat() dan _almaCompareData()
        biar gak hitung dua kali dan hasilnya konsisten. jozz.
        """
        if self._config.use_row_order:
            return
        keys = [m.left_col for m in self._config.key_columns]
        if not keys:
            return
        key_exprs = ", ".join(f'"key_{k}"' for k in keys)
        self._conn.execute("DROP TABLE IF EXISTS _dup_keys")
        sql = f"""
        CREATE TEMP TABLE _dup_keys AS
        SELECT {key_exprs} FROM normalized_left  GROUP BY {key_exprs} HAVING COUNT(*) > 1
        UNION
        SELECT {key_exprs} FROM normalized_right GROUP BY {key_exprs} HAVING COUNT(*) > 1
        """
        self._conn.execute(sql)
        n_dup = self._conn.execute("SELECT COUNT(*) FROM _dup_keys").fetchone()[0]
        # untuk njajal — kalau n_dup gede banget, kemungkinan data masih kotor
        logger.info("[djumboBikinDupKeys] key ambigu ditemukan: %d kombinasi", n_dup)

    def _almaTemukanDuplikat(self):
        """
        Tandai SEMUA baris yang key-nya ambigu (nongol >1x di salah satu sisi).
        Baris ini di-skip dari perbandingan normal — masuk kategori duplicate_key.
        Kedua sisi dilaporkan biar gak ada baris yang 'ilang diam-diam'.
        """
        if self._config.use_row_order:
            return
        keys = [m.left_col for m in self._config.key_columns]
        if not keys:
            return

        compare_cols    = self._config.compare_columns
        key_json_nl     = self._build_key_json("nl", keys)
        key_json_nr     = self._build_key_json("nr", keys)
        left_data_json  = self._build_row_json("nl", [f"left_{cm.left_col}"  for cm in compare_cols])
        right_data_json = self._build_row_json("nr", [f"right_{cm.right_col}" for cm in compare_cols])
        dup_join_nl     = " AND ".join(f'nl."key_{k}" = _dk."key_{k}"' for k in keys)
        dup_join_nr     = " AND ".join(f'nr."key_{k}" = _dk."key_{k}"' for k in keys)

        # Semua baris KIRI yang keynya ambigus
        sql_left = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nl.left_rownum,
            '{RESULT_DUPLICATE_KEY}',
            {key_json_nl},
            {left_data_json},
            '{{}}',
            '[]'
        FROM normalized_left nl
        INNER JOIN _dup_keys _dk ON {dup_join_nl}
        """
        try:
            self._conn.execute(sql_left)
        except Exception as e:
            logger.error("[dup_keys] GAGAL insert kiri: %s", e)
            raise

        # Semua baris KANAN yang keynya ambigus
        sql_right = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nr.right_rownum,
            '{RESULT_DUPLICATE_KEY}',
            {key_json_nr},
            '{{}}',
            {right_data_json},
            '[]'
        FROM normalized_right nr
        INNER JOIN _dup_keys _dk ON {dup_join_nr}
        """
        try:
            self._conn.execute(sql_right)
        except Exception as e:
            logger.error("[dup_keys] GAGAL insert kanan: %s", e)
            raise

        dup_left  = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_DUPLICATE_KEY}'").fetchone()[0]
        logger.info("[almaTemukanDuplikat] total baris duplikat dilaporkan: %d", dup_left)

    def _almaCompareData(self):
        """
        Komparasi utama — inti dari semua ini:
        - Match       : key sama, nilai sama semua
        - Mismatch    : key sama tapi ada nilai beda
        - Missing Left : ada di kanan, gak ada di kiri
        - Missing Right: ada di kiri, gak ada di kanan
        """
        if self._config.use_row_order:
            self._almaCompareRowOrder()
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

        # _dup_keys sudah dibuat di _djumboBikinDupKeys() sebelum method ini dipanggil
        dup_join_nl = " AND ".join(f'nl."key_{k}" = _dk."key_{k}"' for k in keys)
        dup_join_nr = " AND ".join(f'nr."key_{k}" = _dk."key_{k}"' for k in keys)

        # ---------- Diff checks ----------
        # testing logic ini — jangan salah nulis IS DISTINCT FROM
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
        # kalau hasilnya 0 semua cek dulu normalized view-nya, pasti ada yang salah
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

        # log sample key 'missing right' buat bantu diagnosa — berguna pas debugging
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


    def _almaCompareRowOrder(self):
        """
        Mode urutan baris: baris ke-N kiri lawan baris ke-N kanan — no key needed.
        Cocokkan berdasarkan left_rownum = right_rownum. Simpel tapi kadang butuh.
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

        # missing right — ada di kiri, gak punya pasangan di kanan
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

        # missing left — ada di kanan, gak punya pasangan di kiri
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



    # --- group expansion ---

    def _getAktifExpansionRule(self) -> Optional[GroupExpansionRule]:
        """Cari GE rule pertama yang aktif dan relevan dengan kolom yang dikonfigurasi."""
        if not self._group_expansion_rules:
            return None
        compare_left   = {cm.left_col.lower()  for cm in self._config.compare_columns}
        key_cols_lower = {m.left_col.lower()   for m in  self._config.key_columns}
        for rule in self._group_expansion_rules:
            if not rule.enabled or not rule.mapping or not rule.right_cols:
                continue
            lc = rule.left_col.lower()
            if lc in key_cols_lower:
                logger.warning(
                    "[group_exp] Kolom '%s' adalah key column — tidak bisa jadi expansion column. Rule dilewati.",
                    rule.left_col,
                )
                continue
            # Rule match: left_col ada di compare_columns sisi kiri
            if lc in compare_left:
                return rule
        return None

    def _almaCompareGrupExpand(self, rule: GroupExpansionRule):
        """
        Mode 1-to-many group expansion — satu baris kiri bisa cocok ke banyak baris kanan.

        Logika any-match (simpelnya gini):
        - Left ada di mapping + minimal 1 right cocok  → MATCH
        - Left ada di mapping + gak ada right cocok    → MISSING_RIGHT
        - Left tidak di mapping                        → fallback 1-to-1 (ada warning di log)
        - Right tidak ter-cover sama sekali            → MISSING_LEFT
        """
        if self._config.use_row_order:
            logger.warning("[almaCompareGrupExpand] mode row-order gak support GE, fallback normal.")
            self._almaCompareData()
            return

        keys = [m.left_col for m in self._config.key_columns]
        if not keys:
            raise ValueError("Group expansion membutuhkan minimal 1 Key Column.")

        compare_cols = self._config.compare_columns
        left_gcol = f"left_{rule.left_col}"
        n_rc = len(rule.right_cols)

        if n_rc == 0:
            logger.error("[almaCompareGrupExpand] right_cols kosong — fallback ke 1-to-1.")
            self._almaCompareData()
            return

        # --- Buat temp table _ge_expected dengan skema dinamis ---
        self._conn.execute("DROP TABLE IF EXISTS _ge_expected")
        col_defs = ", ".join(f'"rc_{i}" VARCHAR' for i in range(n_rc))
        self._conn.execute(f'CREATE TEMP TABLE _ge_expected (left_val VARCHAR, {col_defs})')

        rows_ins = []
        for lv, rows_list in rule.mapping.items():
            for row_vals in rows_list:
                padded = list(row_vals)[:n_rc]
                padded += [''] * max(0, n_rc - len(padded))
                rows_ins.append([str(lv)] + [str(v) for v in padded])
        if rows_ins:
            ph = ", ".join(["?"] * (n_rc + 1))
            self._conn.executemany(f"INSERT INTO _ge_expected VALUES ({ph})", rows_ins)
        n_map = self._conn.execute("SELECT COUNT(*) FROM _ge_expected").fetchone()[0]
        logger.info("[group_exp] Expected rows dimuat: %d  (left_col=%s, right_cols=%s)",
                    n_map, rule.left_col, rule.right_cols)

        # --- Cek nilai kiri tidak ada di mapping (Q5) ---
        # BUGFIX: ge.left_val adalah nilai raw dari CSV mapping, sedangkan nl.left_gcol
        # sudah dinormalisasi (TRIM/LOWER). Normalisasi ge.left_val agar konsisten.
        _norm_left_val = self._norm.normalize_literal_expr('ge."left_val"')
        try:
            unmapped_rows = self._conn.execute(
                f'SELECT DISTINCT nl."{left_gcol}" FROM normalized_left nl '
                f'WHERE NOT EXISTS (SELECT 1 FROM _ge_expected ge WHERE {_norm_left_val} = nl."{left_gcol}")'
            ).fetchall()
        except Exception as e:
            logger.warning("[group_exp] Cek unmapped gagal: %s", e)
            unmapped_rows = []

        has_unmapped = len(unmapped_rows) > 0
        if has_unmapped:
            vals_list = [str(r[0]) for r in unmapped_rows[:20]]
            logger.warning(
                "[group_exp] %d nilai kolom '%s' di kiri tidak ada di mapping → fallback 1-to-1. "
                "Nilai (maks 20): %s",
                len(unmapped_rows), rule.left_col, vals_list,
            )

        # --- SQL helper expressions ---
        key_join_nl_nr  = " AND ".join(f'nl."key_{k}" = nr."key_{k}"'        for k in keys)
        key_join_nl2_nr = " AND ".join(f'nl2."key_{k}" = nr."key_{k}"'       for k in keys)
        key_join_nl3_nr = " AND ".join(f'nl3."key_{k}" = nr."key_{k}"'       for k in keys)
        key_join_nl_nri = " AND ".join(f'nl."key_{k}" = nr_inner."key_{k}"'  for k in keys)

        # JOIN condition: right row's columns must match expected values.
        # BUGFIX: normalize ge.rc_i literals the same way as normalized_right columns
        # (apply TRIM/LOWER etc.) so that ignore_case / trim_whitespace don't break matching.
        def _norm_ge(alias_col: str) -> str:
            return self._norm.normalize_literal_expr(alias_col)

        def _ge_match_parts(alias: str, nr_alias: str) -> list:
            """Bangun list kondisi JOIN untuk GE — tanpa nested f-string agar
            kompatibel dengan Python 3.11 (backslash tidak boleh di dalam {}).
            """
            parts = []
            for i in range(n_rc):
                rc_col   = rule.right_cols[i]
                rc_ref   = alias + '."rc_' + str(i) + '"'
                nr_col   = nr_alias + '."right_' + rc_col + '"'
                norm_ref = _norm_ge(rc_ref)
                parts.append(nr_col + " = " + norm_ref)
            return parts

        ge_right_match    = " AND ".join(_ge_match_parts('ge',     'nr'))
        ge_right_match2   = " AND ".join(_ge_match_parts('ge2',    'nr'))
        ge_right_match_ml = " AND ".join(_ge_match_parts('ge_ml',  'nr'))
        ge_right_match_in = " AND ".join(_ge_match_parts('ge_in',  'nr_inner'))

        key_json_nl = self._build_key_json("nl", keys)
        key_json_nr = self._build_key_json("nr", keys)

        # Kolom group expansion dikecualikan dari diff check (perbedaannya by-design)
        ge_left_set  = {rule.left_col.lower()}
        ge_right_set = {rc.lower() for rc in rule.right_cols}
        compare_cols_diff = [
            cm for cm in compare_cols
            if cm.left_col.lower() not in ge_left_set
            and cm.right_col.lower() not in ge_right_set
        ]

        # left_data_json: kolom compare dari sisi kiri
        left_data_json = self._build_row_json("nl", [f"left_{cm.left_col}" for cm in compare_cols])

        # right_data_json: kolom compare sisi kanan PLUS semua GE right_cols yang tidak ada
        # di compare_cols — agar hasil detail page menampilkan semua kolom kanan GE.
        _cmp_right_set = {cm.right_col for cm in compare_cols}
        _ge_extra_right_cols = [rc for rc in rule.right_cols if rc not in _cmp_right_set]
        _all_right_cols_for_json = (
            [f"right_{cm.right_col}" for cm in compare_cols]
            + [f"right_{rc}" for rc in _ge_extra_right_cols]
        )
        right_data_json  = self._build_row_json("nr", _all_right_cols_for_json)
        right_data_json2 = self._build_row_json("nr", _all_right_cols_for_json)
        diff_cols_expr   = self._build_diff_cols_expr(compare_cols_diff)

        diff_checks = []
        for cm in compare_cols_diff:
            lc_q = f'"left_{cm.left_col}"'
            rc_q = f'"right_{cm.right_col}"'
            diff_checks.append(
                f"(({lc_q} IS DISTINCT FROM {rc_q}) AND NOT ({lc_q} IS NULL AND {rc_q} IS NULL))"
            )
        mismatch_cond = " OR ".join(diff_checks) if diff_checks else "FALSE"

        # ── Materialisasi intermediate tables untuk performa hash-join ──
        # Menghindari correlated EXISTS yang dievaluasi ulang per baris.

        # BUGFIX: normalize left_val untuk semua join/lookup agar konsisten dengan
        # normalized_left yang sudah di-TRIM/LOWER.
        _norm_left_val_gev  = self._norm.normalize_literal_expr('gev."left_val"')
        _norm_left_val_ge   = self._norm.normalize_literal_expr('ge."left_val"')
        _norm_left_val_ge3  = self._norm.normalize_literal_expr('ge3."left_val"')

        try:
            # _ge_mapped_left: left rows yang group-val-nya ADA di mapping
            self._conn.execute("DROP TABLE IF EXISTS _ge_mapped_left")
            self._conn.execute(f"""
                CREATE TEMP TABLE _ge_mapped_left AS
                SELECT DISTINCT nl.left_rownum
                FROM normalized_left nl
                INNER JOIN (SELECT DISTINCT left_val FROM _ge_expected) gev
                    ON {_norm_left_val_gev} = nl."{left_gcol}"
            """)
        except Exception as e:
            logger.error("[group_exp] GAGAL membuat _ge_mapped_left: %s", e)
            raise

        try:
            # _ge_matched_left: left rows yang cocok dengan minimal 1 right row via ekspansi.
            # Juga simpan right_rownum dari salah satu right row yang cocok (baris pertama per left)
            # agar kita bisa mengambil right_data untuk ditampilkan di hasil detail.
            self._conn.execute("DROP TABLE IF EXISTS _ge_matched_left")
            self._conn.execute(f"""
                CREATE TEMP TABLE _ge_matched_left AS
                SELECT nl.left_rownum,
                       MIN(nr.right_rownum) AS matched_right_rownum
                FROM normalized_left nl
                INNER JOIN _ge_expected ge ON {_norm_left_val_ge} = nl."{left_gcol}"
                INNER JOIN normalized_right nr ON {key_join_nl_nr} AND {ge_right_match}
                GROUP BY nl.left_rownum
            """)
        except Exception as e:
            logger.error("[group_exp] GAGAL membuat _ge_matched_left: %s", e)
            raise

        _norm_left_val_ge2 = self._norm.normalize_literal_expr('ge2."left_val"')
        try:
            # _ge_covered_right: right rows yang di-claim oleh minimal 1 left row via ekspansi
            self._conn.execute("DROP TABLE IF EXISTS _ge_covered_right")
            self._conn.execute(f"""
                CREATE TEMP TABLE _ge_covered_right AS
                SELECT DISTINCT nr.right_rownum
                FROM normalized_right nr
                INNER JOIN normalized_left nl2 ON {key_join_nl2_nr}
                INNER JOIN _ge_expected ge2
                    ON {_norm_left_val_ge2} = nl2."{left_gcol}" AND {ge_right_match2}
            """)
        except Exception as e:
            logger.error("[group_exp] GAGAL membuat _ge_covered_right: %s", e)
            raise

        # ── PART 1a: mapped left rows + minimal 1 kombinasi ditemukan di kanan → MATCH ──
        # Sertakan right_data dari right row yang cocok pertama kali ditemukan,
        # sehingga detail result page dapat menampilkan nilai kolom kanan.
        sql_match = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nl.left_rownum,
            '{RESULT_MATCH}',
            {key_json_nl},
            {left_data_json},
            {right_data_json},
            '[]'
        FROM normalized_left nl
        INNER JOIN _ge_matched_left gml ON gml.left_rownum = nl.left_rownum
        INNER JOIN normalized_right nr   ON nr.right_rownum = gml.matched_right_rownum
        """
        logger.debug("[group_exp] Any-Match SQL:\n%s", sql_match)
        try:
            self._conn.execute(sql_match)
        except Exception as e:
            logger.error("[group_exp] GAGAL Any-Match: %s\nSQL: %s", e, sql_match)
            raise

        # ── PART 1b: mapped left rows + TIDAK ADA kombinasi cocok di kanan → MISSING_RIGHT ──
        sql_mr = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nl.left_rownum,
            '{RESULT_MISSING_RIGHT}',
            {key_json_nl},
            {left_data_json},
            '{{}}',
            '[]'
        FROM normalized_left nl
        INNER JOIN _ge_mapped_left gm ON gm.left_rownum = nl.left_rownum
        LEFT  JOIN _ge_matched_left gml ON gml.left_rownum = nl.left_rownum
        WHERE gml.left_rownum IS NULL
        """
        logger.debug("[group_exp] Missing Right (no match) SQL:\n%s", sql_mr)
        try:
            self._conn.execute(sql_mr)
        except Exception as e:
            logger.error("[group_exp] GAGAL Missing Right (mapped): %s", e)
            raise

        # ── PART 2: unmapped left rows → fallback 1-to-1 ──
        if has_unmapped:
            # 2a: Match/Mismatch (unmapped left × right yg juga tidak ter-cover ekspansi)
            sql_ump_match = f"""
            INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
            SELECT
                nl.left_rownum,
                CASE WHEN {mismatch_cond} THEN '{RESULT_MISMATCH}' ELSE '{RESULT_MATCH}' END,
                {key_json_nl},
                {left_data_json},
                {right_data_json},
                {diff_cols_expr}
            FROM normalized_left nl
            INNER JOIN normalized_right nr ON {key_join_nl_nr}
            LEFT  JOIN _ge_mapped_left gm ON gm.left_rownum = nl.left_rownum
            WHERE gm.left_rownum IS NULL
              AND NOT EXISTS (SELECT 1 FROM _ge_expected ge2 WHERE {ge_right_match2})
            """
            try:
                self._conn.execute(sql_ump_match)
            except Exception as e:
                logger.error("[group_exp] GAGAL Unmapped Match: %s", e)
                raise

            # 2b: Missing Right (unmapped left, tidak ada right unclaimed untuk key ini)
            sql_ump_mr = f"""
            INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
            SELECT
                nl.left_rownum,
                '{RESULT_MISSING_RIGHT}',
                {key_json_nl},
                {left_data_json},
                '{{}}',
                '[]'
            FROM normalized_left nl
            LEFT  JOIN _ge_mapped_left gm ON gm.left_rownum = nl.left_rownum
            WHERE gm.left_rownum IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM normalized_right nr_inner
                WHERE {key_join_nl_nri}
                  AND NOT EXISTS (SELECT 1 FROM _ge_expected ge_in WHERE {ge_right_match_in})
              )
            """
            try:
                self._conn.execute(sql_ump_mr)
            except Exception as e:
                logger.error("[group_exp] GAGAL Unmapped Missing Right: %s", e)
                raise

        # ── PART 3: MISSING_LEFT — right rows tidak ter-cover ekspansi maupun fallback ──
        sql_ml = f"""
        INSERT INTO compare_results (row_id, status, key_values, left_data, right_data, diff_columns)
        SELECT
            nr.right_rownum,
            '{RESULT_MISSING_LEFT}',
            {key_json_nr},
            '{{}}',
            {right_data_json2},
            '[]'
        FROM normalized_right nr
        LEFT  JOIN _ge_covered_right gcr ON gcr.right_rownum = nr.right_rownum
        WHERE gcr.right_rownum IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM normalized_left nl3
            WHERE {key_join_nl3_nr}
              AND NOT EXISTS (SELECT 1 FROM _ge_expected ge3 WHERE {_norm_left_val_ge3} = nl3."{left_gcol}")
              AND NOT EXISTS (SELECT 1 FROM _ge_expected ge_ml WHERE {ge_right_match_ml})
        )
        """
        logger.debug("[group_exp] Missing Left SQL:\n%s", sql_ml)
        try:
            self._conn.execute(sql_ml)
        except Exception as e:
            logger.error("[group_exp] GAGAL Missing Left: %s", e)
            raise

        # Log ringkasan hasil group expansion
        match_cnt = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MATCH}'").fetchone()[0]
        mm_cnt    = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MISMATCH}'").fetchone()[0]
        mr_cnt    = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MISSING_RIGHT}'").fetchone()[0]
        ml_cnt    = self._conn.execute(f"SELECT COUNT(*) FROM compare_results WHERE status='{RESULT_MISSING_LEFT}'").fetchone()[0]
        logger.info(
            "[group_exp] Hasil: match=%d  mismatch=%d  missing_right=%d  missing_left=%d",
            match_cnt, mm_cnt, mr_cnt, ml_cnt,
        )

    # --- helper SQL builder ---

    def _build_key_json(self, table_alias: str, keys: List[str]) -> str:
        """Bangun ekspresi SQL key sebagai JSON object untuk kolom hasil."""
        pairs = []
        for k in keys:
            pairs.append(f"'{k}', \"{table_alias}\".\"key_{k}\"")
        return f"json_object({', '.join(pairs)})"

    def _build_row_json(self, table_alias: str, cols: List[str]) -> str:
        """Bangun ekspresi SQL baris data jadi JSON object."""
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
            clean_name = cm.left_col.replace("'", "''")
            case_parts.append(
                f"CASE WHEN {lc} IS DISTINCT FROM {rc} THEN '{clean_name}' ELSE NULL END"
            )

        # list_filter untuk hapus NULL
        list_expr = f"list_filter([{', '.join(case_parts)}], x -> x IS NOT NULL)"
        return f"to_json({list_expr})"

    # --- ringkasan akhir ---

    def _almaHitungRingkasan(self) -> Dict[str, Any]:
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
        # kirim update progress ke UI
        self._progress_cb(step, done, total)
