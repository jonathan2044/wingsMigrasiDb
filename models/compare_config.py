# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
# All rights reserved.
"""
models/compare_config.py
Model konfigurasi perbandingan data - field mapping, key columns, dll.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class ColumnMapping:
    """Pemetaan satu kolom dari sumber kiri ke kanan."""
    left_col: str
    right_col: str
    alias: str = ""             # nama tampilan

    @property
    def display_name(self) -> str:
        return self.alias or self.left_col


@dataclass
class DataSourceConfig:
    """Konfigurasi sumber data (file atau PostgreSQL)."""
    source_type: str = ""       # 'excel' | 'csv' | 'postgres'

    # File source
    file_path: str = ""
    sheet_name: str = ""        # untuk Excel
    csv_separator: str = ","
    csv_encoding: str = "utf-8"
    has_header: bool = True
    skip_rows: int = 0

    # PostgreSQL source
    connection_id: str = ""     # ID profil koneksi (opsional jika inline diisi)
    schema_name: str = "public"
    table_name: str = ""
    custom_query: str = ""      # kalau pakai custom SQL
    use_custom_query: bool = False
    # Inline connection details — dipakai saat user mengisi form manual tanpa pilih saved profile
    pg_connection_inline: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DataSourceConfig":
        obj = cls()
        for k, v in d.items():
            if hasattr(obj, k):
                setattr(obj, k, v)
        return obj


@dataclass
class ColumnTransformRule:
    """Aturan transformasi nilai kolom sebelum perbandingan.
    Disimpan secara global di settings dan berlaku untuk setiap job
    yang mengaktifkan opsi 'apply_global_transforms'.
    """
    column_name: str                            # nama kolom yang di-match (case-insensitive)
    side: str                                   # "left" | "right" | "both"
    transform_type: str                         # "prefix" | "suffix" | "lpad" | "rpad" | "strip_chars" | "replace" | "substring"
    params: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column_name": self.column_name,
            "side": self.side,
            "transform_type": self.transform_type,
            "params": self.params,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ColumnTransformRule":
        return cls(
            column_name=d.get("column_name", ""),
            side=d.get("side", "both"),
            transform_type=d.get("transform_type", "prefix"),
            params=d.get("params", {}),
            enabled=bool(d.get("enabled", True)),
        )

    def describe_params(self) -> str:
        """Ringkasan parameter yang mudah dibaca manusia."""
        t = self.transform_type
        p = self.params
        if t == "prefix":
            return f"'{p.get('text', '')}' di depan"
        elif t == "suffix":
            return f"'{p.get('text', '')}' di belakang"
        elif t == "lpad":
            return f"pad kiri pjg={p.get('length', '')} char='{p.get('pad_char', '0')}'"
        elif t == "rpad":
            return f"pad kanan pjg={p.get('length', '')} char='{p.get('pad_char', ' ')}'"
        elif t == "strip_chars":
            return f"hapus: '{p.get('chars', '')}'"
        elif t == "replace":
            return f"'{p.get('old', '')}' → '{p.get('new', '')}'"
        elif t == "substring":
            return f"mulai={p.get('start', 1)} pjg={p.get('length', '')}"
        return str(p)


@dataclass
class GroupExpansionRule:
    """Aturan ekspansi 1-to-many untuk kolom group (row + column expansion).
    Disimpan secara global di settings dan berlaku untuk setiap job
    yang mengaktifkan opsi 'apply_group_expansion'.

    Contoh: value 'AA' di sisi kiri (cust_group) di-expand ke beberapa baris kanan,
    di mana setiap baris kanan memiliki beberapa kolom (cust_group, cust_group1, cust_group2).

    Q5: Jika left value TIDAK ada di mapping → fallback 1-to-1 + warning di log.
    Q6: Baris kanan tidak ter-cover mapping → dilaporkan MISSING_LEFT.
    Q7: Satu rule = satu kolom group kiri → N kolom kanan × M baris kanan.
    """
    left_col: str = ""                                   # nama kolom di sisi kiri
    right_cols: List[str] = field(default_factory=list)  # nama kolom-kolom di sisi kanan
    mapping: Dict[str, List[List[str]]] = field(default_factory=dict)  # {left_val: [[row1_vals...], ...]}
    enabled: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "left_col": self.left_col,
            "right_cols": self.right_cols,
            "mapping": self.mapping,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GroupExpansionRule":
        left_col = d.get("left_col", "")
        # Backward compat: format lama memakai "right_col" (string tunggal)
        if "right_cols" in d:
            right_cols = [str(c) for c in d.get("right_cols", [])]
        elif d.get("right_col"):
            right_cols = [str(d["right_col"])]
        else:
            right_cols = [left_col] if left_col else []
        raw_map = d.get("mapping", {})
        mapping: Dict[str, List[List[str]]] = {}
        for k, v in raw_map.items():
            if isinstance(v, list) and v and isinstance(v[0], list):
                # Format baru: list of lists
                mapping[str(k)] = [[str(x) for x in row] for row in v]
            elif isinstance(v, list):
                # Format lama: flat list → setiap elemen = satu baris kanan (1 kolom)
                mapping[str(k)] = [[str(x)] for x in v]
            else:
                mapping[str(k)] = [[str(v)]]
        return cls(
            left_col=left_col,
            right_cols=right_cols,
            mapping=mapping,
            enabled=bool(d.get("enabled", True)),
        )

    def total_mappings(self) -> int:
        """Total baris kanan dari semua nilai kiri."""
        return sum(len(rows) for rows in self.mapping.values())

    def describe(self) -> str:
        n_left = len(self.mapping)
        n_right_rows = self.total_mappings()
        n_cols = len(self.right_cols)
        cols_str = ", ".join(self.right_cols[:3]) + ("..." if n_cols > 3 else "")
        return f"{self.left_col} \u2192 [{cols_str}]  ({n_left} kiri, {n_right_rows} baris kanan)"


@dataclass
class CompareOptions:
    """Opsi normalisasi dan perbandingan data."""
    trim_whitespace: bool = True
    ignore_case: bool = False
    treat_empty_as_null: bool = True
    normalize_date: bool = False
    normalize_number: bool = False
    date_format: str = "%Y-%m-%d"
    decimal_places: int = 2
    apply_global_transforms: bool = True        # gunakan global column transform rules
    apply_group_expansion: bool = True            # gunakan global group expansion rules
    comparison_mode: str = "standard"             # "standard" | "column_expansion" | "row_expansion"

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CompareOptions":
        obj = cls()
        for k, v in d.items():
            if hasattr(obj, k):
                setattr(obj, k, v)
        # Backward compat: job lama tidak punya comparison_mode
        # tapi punya apply_group_expansion=True → anggap column_expansion
        if not d.get("comparison_mode") and d.get("apply_group_expansion"):
            obj.comparison_mode = "column_expansion"
        return obj


@dataclass
class CompareConfig:
    """Konfigurasi lengkap satu sesi perbandingan."""
    job_type: str = ""
    left_source: DataSourceConfig = field(default_factory=DataSourceConfig)
    right_source: DataSourceConfig = field(default_factory=DataSourceConfig)
    key_columns: List[ColumnMapping] = field(default_factory=list)
    compare_columns: List[ColumnMapping] = field(default_factory=list)
    options: CompareOptions = field(default_factory=CompareOptions)
    template_id: Optional[str] = None
    use_row_order: bool = False     # True = cocokkan berdasarkan urutan baris, bukan key column

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_type": self.job_type,
            "left_source": self.left_source.to_dict(),
            "right_source": self.right_source.to_dict(),
            "key_columns": [{"left_col": c.left_col, "right_col": c.right_col, "alias": c.alias}
                             for c in self.key_columns],
            "compare_columns": [{"left_col": c.left_col, "right_col": c.right_col, "alias": c.alias}
                                 for c in self.compare_columns],
            "options": self.options.to_dict(),
            "template_id": self.template_id,
            "use_row_order": self.use_row_order,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CompareConfig":
        obj = cls()
        obj.job_type = d.get("job_type", "")
        obj.left_source = DataSourceConfig.from_dict(d.get("left_source", {}))
        obj.right_source = DataSourceConfig.from_dict(d.get("right_source", {}))
        obj.key_columns = [
            ColumnMapping(**c) for c in d.get("key_columns", [])
        ]
        obj.compare_columns = [
            ColumnMapping(**c) for c in d.get("compare_columns", [])
        ]
        obj.options = CompareOptions.from_dict(d.get("options", {}))
        obj.template_id = d.get("template_id")
        obj.use_row_order = bool(d.get("use_row_order", False))
        return obj
