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

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CompareOptions":
        obj = cls()
        for k, v in d.items():
            if hasattr(obj, k):
                setattr(obj, k, v)
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
