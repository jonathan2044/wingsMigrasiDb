# Copyright (c) 2026 Jonathan Narendra - PT Naraya Prisma Digital
# Website : https://narayadigital.co.id
"""
services/file_reader.py
Baca file Excel dan CSV secara efisien.
Support preview header, list sheet, dan import chunk-based ke DuckDB.
"""

from __future__ import annotations
import csv
import logging
from pathlib import Path
from typing import List, Optional, Tuple, Generator

import pandas as pd
import duckdb

logger = logging.getLogger(__name__)


class FileReaderError(Exception):
    """Error saat membaca file sumber data."""
    pass


class ExcelReader:
    """Pembaca file Excel (.xlsx / .xls)."""

    def __init__(self, file_path: str):
        self._path = Path(file_path)
        if not self._path.exists():
            raise FileReaderError(f"File tidak ditemukan: {file_path}")

    def list_sheets(self) -> List[str]:
        """Daftar nama sheet yang tersedia di file Excel."""
        try:
            xf = pd.ExcelFile(self._path, engine="openpyxl")
            return xf.sheet_names
        except Exception as e:
            raise FileReaderError(f"Gagal membaca sheet: {e}") from e

    def get_headers(
        self,
        sheet_name: str = 0,
        skip_rows: int = 0,
    ) -> List[str]:
        """Ambil nama kolom (header) dari sheet tertentu.
        Nama kolom dikembalikan dalam bentuk ter-sanitasi agar konsisten
        dengan nama kolom yang tersimpan di tabel DuckDB.
        """
        try:
            df = pd.read_excel(
                self._path,
                sheet_name=sheet_name,
                skiprows=skip_rows,
                nrows=0,
                engine="openpyxl",
            )
            return [_sanitize_col(c) for c in df.columns]
        except Exception as e:
            raise FileReaderError(f"Gagal membaca header: {e}") from e

    def estimate_row_count(self, sheet_name=0, skip_rows: int = 0) -> int:
        """Estimasi jumlah baris data di sheet (baca 1 kolom supaya cepat)."""
        try:
            df = pd.read_excel(
                self._path,
                sheet_name=sheet_name,
                skiprows=skip_rows,
                usecols=[0],
                engine="openpyxl",
                dtype=str,
            )
            return len(df)
        except Exception:
            return 0

    def preview(
        self,
        sheet_name: str = 0,
        skip_rows: int = 0,
        n_rows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Preview data. n_rows=None berarti baca semua baris."""
        try:
            kwargs = dict(
                sheet_name=sheet_name,
                skiprows=skip_rows,
                engine="openpyxl",
                dtype=str,
            )
            if n_rows is not None:
                kwargs["nrows"] = n_rows
            return pd.read_excel(self._path, **kwargs)
        except Exception as e:
            raise FileReaderError(f"Gagal preview data: {e}") from e

    def read_chunks(
        self,
        sheet_name: str = 0,
        skip_rows: int = 0,
        chunk_size: int = 10_000,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Baca file Excel dalam chunks untuk file besar.
        Generator yang menghasilkan DataFrame per batch.
        """
        try:
            # pandas tidak punya chunksize untuk Excel, baca semua lalu chunk manual
            df = pd.read_excel(
                self._path,
                sheet_name=sheet_name,
                skiprows=skip_rows,
                engine="openpyxl",
                dtype=str,
            )
            total = len(df)
            logger.info("Total baris Excel: %d, chunk: %d", total, chunk_size)
            for start in range(0, total, chunk_size):
                yield df.iloc[start : start + chunk_size].copy()
        except Exception as e:
            raise FileReaderError(f"Gagal membaca file Excel: {e}") from e

    def import_to_duckdb(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        sheet_name: str = 0,
        skip_rows: int = 0,
        chunk_size: int = 10_000,
        progress_callback=None,
    ) -> int:
        """
        Import seluruh isi sheet ke tabel DuckDB.
        Returns jumlah baris yang berhasil diimport.
        """
        total_rows = 0
        first_chunk = True
        # testing loop chunk Excel — ini yang lambat kalau file gede, pertimbangkan openpyxl langsung
        for chunk in self.read_chunks(sheet_name, skip_rows, chunk_size):
            if chunk.empty:
                continue
            # Sanitasi nama kolom
            chunk.columns = [_sanitize_col(c) for c in chunk.columns]

            if first_chunk:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM chunk WHERE 1=0"
                )
                first_chunk = False

            conn.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
            total_rows += len(chunk)

            if progress_callback:
                progress_callback(total_rows)

        if first_chunk:
            # File kosong atau hanya header — buat tabel kosong agar engine tidak crash
            headers = self.get_headers(sheet_name, skip_rows)
            cols_def = ", ".join(f'"{h}" VARCHAR' for h in headers) if headers else '"_dummy" VARCHAR'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} ({cols_def})")
            logger.warning("File Excel tidak memiliki data: %s", self._path)

        logger.info("Impor Excel selesai: %d baris ke %s", total_rows, table_name)
        return total_rows


class CSVReader:
    """Pembaca file CSV dengan dukungan berbagai encoding dan separator."""

    def __init__(self, file_path: str, separator: str = ",", encoding: str = "utf-8"):
        self._path = Path(file_path)
        self._sep = separator
        self._encoding = encoding
        if not self._path.exists():
            raise FileReaderError(f"File tidak ditemukan: {file_path}")

    def get_headers(self) -> List[str]:
        """Ambil nama kolom dari baris pertama.
        Nama kolom dikembalikan dalam bentuk ter-sanitasi agar konsisten
        dengan nama kolom yang tersimpan di tabel DuckDB.
        """
        try:
            df = pd.read_csv(
                self._path,
                sep=self._sep,
                encoding=self._encoding,
                nrows=0,
                dtype=str,
            )
            return [_sanitize_col(c) for c in df.columns]
        except Exception as e:
            raise FileReaderError(f"Gagal membaca header CSV: {e}") from e

    def estimate_row_count(self) -> int:
        """Estimasi jumlah baris data dengan hitung newline (minus header)."""
        try:
            count = 0
            with open(self._path, "r", encoding=self._encoding, errors="replace") as f:
                for _ in f:
                    count += 1
            return max(0, count - 1)
        except Exception:
            return 0

    def preview(self, n_rows: Optional[int] = None) -> pd.DataFrame:
        """Preview data. n_rows=None berarti baca semua baris."""
        try:
            kwargs = dict(
                sep=self._sep,
                encoding=self._encoding,
                dtype=str,
            )
            if n_rows is not None:
                kwargs["nrows"] = n_rows
            return pd.read_csv(self._path, **kwargs)
        except Exception as e:
            raise FileReaderError(f"Gagal preview CSV: {e}") from e

    def read_chunks(
        self, chunk_size: int = 10_000
    ) -> Generator[pd.DataFrame, None, None]:
        """Generator baca CSV dalam chunks."""
        try:
            reader = pd.read_csv(
                self._path,
                sep=self._sep,
                encoding=self._encoding,
                dtype=str,
                chunksize=chunk_size,
            )
            for chunk in reader:
                yield chunk
        except Exception as e:
            raise FileReaderError(f"Gagal membaca CSV: {e}") from e

    def detect_separator(self) -> str:
        """Deteksi otomatis separator CSV."""
        try:
            with open(self._path, "r", encoding=self._encoding, errors="replace") as f:
                sample = f.read(4096)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=",;\t|")
            return dialect.delimiter
        except Exception:
            return ","

    def import_to_duckdb(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        chunk_size: int = 10_000,
        progress_callback=None,
    ) -> int:
        """Import CSV ke tabel DuckDB.  Returns jumlah baris.

        Path cepat: DuckDB native read_csv (jauh lebih cepat dari pandas chunkloop).
        Jika gagal karena encoding/format, jatuh-balik ke pandas chunk loop.
        """
        try:
            return self._import_to_duckdb_native(conn, table_name, progress_callback)
        except Exception as e:
            logger.warning(
                "DuckDB native CSV reader gagal (%s), beralih ke pandas chunked import.", e
            )
            return self._import_to_duckdb_pandas(conn, table_name, chunk_size, progress_callback)

    def _import_to_duckdb_native(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        progress_callback=None,
    ) -> int:
        """Import CSV via DuckDB built-in reader — 10-50× lebih cepat dari pandas."""
        import re as _re

        safe_path = str(self._path.resolve()).replace("'", "''")

        # Tentukan encoding yang didukung DuckDB
        enc = self._encoding.lower().replace("-", "")
        if enc in ("utf8", "utf"):
            duckdb_enc = "utf-8"
        elif enc in ("latin1", "iso88591", "cp1252"):
            duckdb_enc = "latin-1"
        else:
            duckdb_enc = self._encoding  # serahkan ke DuckDB, jatuh-balik jika tidak didukung

        sep = self._sep
        # DuckDB tidak mendukung sep multi-karakter; pastikan 1 karakter
        if len(sep) != 1:
            raise ValueError(f"Separator harus 1 karakter, dapat: {sep!r}")

        # njajal path cepat native — kalau malah error langsung fallback ke pandas
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(
            f"CREATE TABLE {table_name} AS "
            f"SELECT * FROM read_csv("
            f"  '{safe_path}', "
            f"  sep='{sep.replace(chr(39), chr(39)+chr(39))}', "
            f"  encoding='{duckdb_enc}', "
            f"  all_varchar=true, "
            f"  header=true, "
            f"  auto_detect=false"
            f")"
        )

        # Sanitasi nama kolom (rename kolom yang membutuhkan sanitasi)
        col_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
        raw_names = [row[0] for row in col_info]
        for raw in raw_names:
            san = _sanitize_col(raw)
            if san != raw:
                safe_raw = raw.replace('"', '""')
                safe_san = san.replace('"', '""')
                conn.execute(f'ALTER TABLE {table_name} RENAME COLUMN "{safe_raw}" TO "{safe_san}"')

        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if progress_callback:
            progress_callback(total_rows)

        logger.info("Impor CSV (native) selesai: %d baris ke %s", total_rows, table_name)
        return total_rows

    def _import_to_duckdb_pandas(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        chunk_size: int = 10_000,
        progress_callback=None,
    ) -> int:
        """Import CSV via pandas chunksize loop (fallback path)."""
        total_rows = 0
        first_chunk = True

        for chunk in self.read_chunks(chunk_size):
            if chunk.empty:
                continue
            chunk.columns = [_sanitize_col(c) for c in chunk.columns]

            if first_chunk:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM chunk WHERE 1=0"
                )
                first_chunk = False

            conn.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")
            total_rows += len(chunk)

            if progress_callback:
                progress_callback(total_rows)

        if first_chunk:
            # File kosong atau hanya header — buat tabel kosong agar engine tidak crash
            headers = self.get_headers()
            cols_def = ", ".join(f'"{h}" VARCHAR' for h in headers) if headers else '"_dummy" VARCHAR'
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} ({cols_def})")
            logger.warning("File CSV tidak memiliki data: %s", self._path)

        logger.info("Impor CSV (pandas) selesai: %d baris ke %s", total_rows, table_name)
        return total_rows


# ------------------------------------------------------------------ utilities

def create_reader(
    file_path: str,
    separator: str = ",",
    encoding: str = "utf-8",
) -> "ExcelReader | CSVReader":
    """Factory: buat reader yang sesuai berdasarkan ekstensi file."""
    ext = Path(file_path).suffix.lower()
    if ext in (".xlsx", ".xls"):
        return ExcelReader(file_path)
    elif ext == ".csv":
        return CSVReader(file_path, separator, encoding)
    else:
        raise FileReaderError(f"Ekstensi file tidak didukung: {ext}")


def _sanitize_col(name: str) -> str:
    """Bersihkan nama kolom agar aman dipakai sebagai nama kolom SQL."""
    import re
    name = str(name).strip()
    name = re.sub(r"[^\w]", "_", name)
    if name and name[0].isdigit():
        name = "col_" + name
    return name or "unnamed"
