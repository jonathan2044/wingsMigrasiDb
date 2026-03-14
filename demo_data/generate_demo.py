"""
demo_data/generate_demo.py
Script untuk membuat file demo Excel & CSV untuk pengujian aplikasi.
Jalankan: python demo_data/generate_demo.py
"""

import sys
import os
from pathlib import Path

# Tambah parent dir ke sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import pandas as pd
    import openpyxl
except ImportError:
    print("ERROR: pandas / openpyxl belum diinstall.")
    print("Jalankan: pip install pandas openpyxl")
    sys.exit(1)

import random
import string
from datetime import date, timedelta

OUTPUT_DIR = Path(__file__).parent
SEED = 42
random.seed(SEED)


def rand_str(length=6):
    return "".join(random.choices(string.ascii_uppercase, k=length))


def rand_date():
    start = date(2020, 1, 1)
    return start + timedelta(days=random.randint(0, 1000))


def make_base_data(n=500):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id_transaksi": f"TRX-{i:05d}",
            "tanggal":      rand_date().strftime("%Y-%m-%d"),
            "kode_produk":  f"PRD-{random.randint(1, 50):03d}",
            "nama_produk":  f"Produk {rand_str(4)}",
            "jumlah":       random.randint(1, 100),
            "harga_satuan": random.randint(10000, 500000),
            "total":        None,  # akan dihitung
            "kategori":     random.choice(["A", "B", "C"]),
            "status":       random.choice(["Lunas", "Pending", "Batal"]),
            "keterangan":   rand_str(8),
        })
    for r in rows:
        r["total"] = r["jumlah"] * r["harga_satuan"]
    return rows


def make_modified_data(base: list, n_mismatch=20, n_missing=15):
    """Buat versi modifikasi dari base data untuk skenario compare."""
    import copy
    data = copy.deepcopy(base)

    # Ubah beberapa baris (Mismatch)
    mismatch_indices = random.sample(range(len(data)), min(n_mismatch, len(data)))
    for idx in mismatch_indices:
        data[idx]["jumlah"] = data[idx]["jumlah"] + random.randint(1, 10)
        data[idx]["total"] = data[idx]["jumlah"] * data[idx]["harga_satuan"]

    # Hapus beberapa baris (Missing)
    missing_indices = sorted(
        random.sample(range(len(data)), min(n_missing, len(data))),
        reverse=True,
    )
    for idx in missing_indices:
        data.pop(idx)

    return data


def save_excel(data: list, filename: str, sheet_name: str = "Data"):
    path = OUTPUT_DIR / filename
    df = pd.DataFrame(data)
    df.to_excel(path, index=False, sheet_name=sheet_name)
    print(f"  [OK] {path} ({len(df)} baris)")


def save_csv(data: list, filename: str):
    path = OUTPUT_DIR / filename
    df = pd.DataFrame(data)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  [OK] {path} ({len(df)} baris)")


def main():
    print("Membuat demo data...")

    # 1. Small dataset (100 baris) - cocok untuk uji coba cepat
    base_small = make_base_data(100)
    modified_small = make_modified_data(base_small, n_mismatch=10, n_missing=5)

    save_excel(base_small,    "demo_left_small.xlsx",    "Transaksi")
    save_excel(modified_small, "demo_right_small.xlsx",  "Transaksi")
    save_csv(base_small,      "demo_left_small.csv")
    save_csv(modified_small,  "demo_right_small.csv")

    # 2. Medium dataset (5.000 baris)
    base_medium = make_base_data(5000)
    modified_medium = make_modified_data(base_medium, n_mismatch=100, n_missing=50)

    save_excel(base_medium,    "demo_left_medium.xlsx",   "Transaksi")
    save_excel(modified_medium, "demo_right_medium.xlsx", "Transaksi")
    save_csv(base_medium,      "demo_left_medium.csv")
    save_csv(modified_medium,  "demo_right_medium.csv")

    # 3. Large dataset (50.000 baris) - hanya CSV
    print("Membuat large dataset (50.000 baris, CSV only)...")
    base_large = make_base_data(50000)
    modified_large = make_modified_data(base_large, n_mismatch=500, n_missing=200)

    save_csv(base_large,     "demo_left_large.csv")
    save_csv(modified_large, "demo_right_large.csv")

    # 4. Identical dataset (untuk uji 100% Match)
    save_excel(base_small, "demo_identical_left.xlsx",  "Identik")
    save_excel(base_small, "demo_identical_right.xlsx", "Identik")

    print()
    print("Demo data berhasil dibuat di folder demo_data/")
    print()
    print("File yang tersedia:")
    print("  - demo_left_small.xlsx / demo_right_small.xlsx (100 baris)")
    print("  - demo_left_medium.xlsx / demo_right_medium.xlsx (5.000 baris)")
    print("  - demo_left_large.csv / demo_right_large.csv (50.000 baris)")
    print("  - demo_identical_left.xlsx / demo_identical_right.xlsx (100% Match)")


if __name__ == "__main__":
    main()
