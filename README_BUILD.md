# Data Compare Tool — Panduan Build & Distribusi Windows

## Prasyarat Build

| Kebutuhan | Versi | Catatan |
|-----------|-------|---------|
| **Python** | **3.11 (64-bit)** | Versi lain mungkin bisa, tapi 3.11 paling stabil dengan PySide6 + DuckDB |
| Windows | 10 / 11 (64-bit) | Machine tempat build harus Windows juga |
| RAM | Minimal 4 GB | PyInstaller butuh ~2 GB saat build |
| Disk | Minimal 2 GB | Untuk proses build dan output |
| Koneksi internet | Satu kali saja | Untuk `pip install -r requirements.txt` |

> **Catatan penting:** Build harus dilakukan di mesin **Windows**. File `.app` macOS tidak bisa dibuild dari Windows dan sebaliknya. PyInstaller tidak cross-compile.

---

## Cara Build (Paling Cepat)

### Satu Langkah — Cukup Double-Click

```
build.bat
```

Script ini otomatis:
1. Cek Python 3.11
2. Buat virtual environment `venv\` bila belum ada
3. Install semua dependency dari `requirements.txt`
4. Hapus output lama
5. Jalankan PyInstaller dengan `build.spec`
6. Siapkan struktur portable (copy `_portable` marker, buat `AppData\`)
7. Tanya apakah mau buat file ZIP untuk distribusi

---

## Manual Build (Opsional)

Jika ingin kontrol penuh:

```cmd
REM 1. Buka Command Prompt, masuk ke folder project
cd C:\Projects\DataCompareTool

REM 2. Buat dan aktifkan venv
python -m venv venv
venv\Scripts\activate

REM 3. Install dependencies
pip install -r requirements.txt

REM 4. Build
pyinstaller build.spec --noconfirm

REM 5. Salin portable marker ke output
copy _portable dist\DataCompareTool\_portable
mkdir dist\DataCompareTool\AppData
```

---

## Struktur Output

Setelah build berhasil, folder `dist\DataCompareTool\` berisi:

```
dist\DataCompareTool\
│
├── DataCompareTool.exe   ← Double-click untuk jalankan
├── _portable             ← WAJIB ADA — menandakan mode portable
├── CARA_PAKAI.txt        ← Instruksi singkat untuk user
│
├── AppData\              ← Data user disimpan di sini (portable)
│   ├── app_data.db       ← Dibuat otomatis saat pertama jalan
│   ├── jobs\             ← Hasil perbandingan per-job (DuckDB)
│   ├── exports\          ← File export Excel/CSV
│   └── logs\             ← Log aplikasi
│
├── demo_data\            ← File Excel/CSV contoh
│
├── Qt6Core.dll           ┐
├── Qt6Gui.dll            │ Library PySide6 / Qt
├── Qt6Widgets.dll        │
├── platforms\            │ (qwindows.dll — Windows platform plugin)
├── shiboken6\            ┘
│
├── _duckdb.pyd           ← Native extension DuckDB
├── python311.dll         ← Python runtime (sudah di-bundle)
└── ... (puluhan .dll lainnya)
```

**Ukuran folder:** ~300–500 MB (normal untuk PySide6 + DuckDB)

---

## Cara Distribusi ke Klien

1. Copy **seluruh folder** `dist\DataCompareTool\` ke USB / Google Drive / share folder
2. Tidak perlu install apapun di laptop klien
3. User tinggal double-click `DataCompareTool.exe`
4. Data tersimpan di subfolder `AppData\` di samping .exe — ikut berpindah saat folder di-copy

> **JANGAN** hanya copy file `.exe`-nya saja. Harus satu folder utuh.
>
> **JANGAN** hapus file `_portable` — tanpanya, app akan menyimpan data di `%APPDATA%\DataCompareTool\` dan data tidak portable.

---

## Smoke Test Setelah Build

Lakukan pengujian ini sebelum dikirim ke klien:

### ✅ Test 1: Startup
- [ ] Double-click `DataCompareTool.exe`
- [ ] Aplikasi terbuka dalam 5–15 detik (pertama kali lebih lama)
- [ ] Tidak ada dialog error / crash
- [ ] Dashboard tampil dengan benar

### ✅ Test 2: Portable Mode
- [ ] Setelah aplikasi pernah dijalankan, cek apakah folder `AppData\` sudah terbentuk di samping .exe
- [ ] `AppData\app_data.db` harus ada
- [ ] `AppData\logs\app.log` harus ada, isinya log startup tanpa ERROR

### ✅ Test 3: Buat Job Baru
- [ ] Klik "New Job" atau "Buat Perbandingan Baru"
- [ ] Upload dua file Excel/CSV dari `demo_data\`
- [ ] Pilih key column dan compare column
- [ ] Klik Run → progress bar berjalan
- [ ] Hasil muncul (Summary + Detail)

### ✅ Test 4: Kelola Data
- [ ] Result Summary tampil: 5 kartu status, distribution bar, breakdown
- [ ] Filter Mismatch di Result Detail berfungsi
- [ ] Export ke Excel/CSV tidak error

### ✅ Test 5: Portabilitas
- [ ] Copy seluruh folder ke lokasi lain (misal: `C:\Temp\TestPortable\`)
- [ ] Jalankan dari lokasi baru
- [ ] Job history sebelumnya masih ada di `AppData\`

### ✅ Test 6: Di Mesin Bersih
Idealnya test di VM atau laptop tanpa Python terinstall:
- [ ] Tidak ada error "Python not found"
- [ ] Aplikasi terbuka normal
- [ ] Semua fitur berfungsi sama

---

## Troubleshooting Build

### `ModuleNotFoundError: No module named 'xxx'` saat runtime
Tambahkan modul ke `hiddenimports` di `build.spec`, lalu build ulang:
```python
hiddenimports=[
    ...
    'nama.modul.yang.kurang',
],
```

### DuckDB error saat startup setelah build
```
duckdb.IOException: Could not open file
```
Biasanya karena ada instance lain yang membuka file yang sama. Pastikan tidak ada instance aplikasi yang masih berjalan.

### Aplikasi tidak muncul (blank), tidak ada error
Cek `AppData\logs\app.log` untuk detail error. Pastikan folder `AppData\` bisa ditulis (tidak di folder yang butuh admin privilege).

### Antivirus memblokir `.exe`
False positive umum untuk executable PyInstaller. Tambahkan folder `dist\DataCompareTool\` ke whitelist antivirus sebelum distribusi.

### `qwindows.dll` tidak ditemukan
PySide6 platform plugin tidak ikut terbawa. Solusi:
```python
# Di build.spec, tambahkan di datas:
from PyInstaller.utils.hooks import collect_data_files
pyside6_datas = collect_data_files('PySide6')
# Lalu tambahkan pyside6_datas ke datas=
```
`build.spec` ini sudah melakukan hal tersebut secara otomatis.

### UPX error saat build
UPX opsional. Jika error, nonaktifkan dengan `upx=False` di `build.spec`.

### Build sangat lambat
Normal. PyInstaller 6.x + PySide6 + DuckDB bisa 5–15 menit di build pertama. Build kedua lebih cepat karena ada cache di `build\`.

---

## Konfigurasi Ikon (Opsional)

1. Siapkan file icon `assets\icon.ico` (format ICO, ukuran 256×256)
2. Edit `build.spec`, uncomment baris:
   ```python
   # icon='assets\\icon.ico',
   ```
3. Jalankan ulang `build.bat`

---

## Menjalankan Mode Development (tanpa build)

```cmd
REM Aktifkan venv dulu
venv\Scripts\activate

REM Jalankan langsung
python main.py
```

> **macOS khusus:** Gunakan `pythonw` agar window muncul dengan benar:
> ```bash
> /Users/akazaya/opt/anaconda3/bin/pythonw main.py
> ```
> `python3` biasa di macOS adalah command-line process — macOS tidak menampilkan window-nya ke depan secara otomatis.

---

## Struktur Source Code

```
DataCompareTool/
├── main.py                     ← Entry point
├── requirements.txt            ← Daftar dependency
├── build.spec                  ← PyInstaller config (6.x)
├── build.bat                   ← Script build otomatis
├── _portable                   ← Marker portable mode
│
├── config/
│   ├── settings.py             ← Path management, portable mode detection
│   └── constants.py            ← Status, warna, label
│
├── models/                     ← Data classes (Job, Template, Config, dsb)
├── storage/                    ← DuckDB wrapper, CRUD managers
├── services/                   ← File reader (Excel/CSV streaming)
├── core/                       ← SQL compare engine, normalization
├── workers/                    ← QThread background worker
├── exporters/                  ← Export ke Excel / CSV
├── ui/                         ← PySide6 GUI (pages, components)
│
├── assets/                     ← Icon, gambar (kosong = ok)
└── demo_data/                  ← File contoh untuk testing
```
