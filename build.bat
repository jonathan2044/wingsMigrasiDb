@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul

REM ============================================================
REM  build.bat — Data Compare Tool portable Windows builder
REM
REM  Prasyarat:
REM    - Python 3.11 (64-bit) terinstall dan ada di PATH
REM    - Jalankan dari folder project (bukan subfolder)
REM
REM  Hasil  : dist\DataCompareTool\DataCompareTool.exe
REM  Data   : tersimpan di dist\DataCompareTool\AppData\  (portable)
REM ============================================================

echo.
echo =====================================================
echo   Data Compare Tool — Windows Portable Build Script
echo =====================================================
echo.

REM ── [0] Cek kita ada di folder yang benar ────────────────────────────────
if not exist "main.py" (
    echo [ERROR] Jalankan script ini dari folder root project.
    echo         Seharusnya ada file main.py di folder ini.
    pause & exit /b 1
)

REM ── [1] Cek versi Python (harus 3.11) ─────────────────────────────────────
echo [1/6] Memeriksa Python...
python --version 2>nul | findstr /R "3\.11\." >nul
if errorlevel 1 (
    echo [WARN] Python 3.11 tidak terdeteksi di PATH.
    echo        Versi Python lain mungkin bisa digunakan, tapi 3.11 direkomendasikan.
    echo        Lanjutkan? ^(Y/N^)
    set /p CONTINUE=
    if /i "!CONTINUE!" neq "Y" (exit /b 1)
)
for /f "tokens=*" %%V in ('python --version 2^>^&1') do echo        Ditemukan: %%V

REM ── [2] Setup virtual environment ─────────────────────────────────────────
echo.
echo [2/6] Memeriksa virtual environment...
if not exist "venv\Scripts\activate.bat" (
    echo        venv belum ada, membuat baru...
    python -m venv venv
    if errorlevel 1 (
        echo [ERROR] Gagal membuat venv.
        pause & exit /b 1
    )
    echo        venv berhasil dibuat.
)
call venv\Scripts\activate.bat
echo        venv aktif: %VIRTUAL_ENV%

REM ── [3] Install / update dependencies ─────────────────────────────────────
echo.
echo [3/6] Install dependencies...
pip install -q --upgrade pip
pip install -q -r requirements.txt
if errorlevel 1 (
    echo [ERROR] pip install gagal. Periksa requirements.txt dan koneksi internet.
    pause & exit /b 1
)
echo        Dependencies OK.

REM ── [4] Bersihkan output lama (tapi selamatkan AppData dulu!) ──────────────
echo.
echo [4/6] Membersihkan output build sebelumnya...

REM Backup AppData user agar data tidak hilang saat rebuild
set APPDATA_BACKUP=0
if exist "dist\DataCompareTool\AppData" (
    echo        Ditemukan AppData user, backup dulu...
    if exist "_appdata_backup" rmdir /s /q "_appdata_backup"
    xcopy /e /i /q /y "dist\DataCompareTool\AppData" "_appdata_backup" >nul
    if errorlevel 1 (
        echo [WARN] Backup AppData gagal, lanjutkan rebuild tanpa backup.
    ) else (
        set APPDATA_BACKUP=1
        echo        AppData berhasil di-backup ke _appdata_backup\
    )
)

if exist "dist\DataCompareTool" (
    rmdir /s /q "dist\DataCompareTool"
    echo        dist\DataCompareTool\ dihapus.
)
if exist "build" (
    rmdir /s /q "build"
    echo        build\ dihapus.
)

REM ── [5] Jalankan PyInstaller ──────────────────────────────────────────────
echo.
echo [5/6] Menjalankan PyInstaller...
echo        Proses ini membutuhkan waktu 3-10 menit, harap tunggu...
echo.
pyinstaller build.spec --noconfirm
if errorlevel 1 (
    echo.
    echo [ERROR] PyInstaller gagal! Periksa log di atas.
    echo         Coba tips berikut:
    echo           - pip install --upgrade pyinstaller
    echo           - Periksa ModuleNotFoundError dan tambahkan ke hiddenimports di build.spec
    pause & exit /b 1
)

REM ── [6] Post-build — siapkan struktur portable ────────────────────────────
echo.
echo [6/6] Menyiapkan struktur portable...

REM Pastikan _portable marker ada di dalam dist (settings.py mendeteksinya)
copy /y "_portable" "dist\DataCompareTool\_portable" >nul

REM Restore AppData user dari backup (jika ada), atau buat folder kosong
if "%APPDATA_BACKUP%"=="1" (
    echo        Merestore AppData user dari backup...
    xcopy /e /i /q /y "_appdata_backup" "dist\DataCompareTool\AppData" >nul
    if errorlevel 1 (
        echo [WARN] Restore AppData gagal. Data mungkin tidak terestore.
    ) else (
        echo        AppData berhasil direstore — data user aman!
        rmdir /s /q "_appdata_backup"
    )
) else (
    REM Belum ada AppData sebelumnya, buat folder kosong
    if not exist "dist\DataCompareTool\AppData" (
        mkdir "dist\DataCompareTool\AppData"
    )
)

REM Salin demo_data bila ada
if exist "demo_data" (
    xcopy /e /i /q /y "demo_data" "dist\DataCompareTool\demo_data" >nul
    echo        demo_data disalin.
)

REM Buat file README singkat di dalam dist untuk user
(
    echo Data Compare Tool v1.0.0
    echo ========================
    echo.
    echo Jalankan: DataCompareTool.exe
    echo.
    echo Data Anda disimpan di folder AppData\ di samping file .exe ini.
    echo Untuk memindahkan, copy SELURUH folder ini ke lokasi baru.
    echo.
    echo Jangan hapus file _portable — file ini yang membuat app berjalan portable.
) > "dist\DataCompareTool\CARA_PAKAI.txt"

REM ── Selesai ───────────────────────────────────────────────────────────────
echo.
echo =====================================================
echo   BUILD BERHASIL!
echo.
echo   Output   : dist\DataCompareTool\
echo   Exe      : dist\DataCompareTool\DataCompareTool.exe
echo   Ukuran   :
dir /s /-c "dist\DataCompareTool" 2>nul | find "File(s)"
echo.
echo   Cara distribusi:
echo     Copy seluruh folder dist\DataCompareTool\ ke USB / laptop klien.
echo     User cukup double-click DataCompareTool.exe
echo =====================================================
echo.

REM Tanya apakah mau buat ZIP untuk distribusi
set /p MAKEZIP=Buat file ZIP untuk distribusi? (Y/N): 
if /i "!MAKEZIP!"=="Y" (
    echo Membuat ZIP...
    powershell -Command "Compress-Archive -Path 'dist\DataCompareTool' -DestinationPath 'dist\DataCompareTool_portable.zip' -Force"
    if errorlevel 1 (
        echo [WARN] Gagal buat ZIP. Folder dist\DataCompareTool\ tetap bisa digunakan.
    ) else (
        echo ZIP selesai: dist\DataCompareTool_portable.zip
    )
)

echo.
pause
