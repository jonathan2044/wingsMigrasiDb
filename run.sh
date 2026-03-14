#!/bin/bash
# run.sh — Jalankan aplikasi Data Compare Tool di macOS
# Gunakan pythonw agar window tampil dengan benar
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHONW="$(dirname "$(which python3)")/pythonw"

if [ ! -f "$PYTHONW" ]; then
    # fallback: cari manual di Anaconda
    PYTHONW="/Users/akazaya/opt/anaconda3/bin/pythonw"
fi

if [ ! -f "$PYTHONW" ]; then
    echo "pythonw tidak ditemukan, coba python3 biasa..."
    PYTHONW="python3"
fi

echo "Menggunakan: $PYTHONW"
cd "$SCRIPT_DIR"
exec "$PYTHONW" main.py "$@"
