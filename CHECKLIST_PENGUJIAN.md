# Checklist Pengujian — Data Compare Tool

Centang setiap item setelah diuji. Catat versi, tanggal, dan hasil di kolom keterangan.

---

## A. Uji Fungsional Dasar

### A1. Startup & Navigasi

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 1 | Aplikasi terbuka tanpa error | ☐ | |
| 2 | Sidebar tampil dengan 5 menu | ☐ | |
| 3 | Klik tiap menu → halaman berpindah | ☐ | |
| 4 | Dashboard menampilkan statistik (0 saat pertama kali) | ☐ | |

---

### A2. Buat Job Baru — File vs File

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 5 | Bisa pilih mode "File vs File" | ☐ | |
| 6 | Browse file Excel (.xlsx) kiri & kanan | ☐ | |
| 7 | Sheet dropdown terisi setelah pilih file Excel | ☐ | |
| 8 | Browse file CSV kiri & kanan | ☐ | |
| 9 | Kolom kunci & mapping ter-load dari header file | ☐ | |
| 10 | Bisa tambah/hapus baris mapping kolom | ☐ | |
| 11 | Step "Opsi Compare" bisa di-toggle tiap opsi | ☐ | |
| 12 | Step "Review" menampilkan ringkasan konfigurasi | ☐ | |
| 13 | Klik "Mulai Perbandingan" → job dimulai | ☐ | |
| 14 | Progress bar & log muncul di halaman hasil | ☐ | |
| 15 | Status job berubah jadi "Selesai" setelah proses | ☐ | |

---

### A3. Buat Job Baru — File vs PostgreSQL

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 16 | Mode "File vs PostgreSQL" bisa dipilih | ☐ | |
| 17 | Koneksi PostgreSQL bisa dipilih dari dropdown | ☐ | |
| 18 | Schema & tabel ter-load dari PostgreSQL | ☐ | |
| 19 | Proses compare selesai tanpa error | ☐ | |

---

### A4. Halaman Hasil

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 20 | Summary card Match/Mismatch/Missing/Duplicate tampil | ☐ | |
| 21 | Tabel hasil ter-load dengan data | ☐ | |
| 22 | Filter status (All/Match/Mismatch/dll) berfungsi | ☐ | |
| 23 | Paginasi bekerja (next/prev/first/last/page size) | ☐ | |
| 24 | Export ke Excel menghasilkan file valid | ☐ | |
| 25 | Export ke CSV menghasilkan file valid | ☐ | |
| 26 | File Excel hasil: ada sheet Ringkasan & Detail | ☐ | |
| 27 | Baris berwarna sesuai status (hijau/merah/kuning/dll) | ☐ | |

---

### A5. Riwayat Job

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 28 | Semua job sebelumnya muncul di daftar | ☐ | |
| 29 | Filter berdasarkan status berfungsi | ☐ | |
| 30 | Klik job → buka halaman hasil | ☐ | |
| 31 | Hapus job → hilang dari daftar | ☐ | |

---

### A6. Template

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 32 | Simpan config job sebagai template | ☐ | |
| 33 | Template muncul di halaman Template | ☐ | |
| 34 | "Gunakan Template" → form Job Baru ter-isi | ☐ | |
| 35 | Hapus template | ☐ | |

---

### A7. Pengaturan & Koneksi

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 36 | Tambah koneksi PostgreSQL baru | ☐ | |
| 37 | Test koneksi berhasil (dengan server valid) | ☐ | |
| 38 | Test koneksi gagal (dengan credential salah) → pesan error | ☐ | |
| 39 | Edit koneksi yang sudah ada | ☐ | |
| 40 | Hapus koneksi | ☐ | |
| 41 | Ubah "Rows per Page" → tersimpan setelah restart | ☐ | |

---

## B. Uji Performa

| # | Test Case | File / Baris | Durasi Maks | Hasil | Keterangan |
|---|-----------|--------------|-------------|-------|------------|
| 42 | File Excel 10.000 baris | 10K × 10 col | < 30 detik | ☐ | |
| 43 | File CSV 100.000 baris | 100K × 10 col | < 2 menit | ☐ | |
| 44 | File CSV 500.000 baris | 500K × 10 col | < 5 menit | ☐ | |
| 45 | UI tidak freeze selama proses | — | — | ☐ | |
| 46 | Memory tidak meledak (< 2 GB RAM) | 500K baris | — | ☐ | |
| 47 | Paginasi 500K baris tetap responsif | — | < 1 detik/page | ☐ | |

---

## C. Uji Akurasi Perbandingan

| # | Skenario | Hasil Ekspektasi | Hasil | Keterangan |
|---|----------|-----------------|-------|------------|
| 48 | Data identik kedua file | 100% Match | ☐ | |
| 49 | 1 baris berbeda nilai | 1 Mismatch | ☐ | |
| 50 | 1 baris hanya ada di kiri | 1 Missing Right | ☐ | |
| 51 | 1 baris hanya ada di kanan | 1 Missing Left | ☐ | |
| 52 | Kunci duplikat di kiri | Duplicate Key terdeteksi | ☐ | |
| 53 | Opsi "Abaikan huruf besar/kecil" aktif | "ABC" = "abc" → Match | ☐ | |
| 54 | Opsi "Trim spasi" aktif | " data " = "data" → Match | ☐ | |
| 55 | Opsi "Kosong = NULL" aktif | "" = NULL → Match | ☐ | |
| 56 | Nilai NULL di kedua sisi | Match (bukan Mismatch) | ☐ | |

---

## D. Uji Build Portable

| # | Test Case | Hasil | Keterangan |
|---|-----------|-------|------------|
| 57 | `build.bat` selesai tanpa error | ☐ | |
| 58 | Folder `dist\DataCompareTool\` terbentuk | ☐ | |
| 59 | File `_portable` ada di folder dist | ☐ | |
| 60 | `DataCompareTool.exe` bisa dijalankan di PC bersih (tanpa Python) | ☐ | |
| 61 | Data disimpan di subfolder `AppData\` (portable mode) | ☐ | |
| 62 | Pindahkan folder ke lokasi berbeda → masih bisa dibuka | ☐ | |

---

## E. Uji Error Handling

| # | Skenario | Ekspektasi | Hasil | Keterangan |
|---|----------|-----------|-------|------------|
| 63 | File Excel tidak valid / corrupt | Pesan error jelas, tidak crash | ☐ | |
| 64 | PostgreSQL server tidak tersedia | Pesan error jelas, tidak crash | ☐ | |
| 65 | Kolom kunci tidak ada di file | Validasi mencegah submit | ☐ | |
| 66 | Disk penuh saat export | Pesan error, tidak crash | ☐ | |
| 67 | Tutup aplikasi saat job sedang proses | Worker dibatalkan, tidak hang | ☐ | |

---

*Dokumen ini dibuat untuk versi 1.0.0 — update sesuai perubahan fitur.*
