# Peta Migrasi ke Azure

Dokumen ini memetakan stack lokal ke layanan Azure. Kode pipeline sudah memakai
environment variable, sehingga yang berubah terutama endpoint, kredensial, dan
lapisan penyimpanan.

## Pemetaan komponen

| Lokal | Azure | Catatan |
|---|---|---|
| Garage S3 | ADLS Gen2 | Ganti `S3_ENDPOINT_URL`, bucket, dan kredensial dengan akun storage serta managed identity |
| Prefect worker | Azure Container Apps Job atau Data Factory | Prefect tetap bisa dipakai, tetapi ADF lebih lazim untuk jadwal dan dependensi |
| Spark `local[2]` | Synapse Spark pool atau Databricks | Naikkan core dan shuffle partition |
| Delta gold lokal | Delta Lake di ADLS | Gold sudah Delta lewat `delta-rs`; di Azure gunakan Spark Delta untuk `MERGE` dan `OPTIMIZE` |
| Delta silver lokal | Delta Lake di ADLS | Silver sudah Delta lewat `delta-rs`; gold membacanya melalui `delta-rs` |
| MLflow lokal | Azure ML managed MLflow atau MLflow di AKS | `MLFLOW_TRACKING_URI` diarahkan ke server terkelola |
| Dashboard Streamlit | Container Apps, lalu React + API | API membaca gold, frontend hanya JSON |
| `.env` | Key Vault dan managed identity | Jangan menyimpan kredensial di image atau repo |

## Variabel yang menjadi saklar

| Variabel | Lokal | Azure |
|---|---|---|
| `S3_ENDPOINT_URL` | `http://garage:3900` | endpoint ADLS atau kosong bila memakai identitas terkelola |
| `S3_BUCKET_BRONZE/SILVER/GOLD` | `bronze`, `silver`, `gold` | container atau filesystem ADLS |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | URI Azure ML atau server MLflow terkelola |
| `SPARK_MASTER` | `local[2]` | dikelola Synapse atau Databricks |
| `TRAIN_INCLUDE_PARTIAL` | `true` hanya untuk smoke | `false` untuk produksi |

## Perubahan yang disarankan saat migrasi

1. **Delta Lake untuk gold dan silver sudah diterapkan di lokal dengan
   `delta-rs`.** Di Azure, ganti writer ke Spark Delta di atas ADLS agar
   `MERGE`, `replaceWhere`, dan `OPTIMIZE` berjalan di cluster. Ini juga
   memberi versi tabel untuk lineage MLflow.
2. **Partisi bulanan untuk silver.** Layout harian menghasilkan puluhan ribu
   file kecil dan membuat backfill lambat. Partisi bulanan memotong jumlah file
   sekitar 30 kali.
3. **`cache()` DataFrame bersih** sebelum validasi, DQ, dan tulis.
4. **Naikkan paralelisme.** `local[2]` hanya memakai dua core dari 16 yang
   tersedia di mesin lokal.
5. **DQ per bulan atau per run**, bukan per hari, untuk mengurangi file laporan.
6. **Gunakan `OPTIMIZE` dan `ZORDER`** pada tabel Delta setelah backfill.

## Bukti dari lokal

Backfill silver 2015-2026 dihentikan setelah 2020 karena lambat. Penyebabnya
bukan volume data (sekitar 237 ribu baris per tahun), melainkan ribuan file
kecil, penulisan ulang partisi dinamis di S3A, dan pekerjaan yang diulang tanpa
cache. Ini menjadi dasar pemilihan Delta dan partisi bulanan di Azure.

Delta gold dan silver sudah diuji di Garage. Spark Delta gagal karena S3A
menerima HTTP 400 saat membaca `_last_checkpoint` maupun `_delta_log`.
`delta-rs` berhasil menulis, overwrite dengan predicate, dan time travel. Gold
membaca silver Delta melalui `delta-rs` lalu mengonversinya ke Spark. Di ADLS,
Spark Delta dapat dipakai tanpa batasan itu.

## Yang belum

- Backfill penuh 2015-2026 di Azure.
- Mengaktifkan jadwal retrain setelah run pertama tervalidasi.
- Memindahkan model serving dari folder lokal ke MLflow registry.
- Autentikasi dashboard dan API.
