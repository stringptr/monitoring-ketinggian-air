# Progress: Delta Gold

Branch `feat/delta-gold`, basis `dev` setelah merge training dan discharge
continuous. Fokus: mengganti publish gold dari Parquet staging ke Delta Lake.

## Hasil spike

| Uji | Hasil |
|---|---|
| Spark Delta 4.0 di Garage | Gagal. S3A menerima HTTP 400 saat membaca `_last_checkpoint`, bukan 404 |
| `delta-rs` 1.7 di Garage | Berhasil menulis, overwrite, dan time travel dengan `AWS_S3_ALLOW_UNSAFE_RENAME=true` |
| Sebab | Garage belum mendukung conditional put yang dibutuhkan Spark Delta |

Keputusan: gold lokal memakai `delta-rs`. Di Azure, tabel yang sama dapat dibaca
dan ditulis oleh Spark Delta di atas ADLS.

## Perubahan

1. `gold/delta.py`: storage options, path, `write_delta`, `read_delta`,
   `history`, `table_exists`.
2. `gold/writer.py`: tabel gold ditulis sebagai Delta. Staging, `publish_table`,
   dan salin file dihapus. Laporan DQ tetap Parquet.
3. `gold/pipeline.py`: menulis `station_hourly` dan `features` dengan
   `mode=overwrite` dan predicate `date_utc`, lalu append `dataset_versions`.
4. `training/dataset.py`: membaca fitur dari Delta.
5. `training/writer.py`: `model_metrics` menjadi Delta.
6. `dashboard/app.py`: membaca Delta dengan `delta-rs`.
7. `scripts/predict_gold.py`: membaca fitur dan menulis prediksi Delta.
8. Dockerfile worker dan dashboard menambahkan `deltalake>=1.7`.
9. Test `tests/test_gold_delta.py`: tulis, overwrite predicate, filter, append,
   dan history. Test staging lama dihapus.

## Struktur

```text
gold/delta/station_hourly/
gold/delta/features/
gold/delta/predictions/
gold/delta/model_metrics/
gold/delta/dataset_versions/
```

Tabel Parquet lama di `gold/station_hourly`, `gold/features`, dan
`gold/dataset_versions` dibiarkan sebagai artefak sebelumnya. Consumer baru
membaca jalur `gold/delta`.

## Verifikasi

| Pemeriksaan | Hasil |
|---|---|
| `pytest` | 40 lulus |
| Gold 2026-11-05..10 | 432 baris, 432 trainable, `discharge_grain=hourly` |
| Rerun jendela sama | 432 baris, tidak ada duplikat, history Delta 2 |
| `dataset_versions` | 2 baris, satu per run |
| Training dari Delta | 9 model, 216 train, 144 validasi, 72 test |
| MLflow | Run `9e6d51e3...`, experiment `hydrology-delta-smoke` |
| Prediksi Delta | 1.296 baris, lead 1/6/24 jam, 0 duplikat |
| Dashboard | Membaca 432 baris hourly dan 1.296 prediksi, chart terbentuk |

## Batasan

- `AWS_S3_ALLOW_UNSAFE_RENAME` hanya aman untuk penulis tunggal. Jangan jalankan
  dua penulis gold bersamaan di lokal.
- Silver sekarang Delta. Lihat `docs/progress-delta-silver.md`.
- `model_metrics` ditulis sebagai snapshot, bukan riwayat. Riwayat run ada di
  MLflow.
- Tabel Parquet lama tidak dihapus.
- Spark Delta di lokal belum bisa dipakai sampai Garage mendukung conditional
  put, atau sampai pindah ke ADLS.
