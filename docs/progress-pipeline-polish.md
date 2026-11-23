# Progress: Pipeline Polish

Branch `feat/pipeline-polish`, basis `dev` setelah merge gold (`77dbfc8`).
Fokus: integrasi bronze sampai gold, perbaikan temuan audit, dan pengujian
end-to-end pada data asli. Lingkungan: Docker Compose, Garage, Spark `local[2]`.

## Kondisi awal

- Test bawaan: 11 lulus.
- Bronze berisi 36.528 objek, tetapi bucket `silver` dan `gold` kosong.
- `docker compose up --build` gagal karena build context dashboard mencakup
  `garage/` yang dimiliki root. Belum ada `.dockerignore`.

## Temuan runtime

| Temuan | Bukti |
|---|---|
| Publish gold ke Garage bekerja | Run 2026-11-05..10 menulis 18 file hourly dan 18 file fitur; staging bersih |
| Publish idempoten untuk rentang yang sama | Run kedua tetap 18 + 18 file, 432 + 432 baris, 360 trainable |
| `is_partial` salah pada backfill historis | Run 2025-01-05..10 menandai 432 baris WY2025 sebagai parsial, padahal WY2025 sudah selesai |
| DQ silver menghitung info sebagai gagal | 67 kegagalan continuous: 66 `provisional_values` (info) dan 1 completeness; 66 kegagalan daily: 63 provisional dan 3 completeness |
| Ambang completeness tidak dipakai | Hari dengan 95 dari 96 sampel tetap gagal karena kode membandingkan dengan 96, bukan 0,90 |
| Karantina menempel di tanggal terakhir | Ringkasan satu run ditulis ke `max(ingest_date)`, bukan per tanggal |
| Prediksi tidak bisa dibaca dashboard | `write_dataset` memakai partisi directory, bukan hive; `station_id` hilang dari schema |
| Lead time prediksi bergeser | `issued_at = time_utc + 1 jam`, sehingga h+1 menjadi lead 0 jam, h+6 menjadi 5 jam |
| Prediksi memakai fitur tidak lengkap | Tidak ada filter, LightGBM tetap memprediksi dari baris dengan lag kosong |
| Gold commit dengan 0 baris trainable | `daily-all` 2026-11-16..22 menulis 504 baris, 0 trainable, dan tetap menulis status `committed` |
| Lookback silver lebih pendek dari kebutuhan gold | Gold butuh 168 jam sebelum jendela, sedangkan silver hanya memproses 7 hari; lubang 11-15 November tidak pernah terisi |

## Perbaikan

1. **Retry upstream.** `get_json` sekarang retry 429, 5xx, timeout, dan
   connection error dengan exponential backoff dan jitter.
2. **Ambang silver dipakai.** Completeness dibandingkan dengan
   `SILVER_USGS_COMPLETENESS_MIN` dan `SILVER_METEO_COMPLETENESS_MIN`.
3. **Severity info.** Temuan `info` dicatat tetapi tidak lagi dihitung gagal,
   baik di silver maupun gold.
4. **Karantina per tanggal.** Ringkasan dihitung per `station_id` dan
   `ingest_date`.
5. **Dedupe meteo deterministik.** Baris dengan `ingest_date` terbaru menang.
6. **`is_partial` memakai water year berjalan.** Dihitung dari tanggal hari ini
   di zona `America/Los_Angeles`, bukan dari ujung jendela.
7. **Prediksi hive dan lengkap.** `partitioning_flavor="hive"`,
   `existing_data_behavior="delete_matching"`, hanya baris dengan seluruh fitur
   terisi yang diprediksi.
8. **Lead time benar.** `issued_at` sama dengan `time_utc` fitur, sehingga
   selisih ke `target_time` tepat 1, 6, dan 24 jam.
9. **Lineage prediksi.** Kolom `dataset_version` gold ditambahkan ke tabel
   prediksi.
10. **Empty grace.** Default dinaikkan dari 1 menjadi 5 hari agar gangguan
    sementara tidak dikunci sebagai partisi kosong.
11. **`.dockerignore`.** Build context tidak lagi memuat `garage/`,
    `prefect/metadata/`, dan cache.
12. **Lookback silver 14 hari.** Cukup untuk jendela gold 7 hari ditambah
    lookback fitur 168 jam.
13. **Gate lookback sebelum publish.** Empat fitur 168 jam diperiksa; jika
    lebih dari 20 persen kosong, run gagal dan tidak menulis penanda
    `committed`. Sebelumnya gold bisa publish dengan 0 baris trainable.

## Hasil verifikasi

| Pemeriksaan | Hasil |
|---|---|
| `pytest tests -q` | 23 lulus |
| Silver 2026-10-20..11-10 | 6.335 continuous, 63 daily, 1.584 meteo |
| Silver 2026-11-09..11-10 setelah fix | 0 kegagalan DQ; provisional tetap tercatat sebagai info |
| Gold 2026-11-05..11-10 | 432 baris hourly, 432 fitur, 360 trainable |
| Gold rerun rentang sama | Jumlah file dan baris tidak bertambah |
| Gold historis 2025-01-05..01-10 | 432 baris, `is_partial=False` semua |
| Prediksi (uji 6 hari) | 1.161 baris, 0 duplikat, 18 file hive |
| Lead time | 1, 6, 24 jam tepat |
| `dataset_version` prediksi | `g1-bff53b16` |
| `daily-all` setelah lookback 14 | Silver 4.031 continuous, 42 daily, 1.008 meteo, 0 gagal DQ |
| Gold setelah perbaikan | 504 baris, 432 trainable (85,7 persen), 50 soft pass, 24 info pass, 21 + 21 file |
| Prediksi pada gold asli | 1.512 baris, 21 file, 0 duplikat |
| Dashboard | Membaca 504 baris hourly dan 1.512 prediksi; chart dan hidrograf terbentuk |

## Yang belum selesai

- `daily-all` masih berhenti total jika bronze gagal. Ini disengaja agar gold
  tidak membaca silver setengah jadi, tetapi perlu alarm dan prosedur rerun.
- Debit masih harian. Lima fitur `discharge_lag_*` belum setara notebook.
  Paritas fitur belum diukur pada irisan yang sama.
- Publish belum atomik. Partisi dihapus lalu disalin; kegagalan di tengah dapat
  meninggalkan campuran versi. Delta Lake direncanakan setelah polish.
- Deployment baru dengan silver kosong masih perlu `silver-backfill`; lookback
  14 hari hanya menutup kebutuhan harian, bukan seluruh riwayat.
- Run gold lama dengan 0 trainable tetap ada di riwayat `dataset_versions`
  sebagai jejak audit. Dashboard memakai run terbaru.
- Dashboard belum punya autentikasi.
- Belum ada integrasi MLflow.

## Catatan operasional

- Untuk inferensi, salin model ke `/tmp/models` di dalam container, bukan ke
  `/prefect/models`. `/prefect` adalah bind mount `prefect/app`, jadi menyalin
  ke sana akan menaruh artefak 22 MB di working tree repo.
- `daily-all` sudah dijalankan penuh dengan `USGS_API_KEY`, `EMPTY_GRACE_DAYS=5`,
  dan `SILVER_LOOKBACK_DAYS=14`.
- Run gold lama dengan 0 trainable sengaja dibiarkan di `dataset_versions`
  sebagai jejak audit; dashboard memilih `processed_at` terbaru.

## Langkah berikutnya

1. Ukur paritas fitur dengan notebook pada irisan 2026-11-05..10.
2. Putuskan apakah `00060` continuous ditambahkan ke bronze.
3. Tambahkan pembersihan otomatis untuk run gold yang gagal di tengah publish.
4. Merge branch polish, lalu mulai MLflow.
