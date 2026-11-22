# Progress â€” Gold Layer dan Dashboard

Catatan pekerjaan bagian **Gold + Dashboard** (Vio). Bagian ingestion, bronze,
dan silver dikerjakan terpisah dan tidak diubah oleh pekerjaan ini.

- Branch: `feat/gold-dashboard`
- Basis: `dev` (`9cd06a1`, merge PR silver)
- Status: gold selesai dan teruji pada data asli; dashboard berjalan; tabel
  prediksi masih kosong karena menunggu model dari notebook.

## Ringkasan

| Bagian | Status |
|---|---|
| Gold: dataset per jam | Selesai, 3.600 baris / 50 hari / 3 stasiun |
| Gold: fitur model (48 kolom + 3 target) | Selesai, nama kolom sama dengan notebook |
| Gold: laporan kualitas data | Selesai, 308 soft-pass, 6 info-fail |
| Orkestrasi (deployment `daily-all`) | Selesai, bronze -> silver -> gold dalam satu run |
| Dashboard Streamlit | Berjalan, panel prediksi menunggu `gold/predictions` |
| Inference prediksi | Skrip siap, butuh model Yuda (`data/models`) |
| Automated test | `tests/` (pytest): spesifikasi cepat + validasi Spark |

## Perbaikan setelah code review (22 Nov)

1. **Validasi sebelum tulis.** `gold/validation.py` memeriksa kolom, baris
   kosong, dan duplikat `(station_id, time_utc)` sebelum tabel ditulis. Sebelum
   ini pemeriksaan berjalan setelah `write_table`.
2. **Penanda run.** `gold/dataset_versions` ditulis terakhir dengan
   `status=committed`. Bila penulisan tabel gagal, penanda tidak dibuat, dan
   dashboard menampilkan peringatan.
3. **Versi dataset.** `dataset_version` sekarang hash definisi (`g1-xxxxxxxx`),
   bukan tanggal data. Rentang data dicatat terpisah (`data_start_date`,
   `data_end_date`, `processed_at`, `run_id`).
4. **DQ tingkat jendela.** Laporan DQ punya `scope` (`date`/`window`),
   `window_start`, `window_end`, dan disimpan per `run_id`. Pemeriksaan seperti
   `feature_null_rate` tidak lagi ditempel ke tanggal terakhir.
5. **Spark bersama.** `common/spark.py`; `silver/spark.py` tinggal pembungkus,
   sehingga gold tidak lagi mengimpor modul silver.
6. **`meteo_n`.** Hanya menghitung variabel yang benar-benar ada, ditambah
   `meteo_variables_available` agar angka 12 tidak disalahartikan sebagai
   kekurangan.
7. **Penjadwalan.** Flow `daily_all` menjalankan bronze -> silver -> gold
   berurutan; jadwal `bronze-ingest`, `daily-bronze-silver`, dan `daily-gold`
   dinonaktifkan sehingga tidak ada ingestion ganda dan gold tidak membaca
   silver yang belum selesai.
8. **Dashboard.** Filter `date_utc` dan pemilihan kolom dilakukan di level
   dataset pyarrow, bukan setelah seluruh tabel masuk pandas.
9. **Test.** `tests/test_gold_spec.py` (cepat) dan
   `tests/test_gold_validation.py` (Spark) memeriksa kontrak fitur, versi,
   jendela waktu, partisi, dan aturan validasi.

## Yang dibuat

| File | Isi |
|---|---|
| `prefect/app/gold/config.py` | Pengaturan env, jendela tanggal, horizon target |
| `prefect/app/gold/schemas.py` | Skema kolom gold, daftar 48 fitur, pemetaan nama cuaca |
| `prefect/app/gold/readers.py` | Baca silver dengan pemangkasan partisi per tanggal |
| `prefect/app/gold/hourly.py` | Grid per jam, rollup 15 menit, join debit dan cuaca |
| `prefect/app/gold/features.py` | Fitur dan target dengan jendela 90 persen |
| `prefect/app/gold/quality.py` | Pemeriksaan kelengkapan jam, nilai kosong, trainable |
| `prefect/app/gold/writer.py` | Penulisan idempoten per `station_id` + `date_utc` |
| `prefect/app/gold/pipeline.py` | Orkestrasi satu run gold |
| `prefect/app/flows/gold_flow.py` | Flow Prefect `gold-transform` |
| `prefect/app/prefect.yaml` | Deployment `daily-all` (02.30 UTC), `daily-gold`, dan `gold-backfill` |
| `dashboard/app.py` | Aplikasi Streamlit: prediksi, KPI, hidrograf, kualitas |
| `dashboard/requirements.txt`, `config/dashboard/Dockerfile` | Image dashboard |
| `compose.yaml` | Service `dashboard`, mount `scripts/` untuk worker, `start_period` Postgres |
| `.env.example` | Variabel gold dan port dashboard |
| `scripts/smoke_gold.py` | Uji gold dengan data sintetis, tanpa jaringan |
| `scripts/parity_check.py` | Bandingkan fitur gold dengan notebook |
| `scripts/predict_gold.py` | Isi `gold/predictions` dari model LightGBM |
| `prefect/app/gold/validation.py` | Validasi keras sebelum data ditulis |
| `prefect/app/gold/version.py` | Identitas definisi dataset (`g1-xxxxxxxx`) |
| `prefect/app/common/spark.py` | Spark session bersama untuk semua layer |
| `prefect/app/flows/daily_all.py` | Rantai harian bronze -> silver -> gold |
| `tests/test_gold_spec.py`, `tests/test_gold_validation.py` | Test cepat dan test validasi Spark |
| `GOLD.md` | Dokumentasi teknis lapisan gold |

## Hasil uji pada data asli

Rentang yang diisi: bronze dan silver 2024-01-01 sampai 2026-11-19
(1.054 hari), gold 2026-10-01 sampai 2026-11-19 (50 hari).

| Pemeriksaan | Hasil |
|---|---|
| Partisi bronze USGS | 6.324 (1.054 hari x 3 stasiun x 2 parameter) |
| Partisi bronze cuaca | 3.162 (1.054 hari x 3 stasiun) |
| File silver | 12.642 |
| Baris `gold/station_hourly` | 3.600 (50 hari x 24 jam x 3 stasiun) |
| Baris `gold/features` | 3.600, dengan 3.528 siap latih |
| Versi dataset (definisi) | `g1-bff53b16` |
| Sumber debit | `daily` (tabel `00060` masih harian) |
| DQ | 308 soft-pass, 6 info-fail, 0 hard-fail |
| Waktu proses gold 50 hari | sekitar 11 menit pada `local[2]`, driver 2 GB |
| Run `daily-gold` via Prefect | Completed; jumlah baris tetap 3.600 (riwayat tidak tertimpa) |
| `daily-all` (bronze -> silver -> gold) | Terdaftar; uji penuh menunggu `USGS_API_KEY` |

Uji logika tanpa data asli: `scripts/smoke_gold.py` menghasilkan 216 baris
(3 stasiun x 24 jam x 3 hari) dengan 72 baris siap latih.

## Cara menjalankan

```sh
docker compose up -d

# rantai harian lengkap (jadwal utama)
prefect deployment run 'daily-all/daily-all'

# gold saja untuk rerun manual
prefect deployment run 'gold-transform/daily-gold'

# seluruh riwayat yang tersedia di silver
prefect deployment run 'gold-transform/gold-backfill'

# uji logika tanpa jaringan
docker compose exec prefect-worker python /prefect/scripts/smoke_gold.py

# test
docker compose exec prefect-worker python -m pytest tests -q

# dashboard
# http://127.0.0.1:8501
```

Mengisi prediksi (setelah model notebook tersedia):

```sh
docker cp data/models project-prefect-worker-1:/prefect/models
docker compose exec prefect-worker pip install lightgbm
docker compose exec prefect-worker python /prefect/scripts/predict_gold.py \
  --models-dir /prefect/models --days 7
```

## Keputusan teknis

1. **Grid per jam dibangun eksplisit.** Data punya lubang (satu stasiun bisa
   kosong berhari-hari), sehingga `lag` berbasis baris akan melompati jam yang
   hilang. Grid membuat setiap jam selalu ada, dengan nilai `null` bila tidak
   ada sampel.
2. **`stage_complete` = minimal 3 dari 4 sampel per jam.** Satu jam dengan satu
   sampel tidak setara dengan jam dengan empat sampel.
3. **Nama fitur identik dengan notebook.** Tujuannya agar model Yuda bisa
   dipindahkan ke pipeline tanpa mengganti nama kolom dan hasilnya dapat
   dibandingkan lewat `scripts/parity_check.py`.
4. **Jendela bergulir mensyaratkan 90 persen sampel.** Fungsi agregat Spark
   mengabaikan nilai kosong; tanpa penjaga ini satu nilai dalam jendela 168 jam
   akan dianggap jendela penuh.
5. **Partisi `station_id` + `date_utc`.** Jendela proses memakai hari UTC.
   Partisi tanggal lokal membuat tepi jendela kehilangan baris, dan partisi
   tahun membuat run harian 7 hari menimpa seluruh tahun.
6. **`end_date` dipotong ke hari terakhir yang punya data.** Mencegah gold
   menulis sehari penuh nilai kosong ketika bronze belum berjalan.
7. **Debit harian tetap ditandai.** `discharge_grain` mencatat `daily` atau
   `hourly`, sehingga kekurangan data terlihat di tabel, bukan tersembunyi.

## Masalah yang ditemukan dan diperbaiki saat pengujian

1. `daily_to_hourly` memakai kerangka yang belum punya `date_local`.
2. Fitur siklik (`hour_sin`, `hour_cos`, `doy_sin`, `doy_cos`) belum dihitung.
3. Partisi `station_id/year` membuat run harian menghapus riwayat setahun.
4. `dataset_version` dihitung dari jendela internal, bukan baris yang ditulis.
5. Reader membaca 1.054 partisi per tabel per stasiun; sekarang hanya partisi
   dalam jendela yang dibaca (gold 50 hari turun dari macet menjadi Â±11 menit).

## Batasan versi lokal

1. **Debit masih harian.** Pipeline bronze menyimpan `00060` dari koleksi
   `daily`, sedangkan notebook memakai `00060` continuous 15 menit. Fitur
   `discharge_lag_1/3/6` belum bermakna sampai debit 15 menit ditambahkan.
2. **`dewpoint_c` kosong.** `dew_point_2m` belum diambil pipeline. Kolom ini
   tidak dipakai model, jadi fitur tidak terpengaruh.
3. **Tekanan.** Pipeline memakai `surface_pressure`, notebook `pressure_msl`;
   dipetakan ke `pressure_hpa` dan dicatat sebagai perbedaan semantik.
4. **Periode mengikuti silver.** Gold default mulai 2015-10-01, tetapi silver
   yang terisi baru 2024-01-01 ke atas. Notebook memakai 2007-2026.
5. **Uji paritas belum dijalankan**, karena notebook tidak menyimpan
   `features_complete_<station>.parquet` di repo ini.

## Yang belum dan butuh tim

1. **Model Yuda** (`data/models/*.txt` + `metadata.json`) untuk mengisi
   `gold/predictions`. Tanpa itu panel prediksi dashboard menampilkan pesan
   kosong.
2. **`USGS_API_KEY` di `.env`.** Key hanya dipakai sekali saat pengujian dan
   tidak disimpan ke repo; deployment bronze 02.00 UTC akan gagal tanpanya.
3. **Backfill gold lebih panjang** bila ingin data latih lebih banyak
   (`gold-backfill` dari 2024-01-01 memerlukan waktu lama pada `local[2]`).
4. **Debit 15 menit** (`00060` continuous) perlu ditambahkan ke bronze agar
   fitur debit setara notebook.
5. **Push dan PR** ke `dev` setelah review.
6. **Dashboard belum punya autentikasi**; masih dibuka di `127.0.0.1:8501`.

## Cara memeriksa cepat

```sh
docker compose ps
docker compose logs dashboard --tail 20
docker compose exec prefect-worker prefect deployment ls
docker compose exec prefect-worker prefect flow-run ls --limit 5
```



