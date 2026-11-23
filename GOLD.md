# Gold Layer

Lapisan **gold** menyatukan tiga tabel silver menjadi satu baris per jam per
stasiun, lalu menurunkan fitur model, mencatat prediksi, dan melaporkan
kualitas data. Gold tidak mengambil data dari internet: ia hanya membaca
silver.

Cakupan: **Gold + fitur model + kontrak dashboard**. Ingestion, bronze, dan
silver tidak diubah.

Status implementasi, hasil uji, dan daftar pekerjaan yang belum selesai ada di
[docs/progress-gold-dashboard.md](docs/progress-gold-dashboard.md).

## Kenapa gold ada

Silver sengaja berhenti pada data bersih dengan resolusi asli: 15 menit untuk
`00065` dan `00060`, harian untuk `00060` sebagai pembanding, dan per jam untuk
Open-Meteo. Model dan
dashboard membutuhkan satu tabel yang sudah digabung, karena kalau keduanya
menggabung sendiri-sendiri, definisi jam lengkap dan cara menggeser data bisa
berbeda.

```
silver -> gold/delta/station_hourly -> dashboard
              |
              +-> gold/delta/features -> model -> gold/delta/predictions -> dashboard
              +-> gold/delta/model_metrics -> dashboard
```

## Urutan proses

Satu run berjalan dengan urutan tetap, dan **tidak ada yang ditulis sebelum
validasi lolos**:

1. Baca silver untuk jendela yang diminta (dipotong ke hari terakhir yang
   benar-benar punya data).
2. Bentuk grid per jam, lalu tabel `station_hourly`.
3. Bentuk `features` beserta target.
4. Validasi keras: kolom lengkap, tidak kosong, tidak ada duplikat
   `(station_id, time_utc)`. Kegagalan di sini menghentikan run tanpa menulis
   apa pun.
5. Jalankan pemeriksaan kualitas lunak dan siapkan laporan DQ.
6. Tulis `station_hourly` dan `features` sebagai tabel Delta dengan
   `mode=overwrite` dan predicate `date_utc` sesuai jendela. Delta membuat
   commit atomik per tabel, jadi tidak ada lagi staging dan salin file.
7. Tulis laporan DQ sebagai Parquet, terpisah dari tabel gold.
8. Terakhir, append satu baris ke `gold/delta/dataset_versions` dengan
   `status=committed`. Baris ini adalah penanda bahwa run selesai; consumer
   dapat mengabaikan run yang tidak pernah menulis penanda.

Delta dipakai melalui `delta-rs`. Spark Delta tidak dipakai di lokal karena
S3A di Garage mengembalikan HTTP 400 saat membaca `_last_checkpoint`, bukan
404. `delta-rs` menangani object store dengan `AWS_S3_ALLOW_UNSAFE_RENAME`,
aman untuk penulis tunggal. Di Azure, tabel yang sama dapat dibaca oleh Spark
Delta di atas ADLS.

## Versi dataset

`dataset_version` adalah identitas **definisi** dataset, misalnya `g1-3f9c1a2b`.
Nilainya berasal dari hash daftar fitur, target, jendela, rasio minimal sampel,
ambang jam lengkap, horizon, zona waktu, dan sumber debit (`hourly` atau
`daily`). Sumber debit ikut masuk karena nilai `discharge_lag_*` berbeda meski
nama kolomnya sama. Versi ini hanya berubah bila definisi berubah, bukan setiap
hari.

Informasi pemrosesan dicatat terpisah di `gold/delta/dataset_versions`:
`run_id`, `status`, `data_start_date`, `data_end_date`, `processed_at`, jumlah
baris, jumlah baris siap latih, dan sumber debit. Model sebaiknya menyimpan
`dataset_version` **dan** `data_end_date`, karena keduanya menjawab pertanyaan
berbeda.

## Struktur output

```
gold/delta/station_hourly/          Delta, partisi station_id + date_utc
gold/delta/features/                Delta, partisi station_id + date_utc
gold/delta/predictions/             Delta, partisi station_id + issue_date
gold/delta/model_metrics/           Delta, snapshot metrik terbaru
gold/delta/dataset_versions/        Delta, satu baris per run
gold/_dq/table=features/run_id=<id>/part-*.parquet
```

Setiap tabel Delta memiliki `_delta_log`, sehingga versi sebelumnya bisa
dibaca dengan time travel. `station_hourly` dan `features` dipartisi
`station_id` + `date_utc`, sehingga run harian hanya menimpa hari UTC yang
dihitungnya. Kolom `date_local` tetap tersedia untuk tampilan dan analisis
musiman.

Laporan DQ disimpan per run, bukan per tanggal, karena pemeriksaan tingkat
jendela tidak mewakili satu tanggal. Setiap baris punya `scope` (`date` atau
`window`), `date` (hanya untuk scope `date`), `window_start`, dan
`window_end`.

## Kolom `station_hourly`

| Kelompok | Kolom |
|---|---|
| Waktu | `time_utc`, `time_local`, `date_utc`, `date_local`, `year`, `water_year` |
| Ketinggian air | `stage_mean_ft`, `stage_max_ft`, `stage_min_ft`, `stage_n`, `stage_complete` |
| Debit | `discharge_mean_cfs` (per jam, bila ada), `discharge_daily_cfs` (harian), `discharge_model_cfs` (yang dipakai fitur), `discharge_grain` |
| Cuaca | `precip_mm`, `rain_mm`, `temp_c`, `rh_pct`, `dewpoint_c`, `soil_moist_0_7`, `soil_moist_7_28`, `soil_moist_28_100`, `et0_mm`, `wind_kmh`, `cloud_pct`, `swrad_wm2`, `pressure_hpa` |
| Metadata | `meteo_n` (variabel terisi), `meteo_variables_available` (variabel yang tersedia di sumber), `dataset_version` |

`stage_complete` bernilai benar bila satu jam memuat minimal 3 dari 4 sampel
15 menit. Jam tanpa data tetap ada sebagai baris (grid penuh), dengan nilai
`null`.

## Kolom `features`

48 kolom fitur dengan nama identik dengan notebook:

- `stage_now`, `stage_lag_1..168`, `stage_mean_3..168`, `stage_max_3..168`,
  `stage_diff_1..24`
- `discharge_lag_1..72`
- `precip_sum_1..168`, `precip_sum_24_lag_24`, `precip_sum_72_lag_24`
- `soil_moist_0_7`, `soil_moist_7_28`, `temp_c`, `et0_mm`, `rh_pct`
- `hour_sin`, `hour_cos`, `doy_sin`, `doy_cos`

Target: `y_h1`, `y_h6`, `y_h24` (ketinggian air `horizon` jam ke depan),
ditambah `is_partial` (water year berjalan, dihitung dari tanggal hari ini di
zona `America/Los_Angeles`, bukan dari ujung jendela) dan `is_trainable`
(seluruh fitur dan target terisi). Model tetap dilatih pada **selisih**
`y_h - stage_now`, sama seperti notebook.

## Kolom `predictions`

| Kolom | Isi |
|---|---|
| `station_id` | Stasiun, bagian dari partisi hive |
| `issued_at` | Waktu fitur `time_utc`; horizon dihitung dari waktu ini |
| `target_time` | `issued_at + horizon` |
| `horizon` | 1, 6, atau 24 jam |
| `y_pred` | Level air hasil prediksi, sudah ditambah `stage_now` |
| `y_obs` | Observasi pada `target_time`, bila tersedia |
| `model_version` | Versi model dari metadata notebook |
| `dataset_version` | Versi definisi gold yang dipakai saat inferensi |
| `created_at` | Waktu skrip inferensi menulis baris |

Hanya baris dengan seluruh 48 fitur terisi yang diprediksi. Baris tanpa debit
harian, misalnya jam setelah sumber harian berakhir, dilewati daripada
dipaksakan. Partisi memakai hive (`station_id=.../issue_date=...`) agar dapat
dibaca dashboard dengan `partitioning="hive"`.

Jendela bergulir memakai `min_periods` 90 persen, sama dengan notebook. Ini
penting karena fungsi agregat Spark mengabaikan nilai kosong; tanpa penjaga
itu satu nilai dalam jendela 168 jam akan dianggap jendela penuh.

## Menjalankan

```sh
# seluruh riwayat (perlu silver terisi)
prefect deployment run 'gold-transform/gold-backfill'

# jendela harian gold saja (untuk rerun manual)
prefect deployment run 'gold-transform/daily-gold'

# rantai harian lengkap: bronze -> silver -> gold (jadwal 02.30 UTC)
prefect deployment run 'daily-all/daily-all'

# uji cepat satu bulan
docker compose exec prefect-worker python -c \
  "from gold.config import GoldSettings; from gold.pipeline import run_gold; \
   print(run_gold('2024-01-01','2024-01-31', GoldSettings.from_env()))"

# uji logika tanpa jaringan maupun data asli
docker compose exec prefect-worker python /prefect/scripts/smoke_gold.py

# test
docker compose exec prefect-worker python -m pytest tests -q
```

Hanya `daily-all` yang terjadwal. Deployment bronze, bronze+silver, dan gold
lainnya tetap ada untuk dijalankan manual, tetapi jadwalnya dinonaktifkan agar
tidak ada pekerjaan yang tumpang tindih dan gold tidak pernah membaca silver
yang belum selesai.

Tanpa object storage (`S3_ENDPOINT_URL` kosong), gold membaca dan menulis di
`BRONZE_LOCAL_DIR` dengan struktur yang sama.

## Keputusan dan batasan versi lokal

1. **Debit per jam sebagai fitur.** Pipeline bronze mengambil `00060`
   continuous 15 menit, dan `00060` daily tetap disimpan sebagai pembanding.
   Gold mengisi `discharge_model_cfs` dari nilai per jam dan menandai
   `discharge_grain = "hourly"`. `discharge_daily_cfs` tetap tersedia untuk
   QC. Sumber debit masuk ke hash `dataset_version`, karena hourly dan daily
   menghasilkan nilai `discharge_lag_*` yang berbeda.
2. **Titik embun belum ada.** `dew_point_2m` belum diambil pipeline, jadi
   `dewpoint_c` bernilai `null`. Kolom ini tidak dipakai model, sehingga tidak
   memengaruhi fitur; ia disediakan agar skema tidak berubah nanti.
   `meteo_n` hanya menghitung variabel yang benar-benar tersedia, dan
   `meteo_variables_available` mencatat jumlahnya (saat ini 12) agar angka itu
   tidak disalahartikan sebagai kekurangan data.
3. **Tekanan.** Pipeline mengambil `surface_pressure`, notebook memakai
   `pressure_msl`. Dipetakan ke `pressure_hpa` dan dicatat sebagai perbedaan
   semantik. Tidak dipakai model.
4. **Periode.** Gold mengikuti silver; default `2015-10-01`. Notebook memakai
   2007-2026 dan melatih 2011-2026. Selama silver belum diisi sejak 2007,
   hasil pelatihan dari gold tidak akan identik dengan notebook.
5. **Gold dapat dibangun ulang penuh.** Tidak ada state tambahan; menjalankan
   ulang rentang yang sama menimpa partisi yang sama.

## Uji paritas dengan notebook

Setelah gold berisi dan notebook sudah menyimpan `data/processed/
features_complete_<station>.parquet`:

```sh
python scripts/parity_check.py \
  --gold data/gold/delta/features \
  --notebook data/processed/features_complete_11150500.parquet \
  --station 11150500
```

Skrip membandingkan setiap kolom fitur pada irisan waktu yang sama dan
melaporkan selisih absolut maksimum. Target: di bawah `1e-6`. Selama debit
masih harian, kolom `discharge_lag_*` akan berbeda dan skrip akan
menandainya; itu sesuai dengan batasan nomor 1.



