# Silver layer (daily)

Dokumen ini menjelaskan lapisan **silver** yang dibangun di atas bronze milik
pekerjaan sebelumnya. Bronze tidak diubah: alur ingestion, bucket, dan
partisinya tetap apa adanya. Silver hanya membaca partisi bronze lalu menulis
tabel harian yang sudah bersih, bertipe, dan tervalidasi.

Cakupan: Silver + Transformation + Data Quality. Gold, dashboard, modelling,
dan MLOps tidak termasuk.

## Ringkasan desain

- **Grain:** satu baris per `(station_id, date)`, dengan `date` adalah **hari
  lokal** stasiun (`America/Los_Angeles`), bukan UTC.
- **Jadwal:** sekali sehari, mengikuti prinsip bronze. Setiap run memproses
  window 7 hari lokal terakhir (`SILVER_LOOKBACK_DAYS`) secara idempotent.
- **Backfill:** satu job Spark untuk seluruh histori (default mulai
  `2015-10-01`).
- **Engine:** PySpark local (`local[2]`), dipilih agar transformasi mudah
  dipindahkan ke Azure Synapse/Databricks.
- **Satuan:** dipertahankan seperti sumber (ft, ft³/s, mm, °C, hPa, km/jam,
  W/m², m³/m³, %).

### Kenapa semua tanggal memakai hari lokal?

USGS daily `00060` adalah rata-rata untuk *water day* lokal stasiun. Ini sudah
diverifikasi langsung ke API:

```
USGS daily 00060 untuk 2024-01-01 = 61.1 cfs
rata-rata 15 menit pada hari UTC   (00:00–23:45Z) = 66.299
rata-rata 15 menit pada hari lokal (08:00Z–07:45Z) = 61.175  <- cocok
```

Karena itu gage height dan meteo juga diagregasi per hari lokal supaya ketiga
sumber sejajar. Efek sampingnya: hari lokal `D` baru lengkap setelah
`07:00/08:00Z` tanggal `D+1`, sehingga run harian memproses window ke belakang,
bukan hanya kemarin.

### Gage height harian adalah turunan

USGS tidak menyediakan `00065` di collection `daily` (0 baris untuk ketiga
stasiun, sudah dicek). Karena itu gage height harian dihitung dari data
`continuous` 15 menit yang sudah ada di bronze:

- `mean`, `min`, `max` dari sampel 15 menit,
- `sample_count` dan `expected_samples` (92/96/100 tergantung DST),
- jumlah sampel `Approved` dan `Provisional`,
- `max_gap_seconds` (jarak antar sampel terjauh dalam hari lokal).

## Tabel output

Semua tabel ditulis sebagai Parquet dengan partisi
`station_id=<site_no>/date=YYYY-MM-DD`.

### `silver/usgs_daily`

Discharge harian native (`00060`, statistic `00003`, satuan `ft^3/s`).

| Kolom | Keterangan |
|---|---|
| `station_id` | site number, mis. `11525670` |
| `date` | hari lokal `YYYY-MM-DD` |
| `parameter_code` | `00060` |
| `value_cfs` | nilai harian (double) |
| `unit`, `statistic_id`, `approval_status`, `last_modified` | metadata sumber |

### `silver/usgs_gage_height_daily`

Gage height harian hasil agregasi data 15 menit (`00065`).

| Kolom | Keterangan |
|---|---|
| `gage_height_mean_ft`, `_min_ft`, `_max_ft` | statistik harian (ft) |
| `sample_count` | jumlah sampel valid yang masuk agregasi |
| `expected_samples` | 92/96/100 sesuai panjang hari lokal |
| `approved_samples`, `provisional_samples` | komposisi status approval |
| `max_gap_seconds` | celah terbesar antar sampel dalam hari itu |

### `silver/meteo_daily`

Agregasi Open-Meteo per hari lokal.

| Kolom | Agregasi |
|---|---|
| `precipitation_mm`, `rain_mm`, `et0_mm` | sum |
| `temperature_2m_mean_c`, `_min_c`, `_max_c` | mean/min/max |
| `relative_humidity_2m_mean_pct`, `surface_pressure_mean_hpa` | mean |
| `soil_moisture_0_7_mean`, `_7_28_mean`, `_28_100_mean` | mean |
| `wind_speed_10m_mean_kmh`, `_max_kmh` | mean/max |
| `shortwave_radiation_mean_wm2`, `_sum_whm2` | mean/sum |
| `cloud_cover_mean_pct` | mean |
| `observed_hours`, `expected_hours` | 23/24/25 sesuai DST |

`shortwave_radiation_sum_whm2` adalah jumlah nilai hourly (W/m²) dalam satu
hari; secara numerik setara Wh/m² karena tiap nilai mewakili satu jam.

### `silver/station_daily`

Tabel utama: `station_daily` menggabungkan ketiga sumber di atas dengan
`full outer join` pada `(station_id, date)`.

Kolom tambahan:

- `gage_height_completeness` = `sample_count / expected_samples`
- `meteo_completeness` = `observed_hours / expected_hours`
- `has_discharge`, `has_gage_height`, `has_meteo`

### Artefak kualitas data

```
silver/_dq/table=<table>/run_id=<run_id>/part-*.parquet
silver/_quarantine/table=<table>/station_id=.../date=.../part-*.parquet
```

`_dq` berisi satu baris per check dengan `severity`, `passed`, `failed_count`,
`total_count`, dan `details` JSON. Tabel quarantine:

- `usgs_continuous_00065` — sampel 15 menit yang timestamp/nilainya tidak valid
- `usgs_daily_00060` — baris discharge yang tidak valid
- `meteo_hourly` — nilai Open-Meteo di luar rentang wajar atau timestamp rusak

## Aturan data quality

Hard-fail (flow gagal):

- schema/tipe tidak sesuai kontrak pandera,
- duplikat natural key (`station_id`, `date`, `parameter_code` untuk USGS
  daily; `station_id`, `date` untuk tabel lain).

Soft-fail (dicatat di `_dq`, data tetap ditulis):

| Check | Ambang default |
|---|---|
| `gage_height_completeness` | < 0.90 dari expected samples |
| `meteo_completeness` | < 0.95 dari expected hours |
| `gage_height_max_gap` | > 3 jam |
| `missing_discharge`, `missing_gage_height`, `missing_meteo` | ada baris tanpa sumber |
| `value_range` | nilai negatif / RH atau cloud di luar 0–100 / soil moisture di luar 0–1.5 / suhu di luar −60…60 °C / tekanan di luar 800–1100 hPa |
| `freshness` | data terbaru lebih tua dari 2 hari (hanya untuk run harian) |
| `provisional_gage_height` | ada sampel provisional (info) |

Ambang dapat diubah lewat `SILVER_*` di `.env` (lihat `.env.example`).

## Sumber data bronze yang dipakai

| Sumber | Prefix bronze | Dipakai untuk |
|---|---|---|
| USGS continuous `00065` | `usgs/continuous/parameter_code=00065/station_id=<site>/` | gage height harian |
| USGS daily `00060` | `usgs/daily/parameter_code=00060/station_id=<site>/` | discharge harian |
| Open-Meteo | `open-meteo/station_id=<site>/` | meteo harian |

`LOOKBACK_DAYS=0` di bronze membuat setiap partisi berisi tepat satu hari UTC.
Silver membaca partisi UTC `D` dan `D+1` untuk membentuk hari lokal `D`.

## Menjalankan

### Build ulang image worker

Image worker sekarang berisi Java 17, PySpark, pyarrow, pandera, dan jar S3A.

```sh
docker compose build prefect-server prefect-worker
docker compose up -d
```

### Backfill seluruh histori

Sekali saja, sebagai satu job Spark:

```sh
docker compose exec prefect-worker python -m flows.silver
```

atau lewat deployment manual:

```sh
prefect deployment run 'silver-transform/silver-backfill'
```

Parameter opsional: `start_date`, `end_date` (format `YYYY-MM-DD`).

Jika backfill penuh kehabisan memori, naikkan `--driver-memory` pada
`PYSPARK_SUBMIT_ARGS` di `config/prefect/Dockerfile` (misalnya `4g`), atau
pecah backfill per tahun dengan mengisi `start_date`/`end_date`.

### Run harian

Deployment `daily-bronze-silver` menjalankan bronze lalu silver setiap
`02:30 UTC`:

```sh
prefect deployment run 'daily-bronze-silver/daily-bronze-silver'
```

Nonaktifkan deployment lama `bronze-ingest` di Prefect UI setelah deployment
baru aktif, supaya bronze tidak dijalankan dua kali per hari. Menjalankan
bronze dua kali tidak merusak data (idempotent), hanya menambah request.

### Tanpa object storage

Jika `S3_ENDPOINT_URL` dikosongkan, silver membaca dan menulis ke
`BRONZE_LOCAL_DIR` (default `/prefect/data`) dengan struktur yang sama.

## Status verifikasi

- Transformasi, join, penulisan partisi, kontrak pandera, dan jalur quarantine
  sudah diuji end-to-end dengan PySpark 4.0.2 + Java 21 pada data sintetis
  lokal (dua hari, termasuk nilai rusak). Partisi lama tetap utuh saat satu
  hari diproses ulang.
- Pembacaan/penulisan ke Garage melalui S3A **belum** diuji karena Docker tidak
  berjalan di mesin pengembangan. Setelah `docker compose up`, jalankan backfill
  satu hari lebih dulu untuk memverifikasi koneksi S3A sebelum backfill penuh.
- Image worker bertambah sekitar 600 MB karena jar AWS SDK v2 bundle untuk S3A.

## Batasan yang diketahui

1. **Ukuran tabel harian ≈ 12 ribu baris** (3 stasiun × ~4.000 hari), di bawah
   syarat ≥100.000 records di `rencana.txt`. Tabel hourly bisa ditambahkan
   nanti tanpa mengubah tabel harian.
2. **Bronze tidak me-refresh partisi lama.** Karena ingestion melewati partisi
   yang sudah ada, revisi USGS untuk tanggal lama tidak akan terlihat di
   silver. Silver hanya bisa mengolah ulang apa yang ada di bronze.
3. **Tanggal daily adalah hari lokal.** Jangan membandingkannya langsung
   dengan agregat UTC tanpa konversi.
4. **DST.** Hari transisi berisi 92/100 sampel dan 23/25 jam; kolom
   `expected_*` dan completeness sudah memperhitungkannya.
5. **Gage height harian adalah turunan**, bukan nilai resmi USGS. Jangan
   dikutip sebagai "USGS daily 00065".
6. **Writer memakai Spark + S3A directory committer ke Garage.** Jika object
   store menolak operasi rename/multipart, ganti `silver/writer.py` untuk
   menulis lewat `s3fs`/pyarrow tanpa mengubah transformasinya.

## Peta ke Azure

| Lokal | Azure |
|---|---|
| Garage `bronze`/`silver` | ADLS Gen2 containers |
| `s3a://...` | `abfss://...` |
| PySpark `local[2]` | Synapse Spark pool |
| `spark/hadoop-aws` | kredensial via managed identity / SAS |
| `_dq`/`_quarantine` | tabel/lakehouse yang sama |
| Prefect local worker | Prefect worker di VM/Container Apps (atau orkestrator Azure) |

Transformasi di `silver/transform.py` tidak bergantung pada S3; hanya
`silver/readers.py`, `silver/writer.py`, dan `silver/spark.py` yang memuat
detail storage.

## Struktur kode

```
prefect/app/silver/
  config.py       # env, timezone, threshold, window harian
  spark.py        # SparkSession + konfigurasi S3A/Garage
  schemas.py      # schema JSON bronze, schema output, kontrak pandera
  readers.py      # baca partisi bronze -> DataFrame bertipe
  transform.py    # cleaning, agregasi lokal, join
  quality.py      # hard/soft checks, report
  writer.py       # overwrite partisi, DQ report, quarantine
  pipeline.py     # orkestrasi satu run Spark
prefect/app/flows/
  silver.py       # flow silver_pipeline
  daily_pipeline.py # flow bronze -> silver
```
