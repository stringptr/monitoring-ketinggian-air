# Silver layer (native resolution)

Dokumen ini menjelaskan lapisan **silver** yang dibangun di atas bronze milik
pekerjaan sebelumnya. Bronze tidak diubah: alur ingestion, bucket, dan
partisinya tetap apa adanya. Silver hanya membersihkan data pada **resolusi
asli**, tanpa agregasi dan tanpa join.

Cakupan: Silver + Transformation + Data Quality. Gold, dashboard, modelling,
dan MLOps tidak termasuk.

## Prinsip

Silver mempertahankan granularitas sumber:

- USGS continuous `00065`: satu baris per sampel 15 menit.
- USGS daily `00060`: satu baris per hari native.
- Open-Meteo: satu baris per jam.

Yang dilakukan silver hanya:

1. Deduplikasi.
2. Membersihkan dan mengarantina data rusak.
3. Menambahkan waktu lokal dari UTC.
4. Validasi kontrak data.
5. Menjalankan data quality checks.

Yang **tidak** dilakukan silver:

- agregasi 15 menit/hourly menjadi harian,
- join USGS + Open-Meteo,
- `station_daily` / `station_hourly`,
- resampling, imputasi, forward-fill,
- konversi unit.

Model nantinya memakai fitur asli dan melakukan join di gold/model.

## Struktur output

Struktur mengikuti bronze. Satu partisi bronze menghasilkan satu partisi
silver dengan `ingest_date` yang sama.

```
silver/usgs/continuous/parameter_code=00065/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/usgs/daily/parameter_code=00060/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/open-meteo/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/_dq/table=<table>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/_quarantine/table=<table>/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
```

`ingest_date` adalah hari UTC, sama seperti bronze. Karena itu silver tidak
membutuhkan penulisan ulang lintas partisi.

## Skema

### USGS continuous `00065`

| Kolom | Keterangan |
|---|---|
| `station_id` | site number |
| `ingest_date` | partisi UTC, `YYYY-MM-DD` |
| `time_utc` | timestamp observasi (UTC) |
| `time_local` | timestamp yang sama di `America/Los_Angeles` |
| `date_local` | tanggal lokal `YYYY-MM-DD` |
| `time_series_id`, `monitoring_location_id`, `parameter_code`, `statistic_id` | metadata sumber |
| `value` | nilai gage height (double, satuan asli) |
| `unit` | `ft` |
| `approval_status`, `qualifier`, `method_category` | metadata kualitas |
| `last_modified` | timestamp revisi USGS |

Nilai gage height negatif **dipertahankan** karena bisa valid di bawah datum;
nilai tersebut hanya dilaporkan di DQ.

### USGS daily `00060`

| Kolom | Keterangan |
|---|---|
| `station_id`, `ingest_date` | kunci partisi |
| `time_series_id`, `monitoring_location_id`, `parameter_code`, `statistic_id` | metadata sumber |
| `date_local` | tanggal water-day lokal dari USGS |
| `value` | discharge harian (double, satuan asli) |
| `unit` | `ft^3/s` |
| `approval_status`, `qualifier`, `last_modified` | metadata sumber |

Discharge negatif dianggap tidak valid untuk stasiun ini dan dikarantina.

### Open-Meteo hourly

| Kolom | Keterangan |
|---|---|
| `station_id`, `ingest_date` | kunci partisi |
| `time_utc`, `time_local`, `date_local` | waktu observasi |
| 12 variabel | `precipitation`, `rain`, `soil_moisture_0_to_7cm`, `soil_moisture_7_to_28cm`, `soil_moisture_28_to_100cm`, `temperature_2m`, `relative_humidity_2m`, `surface_pressure`, `et0_fao_evapotranspiration`, `wind_speed_10m`, `shortwave_radiation`, `cloud_cover` |
| `units_json` | `hourly_units` asli dari bronze |
| `latitude`, `longitude`, `elevation`, `timezone` | metadata lokasi |

Nilai variabel yang di luar rentang wajar di-null-kan dan dicatat di
quarantine; baris jam tetap dipertahankan agar variabel lain tetap bisa
dipakai.

## Proses per partisi

Untuk setiap `(sumber, station, ingest_date)`:

1. **Baca bronze** `part-000.json`.
2. **Flatten** payload:
   - USGS: explode `features`, ambil `properties`.
   - Open-Meteo: explode array `hourly` menjadi satu baris per jam.
3. **Deduplikasi**:
   - USGS continuous: `(station_id, parameter_code, time_raw)`, ambil
     `last_modified` terbaru.
   - USGS daily: `(station_id, parameter_code, date_raw)`, ambil
     `last_modified` terbaru.
   - Open-Meteo: `(station_id, time_utc)`.
4. **Cleaning**:
   - cast nilai ke double,
   - parse timestamp,
   - tambahkan `time_local` dan `date_local`,
   - baris dengan timestamp/nilai non-numerik/invalid → `_quarantine`.
5. **Validasi kontrak** dengan pandera.
6. **DQ checks** per `ingest_date` dan station.
7. **Tulis Parquet** dengan overwrite dinamis pada partisi yang diproses.
8. **Tulis `_dq` dan `_quarantine`**.

## Data quality checks

Hard-fail:

- schema/tipe tidak sesuai kontrak,
- duplikat natural key setelah dedupe.

Soft-fail (dicatat, data tetap ditulis):

| Check | Target |
|---|---|
| `completeness` | 96 sampel/hari USGS continuous, 24 jam/hari meteo, 1 nilai/hari USGS daily |
| `max_gap` | jarak antar sampel USGS > `SILVER_MAX_GAP_HOURS` |
| `missing_values` | nilai USGS null |
| `negative_values` | nilai USGS negatif (gage height hanya flag) |
| `provisional_values` | jumlah sampel `Provisional` (info) |
| `unit_consistency` | unit tidak sesuai (`ft`, `ft^3/s`) |
| `missing_variable_values` | jumlah nilai meteo null per variabel |
| `missing_units` | `units_json` kosong |
| `quarantined_rows` | jumlah baris yang masuk quarantine |
| `freshness` | data terbaru lebih tua dari `SILVER_FRESHNESS_DAYS` (run harian saja) |

DQ dikelompokkan per **`ingest_date`** (UTC), bukan per hari lokal. Kolom
`date_local` tetap tersedia untuk kebutuhan model.

## Menjalankan

### Build ulang image worker

Image worker berisi Java 21, PySpark, pyarrow, pandera, dan jar S3A.

```sh
docker compose build prefect-server prefect-worker
docker compose up -d
```

Pada Windows, `config/garage-init/Dockerfile` menormalkan akhir baris
`init.sh` (CRLF → LF) agar skrip bisa dijalankan di container Alpine.

### Backfill

Seluruh histori, diproses per tahun:

```sh
docker compose exec prefect-worker python -m flows.silver_flow
```

atau lewat deployment:

```sh
prefect deployment run 'silver-transform/silver-backfill'
```

Parameter opsional: `start_date`, `end_date` (UTC `YYYY-MM-DD`).

Jika backfill kehabisan memori, naikkan `--driver-memory` pada
`PYSPARK_SUBMIT_ARGS` di `config/prefect/Dockerfile`.

### Run harian

Deployment `daily-bronze-silver` menjalankan bronze lalu silver setiap
`02:30 UTC`:

```sh
prefect deployment run 'daily-bronze-silver/daily-bronze-silver'
```

Nonaktifkan deployment lama `bronze-ingest` di Prefect UI setelah deployment
baru aktif.

### Tanpa object storage

Jika `S3_ENDPOINT_URL` dikosongkan, silver membaca dan menulis ke
`BRONZE_LOCAL_DIR` (default `/prefect/data`) dengan struktur yang sama.

## Migrasi dari implementasi harian sebelumnya

Struktur harian lama tidak kompatibel. Hapus prefix berikut sebelum backfill
ulang:

- `silver/usgs_daily`
- `silver/usgs_gage_height_daily`
- `silver/meteo_daily`
- `silver/station_daily`
- `silver/_dq`
- `silver/_quarantine`

Bucket `silver` boleh dikosongkan pada lingkungan pengembangan, tetapi **jangan
menghapus root bucket** dengan `s3fs.rm("silver", recursive=True)` karena
operasi tersebut juga menghapus bucket di Garage. Hapus prefix/objek di dalam
bucket, atau jalankan ulang `garage-init` setelah bucket terhapus. Bronze tidak
perlu dihapus.

## Verifikasi

- Transformasi native, dedupe, local-time, kontrak pandera, dan quarantine
  diuji dengan data sintetis.
- Uji Docker + Garage dengan data nyata 3 stasiun, 2024-01-01..04:
  - USGS continuous: **1.150 baris** (3 × 4 × 96, dikurangi 2 sampel yang
    memang tidak tersedia di stasiun 11150500),
  - USGS daily: **12 baris** (3 × 4),
  - Open-Meteo: **288 baris** (3 × 4 × 24),
  - tidak ada `station_daily`, `meteo_daily`, `usgs_daily`, atau
    `usgs_gage_height_daily`,
  - DQ berisi 171 baris check; dua soft-fail hanya completeness 95/96 untuk
    stasiun 11150500 pada 2024-01-02 dan 2024-01-03,
  - tidak ada quarantine untuk data nyata tersebut.
- Konversi waktu lokal dicek: sampel `2024-01-01T00:00:00Z` menjadi
  `2023-12-31 16:00` lokal dan `date_local=2023-12-31`.
- Idempotensi/isolasi partisi dicek: memproses ulang 2024-01-04 tidak
  menggandakan baris dan tidak menghapus partisi 2024-01-01..03.
- Deployment Prefect `silver-backfill` untuk 2024-01-01..04 selesai
  `Completed`.

## Batasan

1. **Bronze tidak me-refresh partisi lama.** Revisi USGS untuk tanggal lama
   hanya terlihat jika partisi bronze diperbarui.
2. **Tidak ada data hilang yang diimputasi.** Baris yang tidak ada di bronze
   tidak dibuat di silver.
3. **Tidak ada join.** Model/gold harus menggabungkan USGS dan meteo
   berdasarkan `station_id` + `time_utc`/`date_local`.
4. **Gage height negatif dipertahankan** dan hanya dilaporkan di DQ.
5. **Nilai meteo invalid di-null-kan** dan dicatat di quarantine; baris jam
   tetap ada.

## Peta ke Azure

| Lokal | Azure |
|---|---|
| Garage `bronze`/`silver` | ADLS Gen2 containers |
| `s3a://...` | `abfss://...` |
| PySpark `local[2]` | Synapse Spark pool |
| `spark/hadoop-aws` | managed identity / SAS |
| `_dq`/`_quarantine` | tabel/lakehouse yang sama |
| Prefect local worker | Prefect worker di VM/Container Apps |

Transformasi di `silver/transform.py` tidak bergantung pada S3; hanya
`silver/readers.py`, `silver/writer.py`, dan `silver/spark.py` yang memuat
detail storage.

## Struktur kode

```
prefect/app/silver/
  config.py       # env, timezone, threshold, konstanta tabel
  spark.py        # SparkSession + konfigurasi S3A/Garage
  schemas.py      # schema JSON bronze, schema native, kontrak pandera
  readers.py      # baca partisi bronze -> DataFrame native
  transform.py    # dedupe, cleaning, local time, quarantine
  quality.py      # hard/soft checks per ingest_date
  writer.py       # overwrite partisi, DQ report, quarantine
  pipeline.py     # orkestrasi per tahun/ingest_date
prefect/app/flows/
  silver_flow.py  # flow silver_pipeline
  daily_pipeline.py # flow bronze -> silver
```
