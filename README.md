# Monitoring Ketinggian Air

Pipeline medallion di atas data USGS dan Open-Meteo: **bronze â†’ silver â†’ gold**,
dijalankan oleh Prefect, disimpan di Garage (kompatibel S3), diolah dengan
Spark, lalu disajikan melalui dashboard Streamlit.

## Silver Layer

### Yang dilakukan

- Deduplikasi baris.
- Membersihkan dan mengarantina data rusak.
- Menambahkan `time_local` dan `date_local` dari `time_utc`.
- Validasi kontrak data dengan pandera.
- Menjalankan data quality checks per `ingest_date`.

## Struktur output

```
silver/usgs/continuous/parameter_code=00065/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/usgs/continuous/parameter_code=00060/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/usgs/daily/parameter_code=00060/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/open-meteo/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/_dq/table=<table>/ingest_date=YYYY-MM-DD/
silver/_quarantine/table=<table>/station_id=<site>/ingest_date=YYYY-MM-DD/
```

## Data quality

Hard-fail: schema/tipe tidak sesuai kontrak dan duplikat natural key.

Soft-fail (dicatat di `silver/_dq`): completeness (96 sampel/hari USGS,
24 jam/hari meteo, 1 nilai/hari USGS daily), gap, missing values, nilai
negatif, provisional, konsistensi unit, quarantine, dan freshness.

Gage height negatif dipertahankan dan hanya dilaporkan; discharge negatif
dikarantina. Nilai meteo di luar range di-null-kan dan dicatat di quarantine.

## Bronze Layer

flows/pipeline.py menarik JSON mentah USGS (00065 dan 00060 continuous,
ditambah 00060 daily sebagai pembanding) dan Open-Meteo hourly, satu partisi
per hari UTC di `bronze/`. Partisi yang sudah ada dilewati, sehingga backfill
dapat dihentikan dan dilanjutkan.

## Gold Layer

gold/delta/ menyatukan silver menjadi satu baris per jam per stasiun sebagai
tabel Delta, lalu
menurunkan fitur model dan mencatat kualitas data. Detail skema, keputusan,
dan batasan ada di [GOLD.md](GOLD.md).

- gold/delta/station_hourly: dataset per jam untuk dashboard.
- gold/delta/features: 48 fitur + target y_h1, y_h6, y_h24.
- gold/delta/predictions, gold/delta/model_metrics: diisi oleh proses inference dan
  pelatihan model.
- gold/_dq: laporan kualitas data per tanggal.

Deployment: `daily-all` (02:30 UTC, bronze -> silver -> gold), `daily-gold`
dan `gold-backfill` untuk run manual. Hanya `daily-all` yang terjadwal.

## Dashboard

Streamlit membaca gold/station_hourly dan gold/predictions. Panel utama
adalah grafik prediksi; panel pendukung berisi ringkasan ketinggian air,
galat terbaru, dan hidrograf dengan presipitasi.

```sh
docker compose up -d dashboard
# http://127.0.0.1:8501
```

## Catatan pekerjaan

Status, hasil uji, keputusan, dan batasan lapisan gold serta dashboard dicatat
di [docs/progress-gold-dashboard.md](docs/progress-gold-dashboard.md).

