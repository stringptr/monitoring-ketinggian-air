# Silver Layer — Monitoring Ketinggian Air

Lapisan **silver** di atas bronze USGS + Open-Meteo. Silver mempertahankan
resolusi data asli; tidak melakukan agregasi maupun join.

## Yang dilakukan

- Deduplikasi baris.
- Membersihkan dan mengarantina data rusak.
- Menambahkan `time_local` dan `date_local` dari `time_utc`.
- Validasi kontrak data dengan pandera.
- Menjalankan data quality checks per `ingest_date`.

Yang **tidak** dilakukan: agregasi, resampling, imputasi, konversi unit, dan
join USGS + Open-Meteo. Tabel `station_daily` tidak dibuat; join dilakukan
nanti di gold/model.

## Struktur output

```
silver/usgs/continuous/parameter_code=00065/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/usgs/daily/parameter_code=00060/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/open-meteo/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
silver/_dq/table=<table>/ingest_date=YYYY-MM-DD/
silver/_quarantine/table=<table>/station_id=<site>/ingest_date=YYYY-MM-DD/
```

Satu partisi bronze menghasilkan satu partisi silver dengan `ingest_date`
yang sama. Resolusi tetap: 15 menit (USGS `00065`), harian native (USGS
`00060`), dan per jam (Open-Meteo).

## Menjalankan

Build ulang image worker (Java 21, PySpark, pyarrow, pandera, jar S3A):

```sh
docker compose build prefect-server prefect-worker
docker compose up -d
```

Backfill seluruh histori (diproses per tahun):

```sh
docker compose exec prefect-worker python -m flows.silver_flow
```

Atau lewat deployment:

```sh
prefect deployment run 'silver-transform/silver-backfill'
```

Run harian (bronze → silver, 02:30 UTC):

```sh
prefect deployment run 'daily-bronze-silver/daily-bronze-silver'
```

Jika `S3_ENDPOINT_URL` kosong, silver memakai `BRONZE_LOCAL_DIR`
(default `/prefect/data`) dengan struktur yang sama.

## Data quality

Hard-fail: schema/tipe tidak sesuai kontrak dan duplikat natural key.

Soft-fail (dicatat di `silver/_dq`): completeness (96 sampel/hari USGS,
24 jam/hari meteo, 1 nilai/hari USGS daily), gap, missing values, nilai
negatif, provisional, konsistensi unit, quarantine, dan freshness.

Gage height negatif dipertahankan dan hanya dilaporkan; discharge negatif
dikarantina. Nilai meteo di luar range di-null-kan dan dicatat di quarantine.

## Catatan

- Bronze tidak diubah dan tidak me-refresh partisi lama.
- Data yang tidak ada di bronze tidak dibuat di silver.
- Jangan menghapus root bucket `silver` dengan `s3fs.rm("silver", recursive=True)`
  karena ikut menghapus bucket Garage. Hapus prefix/objek di dalamnya atau
  jalankan ulang `garage-init`.
- Verifikasi data nyata 2024-01-01..04: 1.150 baris USGS continuous, 12 baris
  USGS daily, 288 baris Open-Meteo; tidak ada tabel agregat; DQ hanya
  soft-fail completeness 95/96 untuk stasiun 11150500 pada dua hari.
- Full backfill 2015-10-01 s.d. sekarang belum dijalankan.

## Struktur kode

```
prefect/app/silver/
  config.py       # env, timezone, threshold, konstanta tabel
  spark.py        # SparkSession + konfigurasi S3A/Garage
  schemas.py      # schema bronze, schema native, kontrak pandera
  readers.py      # baca partisi bronze -> DataFrame native
  transform.py    # dedupe, cleaning, local time, quarantine
  quality.py      # hard/soft checks per ingest_date
  writer.py       # overwrite partisi, DQ report, quarantine
  pipeline.py     # orkestrasi per tahun/ingest_date
prefect/app/flows/
  silver_flow.py  # flow silver_pipeline
  daily_pipeline.py # flow bronze -> silver
```
