# Silver Layer

## Yang dilakukan

- Deduplikasi baris.
- Membersihkan dan mengarantina data rusak.
- Menambahkan `time_local` dan `date_local` dari `time_utc`.
- Validasi kontrak data dengan pandera.
- Menjalankan data quality checks per `ingest_date`.

## Struktur output

```
silver/usgs/continuous/parameter_code=00065/station_id=<site>/ingest_date=YYYY-MM-DD/part-000.parquet
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
