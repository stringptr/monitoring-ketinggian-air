# Progress: Delta Silver

Branch `feat/delta-silver`, basis `dev` setelah merge Delta gold. Fokus:
memindahkan empat tabel silver ke Delta Lake dan menyesuaikan gold agar tetap
bisa membacanya.

## Latar belakang

Spark Delta juga gagal **membaca** tabel di Garage. S3A menerima HTTP 400 saat
menjelajah `_delta_log`, bukan 404. Karena itu silver ditulis dan dibaca dengan
`delta-rs`, termasuk saat gold membaca silver.

## Perubahan

1. `common/delta.py`: helper Delta bersama untuk bucket gold dan silver.
2. `gold/delta.py`: wrapper tipis di atas helper bersama, API tetap.
3. `silver/delta.py`: wrapper untuk bucket silver.
4. `silver/writer.py`: empat tabel silver ditulis Delta. Laporan DQ dan
   karantina tetap Parquet.
5. `silver/pipeline.py`: overwrite memakai predicate `ingest_date` per chunk.
6. `gold/readers.py`: membaca silver Delta dengan `delta-rs`, memfilter
   `station_id` dan `time_utc`, lalu mengonversi ke Spark.
7. `tests/test_silver_delta.py`: roundtrip dan predicate overwrite.
8. `tests/test_gold_delta.py` dan test gold lain tetap lulus.

## Struktur

```text
silver/delta/usgs/continuous/parameter_code=00065/
silver/delta/usgs/continuous/parameter_code=00060/
silver/delta/usgs/daily/parameter_code=00060/
silver/delta/open-meteo/
silver/_dq/...            Parquet
silver/_quarantine/...    Parquet
```

## Verifikasi

| Pemeriksaan | Hasil |
|---|---|
| `pytest` | 42 lulus |
| Silver 2026-10-20..11-10 | 6.335 + 6.335 continuous, 66 daily, 1.584 meteo, 0 gagal DQ |
| Silver 2026-11-11..13 | 864 + 864 continuous, 9 daily, 216 meteo, 0 gagal DQ |
| Gold 2026-11-05..10 | 432 baris, 432 trainable, `discharge_grain=hourly` |
| Gold DQ | 44 soft pass, 21 info pass, 0 soft fail |
| Delta history | Tabel silver dan gold punya commit history |

Jendela gold 11-05..10 sempat hanya 360 trainable karena silver berhenti di
11-10, sedangkan target h+24 membutuhkan 11-11. Setelah silver 11-11..13
ditambahkan, seluruh 432 baris trainable.

## Batasan

- Spark Delta belum bisa dipakai di Garage, baik tulis maupun baca.
- Gold harus membaca silver melalui `delta-rs`, sehingga data jendela
  dikonversi ke pandas lalu ke Spark. Untuk v0 ini kecil; untuk backfill besar
  di Azure, gunakan Spark Delta langsung.
- DQ dan karantina tetap Parquet.
- Tabel Parquet silver lama tidak dihapus.
- Partisi silver masih harian. `OPTIMIZE` atau partisi bulanan menyusul.
