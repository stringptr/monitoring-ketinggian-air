# Progress: Discharge Continuous

Branch `feat/discharge-continuous`, basis `dev` setelah merge pipeline polish
(`10a7d50`). Fokus: mengambil `00060` pada resolusi 15 menit dan memakainya
sebagai fitur debit, sementara `00060` daily tetap disimpan sebagai pembanding.

## Perubahan

1. **Bronze.** `USGS_PARAMETERS` sekarang menarik `00065` continuous, `00060`
   continuous, dan `00060` daily.
2. **Silver.** Tabel baru `usgs_continuous_00060`. Pipeline continuous memakai
   satu spesifikasi per parameter, bukan menyalin blok `00065`.
3. **DQ.** Pemeriksaan continuous menerima nama tabel dan satuan yang
   diharapkan. `00060` memakai `ft^3/s`, `00065` memakai `ft`.
4. **Gold.** `GOLD_DISCHARGE_SOURCE=auto` memilih debit per jam.
   `discharge_grain` menjadi `hourly`, sedangkan `discharge_daily_cfs` tetap
   tersedia untuk QC.
5. **Versi dataset.** Sumber debit masuk ke hash `dataset_version`. Hourly dan
   daily menghasilkan versi berbeda karena nilai `discharge_lag_*` berbeda.
6. **Test.** Spesifikasi continuous, prefix per parameter, satuan `ft^3/s`,
   dan penolakan satuan yang salah.

## Hasil verifikasi

| Pemeriksaan | Hasil |
|---|---|
| `pytest tests -q` | 30 lulus |
| Bronze `00060` continuous | 10 partisi per stasiun untuk 2026-11-01..10 |
| Silver | 2.880 baris `usgs_continuous_00060`, 0 gagal DQ |
| Gold 2026-11-05..10 | 432 baris, 432 trainable (100 persen), `discharge_grain=hourly` |
| `discharge_model_cfs` | Sama dengan `discharge_mean_cfs` untuk seluruh baris |
| Paritas sampel | Debit Salinas 2026-11-05 pukul 00:00 sama dengan notebook, 65,8 cfs |
| Versi dataset | `g1-f3c00ecc` (hourly), berbeda dari versi daily |

Perbedaan trainable dibanding versi daily terlihat jelas: dengan daily, jendela
6 hari hanya menghasilkan 360 baris trainable karena debit hari terakhir belum
tersedia. Dengan hourly, seluruh 432 baris trainable.

## Yang belum selesai

- Backfill historis `00060` continuous untuk seluruh periode. Saat ini baru
  sepuluh hari untuk pengujian.
- Backfill silver dan gold 2015-2026 sebelum training dari gold.
- Training dan retrain terjadwal, lalu MLflow.
- Regenerasi prediksi setelah gold memakai debit per jam.
- Migrasi Delta Lake untuk gold dan silver.

## Catatan

`00060` daily tidak dihapus. Nilainya dipakai sebagai pembanding dan QC, bukan
input fitur. Migrasi ke hourly tidak mengubah skema gold.
