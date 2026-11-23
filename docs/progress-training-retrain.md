# Progress: Training dan Retrain

Branch `feat/training-retrain`, basis `dev` (`10a7d50`). Fokus: flow training
dari `gold/features`, evaluasi terhadap persistence, dan gerbang promosi.
MLflow belum diintegrasikan di branch ini.

## Isi

| File | Fungsi |
|---|---|
| `training/config.py` | Split tanggal, hyperparameter, gate, path model |
| `training/dataset.py` | Baca gold features, filter trainable dan non-partial, split kronologis |
| `training/metrics.py` | MAE, RMSE, bias, NSE, KGE, POD, FAR, CSI, gerbang promosi |
| `training/train.py` | 3 stasiun x 3 horizon, target delta, early stopping, baseline persistence |
| `training/writer.py` | Metrik ke `gold/model_metrics`, model ke `gold/models` |
| `training/pipeline.py` | Orkestrasi satu run training |
| `flows/training_flow.py` | Flow Prefect `training-pipeline` |
| `prefect.yaml` | Deployment `train-models`, jadwal mingguan tidak aktif |

## Keputusan

- Training hanya memakai baris `is_trainable` dan `~is_partial`.
- Split: train 2015-10-01..2023-09-30, validasi WY2024, test WY2025..WY2026.
- Target delta `y_h - stage_now`, objective L1, early stopping pada validasi.
- Baseline persistence memprediksi `stage_now`.
- Gerbang promosi: MAE validasi kandidat minimal 2 persen lebih baik dari
  persistence.
- Ambang event dihitung dari persentil 95 target level di data latih.
- Model dan metrik disimpan di gold, bukan di `data/models`.
- Jadwal retrain mingguan, belum diaktifkan sampai backfill historis selesai.

## Verifikasi

- 9 test: metrik regresi, metrik event, gerbang promosi, filter split,
  pemilihan kolom tanpa duplikat, tracking opsional, dan smoke training
  9 model dengan data sintetis.
- Test dijalankan di image `monitoring-training` yang memuat `scikit-learn`
  dan `mlflow`.

## MLflow smoke

- Server lokal: `http://127.0.0.1:5050`, experiment `hydrology-smoke`.
- Dua run tercatat. Run terakhir `44b2461e510d471c8f4b5b1460110a5c`.
- 9 model terdaftar: `hydrology-<site>-h<horizon>`.
- Lineage run terakhir: `dataset_version=g1-f3c00ecc`, `discharge_grain=hourly`.
- Smoke memakai `TRAIN_INCLUDE_PARTIAL=true` dan split 3/2/1 hari, hanya untuk
  membuktikan jalur training, logging, dan registry. Bukan model laporan.
- Server membutuhkan `--allowed-hosts` agar Host `mlflow:5000` diterima.

## Yang belum

- Uji pada gold hasil backfill penuh di Azure.
- Serving membaca model dari MLflow registry, bukan model notebook.
- Mengaktifkan jadwal retrain setelah run pertama tervalidasi.
- Rebase ke `dev` setelah PR discharge continuous merge.
- Migrasi MLflow ke Azure ML atau server terkelola.
