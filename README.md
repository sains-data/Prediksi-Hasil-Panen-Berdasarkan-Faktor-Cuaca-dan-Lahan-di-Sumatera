# Prediksi Hasil Panen Berdasarkan Faktor Cuaca dan Lahan di Sumatera Melalui Sistem Data Lakehouse dengan Arsitektur Medallion

🔍 Proyek Sistem Prediktif Menggunakan Hadoop, Spark, dan Hive

## 📘 Deskripsi Proyek

Proyek ini membangun sistem data lakehouse berbasis Hadoop dan Spark untuk memprediksi hasil panen padi di wilayah Sumatera.

Data diperoleh dari dua sumber utama:

- Data cuaca/iklim dari BMKG
- Data hasil panen dan luas lahan dari BPS

Data dari berbagai sumber **masih terpisah** dan berada di dalam folder:

📂 `./dataset/bronze/`

Seluruh data diproses melalui arsitektur Medallion (Bronze → Silver → Gold), lalu digunakan untuk melatih model regresi dengan Apache Spark MLlib.

---

## 🧱 Arsitektur Sistem (Medallion Architecture)

<img width="960" alt="Medallion Data Pipeline 1" src="https://github.com/user-attachments/assets/86104e8f-1f31-4696-aada-86030087a1be" />

### 🟫 Bronze Layer (Raw Zone - HDFS)

- Menyimpan data mentah dari CSV
- Tidak dilakukan pembersihan
- Format: CSV
- Disimpan di: `hdfs:///bronze/bmkg/` & `hdfs:///bronze/bps/`

### 🪙 Silver Layer (Clean Zone - HDFS + Hive)

- Data dibersihkan dengan Spark SQL (drop null & duplicates)
- Format: Parquet
- Disimpan di: `hdfs:///silver/hasil_panen/`

### 🏅 Gold Layer (Curated Zone - HDFS)

- Hasil feature engineering:
  - Rata-rata suhu tahunan
  - Total curah hujan
  - Rata-rata kelembapan
  - Luas panen
- Format: Parquet
- Disimpan di: `hdfs:///gold/features/`

---

## ⚙️ Tools dan Teknologi

| Tools              | Fungsi                        |
| ------------------ | ----------------------------- |
| Hadoop (HDFS)      | Penyimpanan distributed       |
| Apache Spark       | ETL dan pelatihan model       |
| Docker Compose     | Containerisasi seluruh sistem |
| PySpark MLlib      | Model prediktif (regresi)     |
| Visual Studio Code | Lingkungan pengembangan       |

---

## 📁 Struktur Proyek

```bash
Prediksi-Hasil-Panen-Berdasarkan-Faktor-Cuaca-dan-Lahan-di-Sumatera/
├── docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.datanode
│   └── Dockerfile.namenode
├── dataset/
│   ├── bronze/
│   │   ├── bmkg/
│   │   └── bps/
│   ├── silver/
│   └── gold/
├── scripts/
│   ├── 1_data_ingestion/
│   │   ├── ingest_data.py
│   │   └── README.md   
│   ├── 2_data_processing/
│   │   ├── bronze_to_silver.py
│   │   └── README.md
│   ├── 3_feature_engineering/
│   │   ├── silver_to_gold_features.py
│   │   └── README.md
│   ├── 4_model_training/
│   │   ├── train_predict_model.py
│   │   └── README.md
│   ├── 5_evaluation/
│   │   ├── evaluate_model.py
│   │   └── README.md
│   └── 6_visualization/
│       ├── visualization_model.py
│       └── README.md
└── README.md
```

## 👯Team Kelompok 25

#### Hizkia Christovita Siahaan - 122140110

#### Raid Muhammad Naufal - 122450027

#### Izza Lutfia - 122450090

#### Dinda Nababan - 122450120
