
# Prediksi Hasil Panen Berdasarkan Faktor Cuaca dan Lahan di Sumatera Melalui Sistem Data Lakehouse dengan Arsitektur Medallion
🔍 Proyek Sistem Prediktif Menggunakan Hadoop, Spark, dan Hive

## 📘 Deskripsi Proyek
Proyek ini membangun sistem data lakehouse berbasis Hadoop dan Spark untuk memprediksi hasil panen padi di wilayah Sumatera.  
Data diperoleh dari dua sumber utama:

- Prakiraan cuaca harian dari BMKG  
- Data hasil panen dan luas lahan dari BPS  

Kedua data telah digabung menjadi 1 file:  
📂 `Data_Tanaman_Padi_Sumatera.csv`

Seluruh data diproses melalui arsitektur Medallion (Bronze → Silver → Gold), lalu digunakan untuk melatih model regresi dengan Apache Spark MLlib.

---

## 🧱 Arsitektur Sistem (Medallion Architecture)

![Medallion Data Pipeline](https://github.com/user-attachments/assets/a6773297-84d5-41c3-bed7-1c83a986d4bd)
### 🟫 Bronze Layer (Raw Zone - HDFS)
- Menyimpan data mentah dari CSV
- Tidak dilakukan pembersihan
- Format: CSV
- Disimpan di: `hdfs:///bronze/hasil_panen/`

### 🪙 Silver Layer (Clean Zone - HDFS + Hive)
- Data dibersihkan dengan Spark SQL (drop null & duplicates)
- Format: Parquet
- Disimpan di: `hdfs:///silver/hasil_panen/`
- Terdaftar sebagai tabel eksternal Hive

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

| Tools             | Fungsi                                      |
|-------------------|---------------------------------------------|
| Hadoop (HDFS)     | Penyimpanan distributed                     |
| Apache Spark      | ETL dan pelatihan model                     |
| Apache Hive       | Eksplorasi SQL data                         |
| Docker Compose    | Containerisasi seluruh sistem               |
| PySpark MLlib     | Model prediktif (regresi)                   |
| Visual Studio Code| Lingkungan pengembangan                     |

---

## 📁 Struktur Proyek

```bash
proyek-panen/
├── docker/
│   └── docker-compose.yml
├── scripts/
│   ├── ingest_data.py
│   ├── bronze_to_silver.py
│   ├── register_to_hive.py
│   ├── silver_to_gold_features.py
│   ├── train_predict_model.py
│   └── evaluate_model.py
├── data-lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── Data_Tanaman_Padi_Sumatera.csv
```

## 👯Team Kelompok 25
#### Hizkia Christovita Siahaan - 122140110
#### Raid Muhammad Naufal - 122450027
#### Izza Lutfia - 122450090
#### Dinda Nababan - 122450120

