# Data Ingestion - Bronze Layer

## Overview

Tahap data ingestion adalah proses pertama dalam pipeline data lakehouse yang bertujuan untuk memindahkan data mentah dari local storage ke HDFS (Hadoop Distributed File System) sebagai bronze layer.

## Implementasi

### 1. Script Ingest Data (`ingest_data.sh`)

Script bash ini bertanggung jawab untuk mengupload file CSV dataset ke HDFS dengan struktur direktori yang terorganisir.

```bash
#!/bin/bash

DATASET_DIR="/tmp/bronze"
HDFS_DIR="/bronze"
```

#### Konfigurasi

- **DATASET_DIR**: Direktori sumber data di container (`/tmp/bronze`)
- **HDFS_DIR**: Direktori tujuan di HDFS (`/bronze`)

#### Fungsi Utama

##### 1. Validasi Dataset

```bash
if [ !-d"$DATASET_DIR/bmkg" ] || [ !-d"$DATASET_DIR/bps" ]; then
    echo"Error: Expected subdirectories 'bmkg' and 'bps' not found"
    exit1
fi
```

- Memverifikasi struktur direktori dataset
- Memastikan folder `bmkg` dan `bps` tersedia

##### 2. Pembuatan Direktori HDFS

```bash
hdfsdfs-mkdir-p$HDFS_DIR/bmkg$HDFS_DIR/bps
```

- Membuat struktur direktori di HDFS
- Menggunakan flag `-p` untuk membuat parent directories

##### 3. Processing File CSV

```bash
forcsv_filein $(find$DATASET_DIR-name"*.csv"); do
    total=$((total+1))
    filename=$(basename"$csv_file")
    rel_path=${csv_file#$DATASET_DIR/}
    hdfs_path="$HDFS_DIR/$rel_path"  

    ifhdfsdfs-put-f"$csv_file""$hdfs_path"2>/dev/null; then
        echo"  SUCCESS: Uploaded to HDFS"
        success=$((success+1))
    else
        echo"  ERROR: Failed to upload"
    fi
done

```

**Langkah-langkah processing:**

1. **File Discovery**: Menggunakan `find` untuk mencari semua file `.csv`
2. **Path Calculation**: Menghitung relative path untuk struktur HDFS
3. **Upload Process**: Menggunakan `hdfs dfs -put -f` untuk upload
4. **Error Handling**: Menangani kegagalan upload dengan logging

##### 4. Summary dan Verifikasi

```bash
echo"SUMMARY:"
echo"  Total files: $total"
echo"  Successful: $success"
echo"  Failed: $((total - success))"

hdfsdfs-ls-R$HDFS_DIR

```

### 2. Cara Penggunaan

#### Persiapan

```bash
# 1. Copy dataset ke container (pada root)
docker cp dataset/bronze namenode:/tmp/bronze/

# 2. Copy script ke container
docker cp scripts/1_ingest_data/ingest_data.sh namenode:/tmp/ingest_data.sh
```

#### Eksekusi

```bash
# 3. Jalankan script
docker exec -it namenode bash /tmp/ingest_data.sh
```

#### Verifikasi

```bash
# 4. Cek hasil di HDFS
docker exec -it namenode hdfs dfs -ls -R /bronze
```

### 3. Struktur Data Hasil

Setelah ingestion berhasil, struktur data di HDFS akan menjadi:

```
/bronze/
├── bmkg/
│   ├── aceh.csv
│   ├── babel.csv
│   ├── bengkulu.csv
│   ├── jambi.csv
│   ├── kepri.csv
│   ├── lampung.csv
│   ├── riau.csv
│   ├── sumbar.csv
│   ├── sumsel.csv
│   └── sumut.csv
└── bps/
    └── Luas_Panen_Produksi_dan_Produktivitas_Padi_2023.csv

```

### 4. Error Handling

Script dilengkapi dengan error handling untuk:

- **Directory validation**: Memastikan direktori dataset exist
- **Upload failures**: Menangani kegagalan upload individual file
- **Permission issues**: Menggunakan appropriate user privileges
- **Progress tracking**: Menampilkan progress dan summary

### 5. Monitoring

Output script memberikan informasi:

- Total file yang diproses
- Jumlah file berhasil diupload
- Jumlah file yang gagal
- Listing final structure di HDFS

### 6. Troubleshooting

#### Common Issues:

1. **Permission Denied**: Gunakan `docker exec -u root`
2. **Directory Not Found**: Pastikan dataset sudah di-copy ke container
3. **HDFS Connection**: Pastikan namenode container running

#### Debug Commands:

```bash
# Cek status HDFS
docker exec -it namenode hdfs dfs admin -report

# Cek file permissions
docker exec -it namenode ls -la /tmp/bronze/

# Manual upload test
docker exec -it namenode hdfs dfs -put /tmp/bronze/bmkg/aceh.csv /test.csv
```

---

## Next Steps

Setelah data ingestion selesai, data siap untuk tahap berikutnya dalam pipeline:

### 2. **Staging & Cleaning** (`bronze_to_silver.py`)

- **Data Cleaning**: Menghapus duplikasi dan data tidak valid
- **Data Standardization**: Normalisasi format tanggal, angka, dan teks
- **Schema Validation**: Memastikan konsistensi struktur data

### 2. **Hive Metastore Registration** (`register_to_hive.py`)

- **Table Registration**: Mendaftarkan tabel silver ke Hive Metastore
- **Schema Management**: Mengelola metadata dan schema evolution
- **Partition Strategy**: Implementasi partitioning untuk optimasi query
- **Data Catalog**: Membuat catalog data untuk discovery

### 3. **Feature Engineering** (`silver_to_gold_features.py`)

- **Feature Creation**: Membuat fitur baru dari data cuaca dan lahan
- **Aggregation**: Menghitung statistik agregat (rata-rata, total, trend)
- **Time Series Features**: Ekstraksi seasonal patterns dan trends
- **Gold Layer**: Pembuatan data marts untuk analytics dan ML

### 4. **Machine Learning Pipeline** (`train_predict_model.py`)

- **Model Training**: Melatih model prediksi hasil panen
- **Feature Selection**: Memilih fitur terbaik untuk prediksi
- **Cross Validation**: Validasi performa model
- **Model Persistence**: Menyimpan model terlatih

### 5. **Model Evaluation** (`evaluate_model.py`)

- **Performance Metrics**: Menghitung MAE, RMSE, R²
- **Prediction Analysis**: Analisis akurasi prediksi
- **Model Comparison**: Perbandingan berbagai algoritma
- **Reporting**: Generate laporan evaluasi model

---

**Note**: Script ini adalah implementasi bronze layer dalam arsitektur Data Lakehouse menggunakan Hadoop ecosystem.
