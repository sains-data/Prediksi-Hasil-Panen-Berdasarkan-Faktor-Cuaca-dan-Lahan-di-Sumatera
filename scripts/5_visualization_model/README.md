# 📊 Visualization and Analysis

## 📁 Project Overview
Proyek ini berfokus pada visualisasi performa model prediksi produksi padi menggunakan data historis dan sintetis. Visualisasi ini merupakan bagian dari sistem yang dibangun di atas Apache Spark dan Hadoop untuk menganalisis data produksi padi dari berbagai provinsi di Indonesia. Model utama yang digunakan adalah **Gradient Boosted Trees (GBT)**.

---

## 📌 Key Features
- 📈 Analisis performa model prediksi
- 🔍 Evaluasi error berdasarkan provinsi, tahun, dan rentang produksi
- 🌾 Analisis korelasi fitur terhadap produksi
- 🌐 Visualisasi interaktif berbasis Plotly (HTML)
- 📄 Laporan analisis komprehensif otomatis (Markdown & Text)

---

## 📊 Visualisasi dan Analisis

### 1. **Model Performance Overview** (`05_model_performance.png`)
- **R² = 0.9593** (akurasi tinggi)
- **Mean Error**: 9.57%, **Median Error**: 7.22%
- Provinsi dengan performa terbaik: **Lampung** (7.1%)
- Provinsi dengan performa terburuk: **Bengkulu** (13.0%)
- Distribusi error terkonsentrasi <20%

### 2. **Visualisasi Interaktif**
- [`interactive_correlation_matrix.html`](./visualizations/interactive_correlation_matrix.html): Korelasi antar fitur.
- [`interactive_time_series.html`](./visualizations/interactive_time_series.html): Tren produksi padi tiap tahun per provinsi.
- [`interactive_actual_vs_predicted.html`](./visualizations/interactive_actual_vs_predicted.html): Perbandingan aktual vs prediksi secara interaktif.

---

## 📂 Struktur Output
- `visualizations/`: berisi PNG dan HTML dari visualisasi
- `reports/`: laporan analisis dalam format `.md`, `.txt`, dan `.csv`

---

## 🛠 Teknologi yang Digunakan
- **Apache Spark + PySpark** untuk pemrosesan data
- **Matplotlib, Seaborn, Plotly** untuk visualisasi
- **Sklearn** untuk evaluasi model
- **Hadoop HDFS** sebagai storage
- **Docker** (opsional untuk deployment terisolasi)

---

## 🚀 Cara Menjalankan
1. Pastikan Spark, Hadoop, dan HDFS aktif.
2. Jalankan script utama:
   ```bash
   python visualization_model.py
   ```
3. Hasil visualisasi akan disimpan di folder:
   ```
   visualizations/
   reports/
   ```

---

## 📑 Laporan Komprehensif
Laporan otomatis `comprehensive_analysis_report.md` mencakup:
- Metodologi dan arsitektur
- Evaluasi performa model
- Rekomendasi pengembangan lanjutan
- Statistik dan analisis berdasarkan provinsi, tahun, dan rentang produksi

---

## 📌 Kesimpulan
Model prediksi GBT menunjukkan performa sangat baik dengan akurasi tinggi secara konsisten di berbagai provinsi. Proyek ini bisa digunakan sebagai dasar untuk sistem perencanaan dan monitoring produksi padi skala nasional.
