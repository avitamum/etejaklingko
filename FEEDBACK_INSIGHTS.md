# Feedback Analysis – JakLingko Data Warehouse

## Pengenalan

File dokumentasi ini menjelaskan struktur data feedback dan insights yang dapat diambil melalui query OLAP di `olap_queries.py` (Q6-Q8).

---

## 1. Struktur Data Feedback

### Tabel Sumber: `pengguna.feedback` (PostgreSQL)

```sql
CREATE TABLE feedback (
    feedback_id    UUID                      PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaksi_id   UUID                      NOT NULL UNIQUE,   -- FK → transaksi (UNIK!)
    pengguna_id    UUID                      NOT NULL,          -- FK → pengguna
    rating         SMALLINT                  NOT NULL CHECK (rating BETWEEN 1 AND 5),
    komentar       TEXT,
    kategori       kategori_feedback_enum    NOT NULL DEFAULT 'Umum',
    created_at     TIMESTAMPTZ               NOT NULL DEFAULT NOW()
);

CREATE TYPE kategori_feedback_enum AS ENUM
  ('Umum', 'Fasilitas Busway', 'Aplikasi Jaklingko');
```

### ⚠️ **Constraint Penting**

Kolom `transaksi_id` memiliki constraint **UNIQUE**, berarti:
- **1 transaksi = maksimal 1 feedback**
- Setiap `transaksi_id` hanya bisa muncul sekali di tabel feedback
- Jika mencoba insert feedback dengan `transaksi_id` yang sudah ada → ERROR: `duplicate key value violates unique constraint`

Solusinya: Gunakan `transaksi_id` yang berbeda-beda. Jika ingin banyak feedback, tambah lebih banyak transaksi records terlebih dahulu.

### Kolom-Kolom Penting

| Kolom | Tipe | Keterangan | Contoh |
|-------|------|-----------|---------|
| `feedback_id` | UUID | Primary key unik | `f1a2b3c4-0001-...` |
| `transaksi_id` | UUID | Referensi ke transaksi (UNIQUE) | `b1c2d3e4-0001-...` |
| `pengguna_id` | UUID | Referensi ke pengguna | `a1b2c3d4-0001-...` |
| `rating` | INT (1-5) | Tingkat kepuasan (1=sangat buruk, 5=sangat baik) | `4` |
| `komentar` | TEXT | Feedback kualitatif | "Aplikasi user-friendly!" |
| `kategori` | ENUM | Kategori feedback (3 pilihan) | `'Aplikasi Jaklingko'` |
| `created_at` | TIMESTAMPTZ | Waktu feedback dibuat | `2024-06-01 08:00:00+07` |

### Integrasi ke Data Warehouse

Saat ETL, feedback di-join ke `fact_transaksi_perjalanan` dengan kolom tambahan:

```sql
ALTER TABLE fact_transaksi_perjalanan ADD COLUMN (
    rating                INT           COMMENT '1-5, NULL jika tidak ada feedback',
    komentar              TEXT,
    feedback_kategori_key INT           FOREIGN KEY → dim_feedback_kategori,
    has_feedback          TINYINT(1)    DEFAULT 0
);
```

---

## 2. Kategori Feedback & Maknanya

### A. **Umum** (Layanan Keseluruhan)

**Deskripsi:** Feedback tentang kualitas layanan transportasi secara keseluruhan.

**Aspek yang diukur:**
- Ketepatan waktu (on-time performance)
- Keramahan sopir dan kru
- Kenyamanan perjalanan
- Kepuasan umum terhadap layanan

**Contoh Komentar:**
- ⭐⭐⭐⭐⭐ "Layanan sangat memuaskan, sopir ramah dan tepat waktu!"
- ⭐⭐⭐⭐ "Pada umumnya bagus, tapi agak lama di halte terakhir"
- ⭐⭐⭐ "Layanan biasa saja, tidak ada yang istimewa"

**Insights yang dapat diambil:**
- Rata-rata rating → indikator kepuasan umum layanan
- Trend rating per bulan → performa kualitas layanan
- Perbandingan rating antar rute → identifikasi rute bermasalah

---

### B. **Fasilitas Busway** (Infrastruktur & Armada)

**Deskripsi:** Feedback tentang kualitas fisik halte, armada, dan fasilitas pendukung.

**Aspek yang diukur:**
- Kebersihan halte dan armada
- Kenyamanan kursi tunggu dan tempat duduk
- Fungsi AC, pintu, sistem pembayaran
- Adanya toilet, WiFi, parkir, lift
- Ketersediaan papan informasi

**Contoh Komentar:**
- ⭐⭐⭐⭐ "Kursi tunggu nyaman dan bersih, AC berfungsi baik"
- ⭐⭐ "Toilet di halte kotor dan tidak terawat dengan baik"
- ⭐⭐⭐ "WiFi sering putus, perlu diperbaiki"

**Insights yang dapat diambil:**
- Rating fasilitas per halte → prioritas maintenance
- Komponen mana yang paling sering komplain (toilet, AC, WiFi)
- Efektivitas program pembersihan halte berdasarkan rating trendi
- ROI penambahan fasilitas (dibanding rating sebelum/sesudah)

---

### C. **Aplikasi Jaklingko** (UX/UI Mobile App)

**Deskripsi:** Feedback tentang pengalaman pengguna aplikasi mobile JakLingko.

**Aspek yang diukur:**
- Kemudahan navigasi & booking
- Kecepatan aplikasi (lag, crash)
- Fitur real-time tracking & notifikasi
- Integrasi payment gateway (GoPay, OVO, Dana, dll)
- Kejelasan informasi rute & tarif
- Bug dan error

**Contoh Komentar:**
- ⭐⭐⭐⭐⭐ "Aplikasi sangat user-friendly, booking mudah dan cepat!"
- ⭐⭐⭐ "Aplikasi kadang crash ketika lagi rush hour"
- ⭐⭐⭐⭐ "Fitur notifikasi real-time sangat membantu, tapi boros kuota"
- ⭐ "Aplikasi error, tidak bisa bayar melalui GoPay"

**Insights yang dapat diambil:**
- Trend crash/error aplikasi → analisis dengan tim dev
- Features paling disukai vs. yang perlu perbaikan
- Payment method mana yang paling reliable
- User satisfaction terhadap update aplikasi baru

---

## 3. Query OLAP untuk Feedback (Q6-Q8)

### Q6: Rating Distribution & Kepuasan per Kategori

**Tujuan:** Mengukur kepuasan pengguna secara agregat per kategori feedback.

**SQL Konsep:**
```sql
SELECT 
  kategori_nama,
  COUNT(*) as jml_feedback,
  AVG(rating) as avg_rating,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rating) as median_rating,
  COUNT(CASE WHEN rating >= 4 THEN 1 END) * 100.0 / COUNT(*) as pct_satisfied
FROM fact_transaksi_perjalanan
WHERE has_feedback = 1
GROUP BY kategori_nama
ORDER BY avg_rating DESC;
```

**Output Contoh:**
```
+-----------------------+-------------+----------+---------------+
| kategori_nama         | jml_feedback| avg_rating| pct_satisfied |
+-----------------------+-------------+----------+---------------+
| Aplikasi Jaklingko    | 4280        | 4.15     | 82.5%         |
| Fasilitas Busway      | 3650        | 4.02     | 78.9%         |
| Umum                  | 2180        | 3.98     | 76.2%         |
+-----------------------+-------------+----------+---------------+
```

**Interpretasi:**
- **Aplikasi Jaklingko** = kategori paling puas (4.15 bintang)
- **Fasilitas Busway** = perlu improvement (4.02 bintang)
- **Umum** = terendah, mungkin ada issue dengan kualitas layanan inti

**Actionable Insights:**
- Prioritas perbaikan: Umum → Fasilitas → Aplikasi
- Target: Naikkan semua kategori ke ≥4.2 bintang
- Benchmark: Standard industri transportasi umum = 4.0+ bintang

---

### Q7: Rating Breakdown (1-5 Bintang) per Kategori

**Tujuan:** Melihat distribusi detail rating (1, 2, 3, 4, 5) per kategori.

**SQL Konsep:**
```sql
SELECT 
  kategori_nama,
  rating,
  COUNT(*) as jml_feedback
FROM fact_transaksi_perjalanan
WHERE has_feedback = 1
GROUP BY kategori_nama, rating
ORDER BY kategori_nama, rating;
```

**Output Contoh:**
```
| kategori_nama      | rating | jml_feedback |
|--------------------|--------|--------------|
| Aplikasi Jaklingko | 1      | 142          |  ← Permasalahan serius
| Aplikasi Jaklingko | 2      | 356          |  ← Cukup bermasalah
| Aplikasi Jaklingko | 3      | 743          |  ← Netral
| Aplikasi Jaklingko | 4      | 1850         |  ← Puas
| Aplikasi Jaklingko | 5      | 1189         |  ← Sangat puas
|--------------------|--------|--------------|
| Fasilitas Busway   | 1      | 198          |
| Fasilitas Busway   | 2      | 287          |
| Fasilitas Busway   | 3      | 892          |
| Fasilitas Busway   | 4      | 1523         |
| Fasilitas Busway   | 5      | 750          |
```

**Visualisasi:** Stacked bar chart dengan 5 warna (merah→hijau)

**Interpretasi:**
- Jika banyak rating 1-2 → ada masalah serius
- Jika banyak rating 5 → pengguna sangat puas
- Jika rating 3 banyak → pengguna ragu-ragu

**Actionable Insights:**
- Aplikasi Jaklingko: 1417 feedback (1-2) = ~33% ada issue → **fix immediately**
- Fasilitas Busway: 485 feedback (1-2) = ~13% ada issue → moderate priority
- Umum: perlu breakdown lebih detail

---

### Q8: Feedback per Rute & Average Rating Satisfaction

**Tujuan:** Mengidentifikasi rute mana yang paling/least puas untuk prioritas improvement.

**SQL Konsep:**
```sql
SELECT 
  rute_id,
  nama_rute,
  jenis_moda,
  COUNT(*) as jml_feedback,
  AVG(rating) as avg_rating,
  COUNT(CASE WHEN rating >= 4 THEN 1 END) * 100.0 / COUNT(*) as pct_satisfied
FROM fact_transaksi_perjalanan ft
JOIN dim_rute dr ON ft.rute_key = dr.rute_key
WHERE has_feedback = 1 AND COUNT(*) > 50  -- minimal 50 feedback
GROUP BY rute_id, nama_rute, jenis_moda
ORDER BY avg_rating DESC;
```

**Output Contoh:**
```
| rute_id | nama_rute                        | jenis_moda | jml_fdbk | avg_rating | pct_satisfied |
|---------|----------------------------------|------------|----------|------------|---------------|
| R002    | Lebak Bulus - Bundaran HI        | MRT        | 845      | 4.42       | 87.8%         |
| R001    | Blok M - Kota                    | BRT        | 623      | 4.21       | 82.7%         |
| R003    | Cibubur - Dukuh Atas             | LRT        | 456      | 4.08       | 79.4%         |
| R005    | Kampung Rambutan - Blok M        | Mikrotrans | 312      | 3.95       | 75.0%         |
| R004    | Tanah Abang - Pulo Gadung        | Bus        | 198      | 3.87       | 71.2%         |
```

**Interpretasi:**
- **MRT (R002)** = best performing rute
- **Bus (R004)** = perlu perhatian khusus
- Gap antara best (4.42) dan worst (3.87) = 0.55 → significant

**Actionable Insights:**
- Benchmark MRT ke rute lain → copy best practices
- Bus R004 → audit khusus (driver quality, armada condition, facilities)
- Target: minimal 4.0 rating untuk semua rute dalam 6 bulan

---

## 4. Data Dummy untuk Testing

File: **`feedback_dummy.csv`**

Struktur (10 rows dengan transaksi_id unik):
```csv
transaksi_id,pengguna_id,rating,komentar,kategori
b1c2d3e4-0001-0001-0001-000000000001,a1b2c3d4-0001-0001-0001-000000000001,5,Layanan sangat memuaskan sopir ramah dan tepat waktu!,Umum
b1c2d3e4-0002-0002-0002-000000000002,a1b2c3d4-0002-0002-0002-000000000002,4,Pada umumnya bagus tapi agak lama di halte terakhir,Umum
b1c2d3e4-0003-0003-0003-000000000003,a1b2c3d4-0003-0003-0003-000000000003,3,Layanan biasa saja tidak ada yang istimewa,Umum
b1c2d3e4-0004-0004-0004-000000000004,a1b2c3d4-0004-0004-0004-000000000004,4,Kursi tunggu nyaman dan bersih AC berfungsi baik,Fasilitas Busway
b1c2d3e4-0005-0005-0005-000000000005,a1b2c3d4-0005-0005-0005-000000000005,2,Toilet di halte kotor dan tidak terawat dengan baik,Fasilitas Busway
b1c2d3e4-0006-0006-0006-000000000006,a1b2c3d4-0001-0001-0001-000000000001,5,Aplikasi sangat user-friendly booking mudah dan cepat!,Aplikasi Jaklingko
b1c2d3e4-0007-0007-0007-000000000007,a1b2c3d4-0002-0002-0002-000000000002,3,Aplikasi kadang crash ketika lagi rush hour,Aplikasi Jaklingko
b1c2d3e4-0008-0008-0008-000000000008,a1b2c3d4-0003-0003-0003-000000000003,4,Fitur notifikasi real-time sangat membantu tapi boros kuota,Aplikasi Jaklingko
b1c2d3e4-0009-0009-0009-000000000009,a1b2c3d4-0004-0004-0004-000000000004,1,Aplikasi error tidak bisa bayar melalui GoPay,Aplikasi Jaklingko
b1c2d3e4-0010-0010-0010-000000000010,a1b2c3d4-0005-0005-0005-000000000005,5,Update terbaru tambah fitur lokasi real-time sangat bagus!,Aplikasi Jaklingko
```

**Catatan:** Setiap `transaksi_id` **UNIK** (primary key constraint) - satu feedback per transaksi.

Cara menggunakan:
```bash
# Ingest dummy feedback ke DWH
python ingest_ke_dwh.py feedback_dummy.csv

# Jalankan Q6-Q8 untuk melihat insights
spark-submit --packages mysql:mysql-connector-java:8.0.33 olap_queries.py
```

---

## 5. KPI & Monitoring Dashboard

### Target KPI Feedback

| KPI | Target | Alert |
|-----|--------|-------|
| Avg Rating (Overall) | ≥ 4.0 | < 3.8 |
| Pct Satisfied (≥4 bintang) | ≥ 80% | < 75% |
| Avg Rating - Aplikasi | ≥ 4.2 | < 4.0 |
| Avg Rating - Fasilitas | ≥ 4.1 | < 4.0 |
| Avg Rating - Umum | ≥ 4.0 | < 3.8 |
| Rating 1-2 (Critical Feedback) | < 10% | > 15% |

### Monitoring Frequency

- **Daily:** Rating trend by kategori (real-time dashboard)
- **Weekly:** Top complaints, low-rating routes
- **Monthly:** Trend analysis, KPI review, action plan
- **Quarterly:** Satisfaction trend vs. competitor, ROI evaluation

---

## 6. Integrasi dengan Sistem Operasional

### Feedback Loop - Rute Perbaikan

```
1. User memberikan feedback (app → PostgreSQL feedback table)
   ↓
2. ETL PySpark ekstrak & load ke DWH (daily)
   ↓
3. Query OLAP Q6-Q8 generate insights
   ↓
4. Dashboard menampilkan KPI & alerts
   ↓
5. Management review & assign action items
   ↓
6. Operasional execute improvement
   ↓
7. Monitor hasil di Q6-Q8 (metric trending)
```

### Contoh Action Items dari Insights

**Dari Q6 (Rating Kategori):**
- Aplikasi rating 3.5 → Assign dev team untuk bug fixing sprint
- Fasilitas rating 3.8 → Increase maintenance budget untuk halte

**Dari Q7 (Rating Breakdown):**
- Banyak rating 2 di Fasilitas → Survey halte mana yang dirty → prioritas cleaning
- Banyak rating 1 di Aplikasi → Check payment gateway integration issues

**Dari Q8 (Rating per Rute):**
- R004 Bus rating 3.7 → Audit driver training, armada maintenance
- R002 MRT rating 4.4 → Promote best practices dari MRT ke rute lain

---

## 7. Dokumentasi Teknis

### Schema Changes (DWH)

**Tambahan Tabel:**
```sql
CREATE TABLE dim_feedback_kategori (
    feedback_kategori_key INT PRIMARY KEY AUTO_INCREMENT,
    kategori_nama VARCHAR(50) NOT NULL UNIQUE,
    deskripsi VARCHAR(255)
);

INSERT INTO dim_feedback_kategori VALUES
  (1, 'Umum', 'Feedback umum tentang layanan secara keseluruhan'),
  (2, 'Fasilitas Busway', 'Feedback tentang fasilitas halte dan armada'),
  (3, 'Aplikasi Jaklingko', 'Feedback tentang aplikasi mobile JakLingko');
```

**Kolom Tambahan di Fact Table:**
```sql
ALTER TABLE fact_transaksi_perjalanan ADD COLUMN (
    feedback_kategori_key INT,
    rating TINYINT COMMENT '1-5',
    komentar TEXT,
    has_feedback TINYINT(1) DEFAULT 0,
    FOREIGN KEY (feedback_kategori_key) REFERENCES dim_feedback_kategori(feedback_kategori_key),
    INDEX idx_rating (rating),
    INDEX idx_has_feedback (has_feedback)
);
```

---

**Last Updated:** 2024-06-01
**Version:** 1.0
