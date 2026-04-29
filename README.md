# JakLingko – Data Pipeline End-to-End
## Laporan UTS Data Engineering

---

## Arsitektur Keseluruhan

```
╔══════════════════════════════════════════════════════════════════════╗
║                    SUMBER DATA OPERASIONAL                           ║
║                                                                      ║
║  ┌──────────────────────────┐    ┌──────────────────────────────┐   ║
║  │  SUMBER 1: MySQL         │    │  SUMBER 2: PostgreSQL         │   ║
║  │  jaklingko_mysql         │    │  jaklingko_pg                 │   ║
║  │                          │    │                               │   ║
║  │  • rute                  │    │  • pengguna                   │   ║
║  │  • halte                 │    │  • transaksi                  │   ║
║  │  • armada                │    │  • feedback                   │   ║
║  └───────────┬──────────────┘    └─────────────┬─────────────────┘   ║
╚══════════════╪══════════════════════════════════╪════════════════════╝
               │                                  │
      Ingest CSV ke sumber              Ingest CSV ke sumber
      (ingest_sumber_csv.py)            (ingest_sumber_csv.py)
               │                                  │
               └──────────────┬───────────────────┘
                              │
               ┌──────────────▼──────────────┐
               │     ETL – PySpark            │
               │     etl_pyspark.py           │
               │                              │
               │  EXTRACT  (JDBC kedua DB)    │
               │  TRANSFORM                   │
               │   ├─ cleaning & dedup        │
               │   ├─ kolom turunan           │
               │   ├─ join lintas sumber      │
               │   └─ generate dim_waktu      │
               │  LOAD (JDBC → MySQL DWH)     │
               └──────────────┬──────────────┘
                              │
                              │   ◄── JUGA bisa diisi dari:
                              │       CSV / JSON / Excel
                              │       (ingest_ke_dwh.py)
                              │
╔═════════════════════════════▼══════════════════════════════════════╗
║         DATA WAREHOUSE: jaklingko_dwh (MySQL Workbench)            ║
║                                                                    ║
║   dim_waktu      dim_halte     dim_rute      dim_pengguna          ║
║       └──────────────┴────────────┴──────────────┘                 ║
║                              │                                     ║
║              fact_transaksi_perjalanan                             ║
║                    (+ etl_log)                                     ║
╚════════════════════════════════════════════════════════════════════╝
                              │
               ┌──────────────▼──────────────┐
               │    OLAP & Visualisasi        │
               │    olap_queries.py           │
               │                              │
               │  Q1: Pendapatan/bln (Rollup) │
               │  Q2: Per Rute/Moda (Slice)   │
               │  Q3: Jam Sibuk (Dice)        │
               │  Q4: Metode Bayar (Dice)     │
               │  Q5: Top Halte (Ranking)     │
               └──────────────────────────────┘
```

---

## Struktur File

```
jaklingko_v2/
├── 01_database_ddl/
│   ├── mysql_sumber.sql           ← DDL jaklingko_mysql (Sumber 1)
│   └── postgresql_sumber.sql     ← DDL jaklingko_pg   (Sumber 2)
├── 02_csv_ingest/
│   └── ingest_sumber_csv.py      ← Ingest CSV ke DB operasional
├── 03_data_warehouse/
│   └── mysql_dwh_star_schema.sql ← DDL jaklingko_dwh (MySQL DWH)
├── 04_etl_pyspark/
│   └── etl_pyspark.py            ← ETL PySpark (extract→transform→load)
├── 05_ingest_ke_dwh/
│   └── ingest_ke_dwh.py          ← Ingest CSV/JSON/Excel langsung ke DWH
├── 06_olap_queries/
│   └── olap_queries.py           ← Query OLAP + visualisasi grafik
└── README.md
```

---

## Bagian 1: Database Operasional (2 Sumber)

### Sumber 1 – MySQL (`jaklingko_mysql`)

Menyimpan data **master/referensi** yang jarang berubah.

| Tabel | Isi | Kolom Kunci |
|-------|-----|-------------|
| `rute` | Daftar rute layanan | rute_id, jenis_moda (ENUM), tarif_dasar |
| `halte` | Halte/stasiun per rute | halte_id, rute_id (FK), koordinat GPS |
| `armada` | Kendaraan per rute | armada_id, kapasitas, kondisi |

**Alasan MySQL:** Cocok untuk data master yang bersifat referensial. Tipe `ENUM` pada `jenis_moda` memastikan hanya nilai valid yang masuk (BRT/MRT/LRT/Bus/Mikrotrans).

### Sumber 2 – PostgreSQL (`jaklingko_pg`)

Menyimpan data **transaksional** dengan volume tinggi.

| Tabel | Isi | Kolom Kunci |
|-------|-----|-------------|
| `pengguna` | Akun pengguna aplikasi | UUID, email, saldo_jakcard |
| `transaksi` | Setiap perjalanan tap-in/out | waktu_tap_in/out, total_bayar, metode_bayar |
| `feedback` | Rating & komentar pasca perjalanan | rating (1–5), transaksi_id (FK), kategori (ENUM) |

**Tabel Feedback – Kategori ENUM:**

Kolom `kategori` mempunyai 3 pilihan:
- **`Umum`** – Feedback umum tentang layanan secara keseluruhan
- **`Fasilitas Busway`** – Feedback tentang kualitas halte, toilet, fasilitas di armada
- **`Aplikasi Jaklingko`** – Feedback tentang user experience aplikasi mobile

Setiap transaksi hanya dapat punya **1 feedback** (UNIQUE constraint pada `transaksi_id`), tapi pengguna yang sama bisa memberikan feedback untuk beberapa transaksi berbeda.

**Contoh Data Feedback:**
| transaksi_id | pengguna_id | rating | komentar | kategori |
|---|---|---|---|---|
| b1c2d3e4-0001... | a1b2c3d4-0001... | 5 | Layanan sangat memuaskan, sopir ramah! | Umum |
| b1c2d3e4-0004... | a1b2c3d4-0004... | 4 | Kursi tunggu nyaman dan bersih | Fasilitas Busway |
| b1c2d3e4-0005... | a1b2c3d4-0005... | 1 | Aplikasi error, tidak bisa bayar GoPay | Aplikasi Jaklingko |

**Alasan PostgreSQL:** Mendukung UUID native, *generated column* (`durasi_menit` dihitung otomatis dari selisih waktu), dan tipe custom ENUM via `CREATE TYPE`.

---

## Bagian 2: Ingest CSV ke Database Operasional

Script `ingest_sumber_csv.py` memindahkan data dari file CSV ke masing-masing database sumber.

**Urutan ingest wajib mengikuti dependensi FK:**
`rute` → `halte` → `armada` (MySQL), lalu `pengguna` → `transaksi` (PostgreSQL)

**Langkah proses:**
1. Baca CSV dengan `pd.read_csv()` + tipe eksplisit
2. Validasi kolom wajib dan drop duplikat
3. Strip whitespace semua kolom string
4. Load via `df.to_sql()` dengan batch INSERT (`method="multi"`)

---

## Bagian 3: Data Warehouse – Star Schema (MySQL `jaklingko_dwh`)

DWH menggunakan **database MySQL terpisah** (`jaklingko_dwh`) di MySQL Workbench yang sama, agar mudah diakses dan dikelola tanpa campur aduk dengan data sumber.

### Komponen Star Schema

**Fact Table: `fact_transaksi_perjalanan`**
- Grain: 1 baris = 1 perjalanan
- Measures (additive): `total_bayar`, `diskon`, `tarif`, `durasi_menit`, `jarak_tempuh_km`
- Degenerate dimension: `transaksi_id`, `armada_id`
- Kolom metadata ETL: `sumber_data` (postgresql/csv/json/excel), `etl_batch_id`

**Tabel Dimensi:**

| Dimensi | Kunci | Atribut Penting |
|---------|-------|----------------|
| `dim_waktu` | waktu_key | tahun, bulan, nama_hari, is_weekend |
| `dim_halte` | halte_key | nama_halte, kota, wilayah, fasilitas |
| `dim_rute` | rute_key | jenis_moda, jarak_km, tarif_dasar |
| `dim_pengguna` | pengguna_key | kelompok_usia, segment_pengguna, wilayah |

Semua dimensi mendukung **SCD Type 2** (kolom `valid_dari`, `valid_sampai`, `is_current`) untuk melacak perubahan historis.

**Tabel Tambahan: `etl_log`** — mencatat setiap eksekusi batch ETL (sumber, jumlah baris, status, timestamp).

---

## Bagian 4: ETL PySpark

### Alur Pipeline (`etl_pyspark.py`)

**EXTRACT**
- Baca `rute`, `halte` dari MySQL via JDBC
- Baca `pengguna`, `transaksi` dari PostgreSQL via JDBC
- Transaksi menggunakan *pushdown query* agar filter dilakukan di sisi DB (hemat bandwidth)

**TRANSFORM**
| Fungsi | Tugas |
|--------|-------|
| `bersihkan()` | Trim whitespace kolom string |
| `tangani_missing()` | Drop baris tanpa FK kritis; fillna kolom opsional |
| `deduplikasi()` | Simpan baris terbaru (Window.partitionBy + row_number) |
| `kolom_turunan_transaksi()` | Tambah jam_tap_in, sesi_hari, durasi_menit, is_sukses |
| `kolom_turunan_pengguna()` | Tambah kelompok_usia, segment_pengguna, wilayah |
| `perkaya_halte()` | Tambah kota dan wilayah |
| `buat_dim_waktu()` | Generate date spine 365 hari tanpa butuh DB |
| `bangun_fact()` | Join lintas MySQL–PostgreSQL → fact table |

**LOAD → `jaklingko_dwh` (MySQL)**
- Dimensi dimuat mode `overwrite` (full refresh)
- Fact dimuat mode `append` (incremental)

---

## Bagian 5: Ingest Format Lain Langsung ke DWH

Script `ingest_ke_dwh.py` memungkinkan data baru masuk **langsung ke DWH** tanpa harus melalui database operasional. Mendukung tiga format:

| Format | Fungsi Baca | Keterangan |
|--------|-------------|------------|
| CSV    | `pd.read_csv()` | Encoding UTF-8, separator koma |
| JSON   | `pd.read_json()` | Array of objects atau JSON Lines |
| Excel  | `pd.read_excel()` | Sheet pertama, header baris 1 |

**Alur Proses:**
1. **Baca file** sesuai ekstensi (auto-detect)
2. **Validasi** – cek kolom wajib, enum status & metode_bayar, drop duplikat
3. **Lookup dimensi** – ambil surrogate key dari tabel DWH yang sudah ada
4. **Transform** – join data dengan surrogate key, hitung kolom turunan
5. **Cek duplikat DWH** – skip `transaksi_id` yang sudah ada
6. **Load** ke `fact_transaksi_perjalanan` dengan label `sumber_data = 'csv'/'json'/'excel'`

**Cara Menjalankan:**
```bash
# Ingest satu file
python ingest_ke_dwh.py data/ingest/transaksi_baru.csv
python ingest_ke_dwh.py data/ingest/transaksi_baru.json
python ingest_ke_dwh.py data/ingest/transaksi_excel.xlsx

# Ingest semua file dalam folder
python ingest_ke_dwh.py data/ingest/
```

---

## Bagian 6: Query OLAP

### OLAP Operasional (Q1-Q5)

| Query | Operasi OLAP | Pertanyaan Bisnis |
|-------|---------|------------------|
| Q1 | Roll-up (Tahun→Bulan) | Tren pendapatan & volume transaksi bulanan 2024 |
| Q2 | Slice (status=Sukses) | Rute & moda transportasi paling menguntungkan |
| Q3 | Dice (hari kerja, BRT+MRT) | Jam puncak penumpang (peak hour analysis) |
| Q4 | Slice+Dice (Juni, Jabodetabek) | Preferensi metode bayar per segmen pengguna |
| Q5 | Ranking (RANK OVER) | Top-10 halte tersibuk berdasarkan penumpang naik |

### OLAP Feedback & Kepuasan Pengguna (Q6-Q8)

Tabel `feedback` di PostgreSQL terdapat 3 kategori ENUM: **Umum**, **Fasilitas Busway**, **Aplikasi Jaklingko**

Data dari feedback di-integrate ke fact table saat ETL, dengan kolom tambahan:
- `rating` (1-5) – tingkat kepuasan pengguna
- `komentar` – masukan kualitatif
- `feedback_kategori_key` – referensi ke `dim_feedback_kategori`
- `has_feedback` (flag) – menandai transaksi yang punya feedback

| Query | Operasi OLAP | Insights |
|-------|---------|----------|
| **Q6** | **Slice** (has_feedback=1) + **Group By kategori** | Rata-rata rating & % kepuasan (≥4 bintang) per kategori feedback |
| **Q7** | **Pivot rating** (1-5) per kategori | Breakdown detail distribusi bintang per kategori → identifikasi pain points |
| **Q8** | **Ranking rute** by avg_rating + **Dice** (feedback>50) | Kepuasan pengguna per rute → prioritas improvement |

**Format Output Q6-Q8:**
- Q6: Tabel kepuasan agregat + 2 visualisasi (bar chart rating, % satisfied)
- Q7: Stacked bar chart 5-bintang per kategori
- Q8: Rute ranking by satisfaction + detail % pengguna puas

**File Data Dummy:**
- `feedback_dummy.csv` – 10 baris feedback dengan 3 kategori untuk testing

---

## Cara Menjalankan (Urutan Lengkap)

```bash
# 1. Buat tabel database sumber
mysql < 01_database_ddl/mysql_sumber.sql
psql  < 01_database_ddl/postgresql_sumber.sql

# 2. Buat tabel DWH di MySQL Workbench
mysql < 03_data_warehouse/mysql_dwh_star_schema.sql

# 3. (Opsional) Ingest data dari CSV ke DB sumber
python 02_csv_ingest/ingest_sumber_csv.py

# 4. Jalankan ETL PySpark (sumber → DWH)
spark-submit \
  --packages mysql:mysql-connector-java:8.0.33,\
             org.postgresql:postgresql:42.6.0 \
  04_etl_pyspark/etl_pyspark.py

# 5. (Opsional) Ingest file tambahan langsung ke DWH
python 05_ingest_ke_dwh/ingest_ke_dwh.py data/ingest/transaksi_baru.csv
python 05_ingest_ke_dwh/ingest_ke_dwh.py data/ingest/transaksi_tambahan.json
python 05_ingest_ke_dwh/ingest_ke_dwh.py data/ingest/transaksi_excel.xlsx

# 6. Jalankan OLAP + buat grafik
spark-submit \
  --packages mysql:mysql-connector-java:8.0.33 \
  06_olap_queries/olap_queries.py
```

---

## Dependensi

```bash
pip install pandas sqlalchemy pymysql psycopg2-binary openpyxl matplotlib pyspark
```
