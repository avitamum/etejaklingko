"""
============================================================
 JAKLINGKO DATA PIPELINE - UTS
 FILE  : etl_pyspark.py
 DESC  : ETL lengkap PySpark
         EXTRACT  -> jaklingko_mysql (MySQL) + jaklingko_pg (PostgreSQL)
         TRANSFORM-> Join, cleaning, dedup, kolom turunan
         LOAD     -> jaklingko_dwh (MySQL Workbench)

 Jalankan:
   spark-submit \
     --jars C:\\mysql-connector-j-9.6.0.jar,C:\\jdbc\\postgresql-42.7.9.jar \
     etl_pyspark.py
============================================================

DAFTAR BUG YANG DIPERBAIKI:
  BUG-1 : cast("tinyint") tidak valid di PySpark → ganti ByteType()
  BUG-2 : Double-join DataFrame dim_halte yang sama → rename kolom
           dengan prefix hn_ / ht_ sebelum join agar tidak ambigu
  BUG-3 : dim_feedback_kategori dibaca dari DWH sebelum ada isinya
           → selalu bangun dari data feedback + default list agar konsisten
  BUG-4 : load() tidak menulis dim_feedback_kategori ke DWH
           → tambahkan tulis_jdbc dim_feedback_kategori sebelum fact
  BUG-5 : deduplikasi(df_transaksi, ..., "created_at") → crash jika
           kolom created_at tidak ada di tabel transaksi
           → fallback ke waktu_tap_in jika created_at tidak tersedia
  BUG-6 : feedback_kategori_key selalu NULL di fact table
           ROOT CAUSE: join dim_feedback_kategori dilakukan di dalam
           chained-join utama menggunakan col("fb.kategori"). PySpark
           tidak bisa me-resolve alias "fb" setelah multiple chained
           joins, sehingga kondisi join selalu False → NULL.
           FIX: pre-join feedback dengan dim_feedback_kategori TERLEBIH
           DAHULU (sebelum chain utama) untuk menghasilkan
           df_fb_with_key yang sudah mengandung feedback_kategori_key.
           Kemudian df_fb_with_key di-join sekali ke chain utama.
"""

import os

# ─── Setup Environment Variables ──────────────────────────────────────────────
os.environ["JAVA_HOME"]    = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["PATH"]         = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["HADOOP_HOME"]  = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# ── FIX: PYSPARK_PYTHON ────────────────────────────────────────────────────────
# Spark JVM executor spawn Python worker menggunakan perintah "python" di PATH.
# Di Windows, perintah "python" sering diarahkan ke Microsoft Store alias
# (bukan Python yang sebenarnya) → "Python worker failed to connect back".
# Solusi: paksa Spark pakai executable Python yang sama dengan script ini
# menggunakan sys.executable, yang selalu menunjuk ke binary Python yang aktif.
import sys
os.environ["PYSPARK_PYTHON"]        = sys.executable   # worker di executor
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable   # driver (script ini)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, trim, to_timestamp, to_date,
    date_format, year, month, quarter, dayofweek,
    weekofyear, hour, floor as _floor,
    count, sum as _sum, avg, round as _round,
    monotonically_increasing_id, broadcast, current_date,
    current_timestamp, row_number, datediff
)
from pyspark.sql.types import (
    IntegerType, ShortType, DecimalType, BooleanType, ByteType
)
from pyspark.sql.window import Window
from datetime import datetime
import mysql.connector

# ─── Batch ID (dipakai sebagai metadata di setiap baris) ──────────────────────
BATCH_ID = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

# ============================================================
# 1. SPARK SESSION
# ============================================================
def buat_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("JakLingko-ETL")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.jars",
                "C:\\mysql-connector-j-9.6.0.jar,C:\\jdbc\\postgresql-42.7.9.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================================================
# 2. KONFIGURASI JDBC
# ============================================================
MYSQL_SRC = {
    "url"      : "jdbc:mysql://localhost:3306/jaklingko_mysql?useSSL=false&serverTimezone=Asia/Jakarta",
    "user"     : "root",
    "password" : "avitam346",
    "driver"   : "com.mysql.cj.jdbc.Driver",
}

PG_SRC = {
    "url"      : "jdbc:postgresql://localhost:5433/jaklingko_pg",
    "user"     : "postgres",
    "password" : "avitahana123",
    "driver"   : "org.postgresql.Driver",
}

MYSQL_DWH = {
    "url"      : "jdbc:mysql://localhost:3306/jaklingko_dwh?useSSL=false&serverTimezone=Asia/Jakarta",
    "user"     : "root",
    "password" : "avitam346",
    "driver"   : "com.mysql.cj.jdbc.Driver",
}

# ============================================================
# 3. HELPER: BACA & TULIS JDBC
# ============================================================
def baca_jdbc(spark, cfg, tabel):
    """Membaca data dari JDBC."""
    df = spark.read.jdbc(
        url=cfg["url"],
        table=tabel,
        properties={
            "user"    : cfg["user"],
            "password": cfg["password"],
            "driver"  : cfg["driver"]
        }
    )
    print(f"  [READ] {tabel}: {df.count()} baris")
    return df


def jalankan_sql(cfg, sql_cmd):
    """Jalankan SQL command langsung via mysql.connector."""
    conn = mysql.connector.connect(
        host="localhost",
        user=cfg["user"],
        password=cfg["password"],
        database="jaklingko_dwh"
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql_cmd)
        conn.commit()
        print(f"  [SQL] Executed: {sql_cmd[:80]}...")
    finally:
        cursor.close()
        conn.close()


def tulis_jdbc(df, cfg, tabel, mode="append"):
    """Menulis DataFrame ke tabel MySQL DWH via JDBC."""
    print(f"  [WRITE] {tabel}: {df.count()} baris (mode={mode})")

    if mode == "overwrite":
        print(f"  [FK] Mode overwrite detected - disabling FK checks for {tabel}")
        jalankan_sql(cfg, "SET FOREIGN_KEY_CHECKS=0")
        jalankan_sql(cfg, f"DELETE FROM {tabel}")
        jalankan_sql(cfg, "SET FOREIGN_KEY_CHECKS=1")
        mode = "append"

    df.write.jdbc(
        url=cfg["url"],
        table=tabel,
        mode=mode,
        properties={
            "user"    : cfg["user"],
            "password": cfg["password"],
            "driver"  : cfg["driver"]
        }
    )
    print(f"  [OK] {tabel} loaded successfully")

# ============================================================
# 4. EXTRACT
# ============================================================
def extract(spark):
    print("\n[FASE 1] EXTRACT")

    df_rute      = baca_jdbc(spark, MYSQL_SRC, "rute")
    df_halte     = baca_jdbc(spark, MYSQL_SRC, "halte")
    df_pengguna  = baca_jdbc(spark, PG_SRC,    "pengguna")
    df_transaksi = baca_jdbc(spark, PG_SRC,    "transaksi")
    df_feedback  = baca_jdbc(spark, PG_SRC,    "feedback")

    return df_rute, df_halte, df_pengguna, df_transaksi, df_feedback

# ============================================================
# 5. TRANSFORM
# ============================================================

# ── 5a. Cleaning ─────────────────────────────────────────────────────────────
def bersihkan(df, kolom_string: list):
    for k in kolom_string:
        if k in df.columns:
            df = df.withColumn(k, trim(col(k)))
    return df


def tangani_missing(df):
    """Drop baris tanpa FK kritis; isi kolom non-kritis."""
    df = df.dropna(subset=["pengguna_id", "rute_id", "waktu_tap_in"])
    df = df.fillna({"halte_turun_id": "UNKNOWN", "armada_id": "UNKNOWN", "diskon": 0})
    return df


def deduplikasi(df, pk: str, urutan_col: str = "created_at"):
    """
    Pertahankan satu baris terbaru per primary key.
    BUG-5 FIX: fallback jika kolom urutan tidak tersedia.
    """
    if urutan_col not in df.columns:
        for alt in ["waktu_tap_in", "updated_at", pk]:
            if alt in df.columns:
                print(f"  [DEDUP] Kolom '{urutan_col}' tidak ditemukan, fallback ke '{alt}'")
                urutan_col = alt
                break

    w = Window.partitionBy(pk).orderBy(col(urutan_col).desc())
    before = df.count()
    df = (df.withColumn("_rn", row_number().over(w))
            .filter(col("_rn") == 1)
            .drop("_rn"))
    print(f"  [DEDUP] {pk}: {before} -> {df.count()} baris")
    return df


# ── 5b. Kolom Turunan ─────────────────────────────────────────────────────────
def kolom_turunan_transaksi(df):
    """Tambah jam_tap_in, sesi_hari, durasi_menit, tanggal_perjalanan."""
    df = (df
          .withColumn("waktu_tap_in",  to_timestamp(col("waktu_tap_in")))
          .withColumn("waktu_tap_out", to_timestamp(col("waktu_tap_out")))
          .withColumn("jam_tap_in", hour("waktu_tap_in"))
          .withColumn("sesi_hari",
                      when(col("jam_tap_in").between(5, 9),   lit("Pagi"))
                      .when(col("jam_tap_in").between(10, 14), lit("Siang"))
                      .when(col("jam_tap_in").between(15, 18), lit("Sore"))
                      .otherwise(lit("Malam")))
          .withColumn("durasi_menit",
                      when(col("waktu_tap_out").isNotNull(),
                           ((F.unix_timestamp("waktu_tap_out") -
                             F.unix_timestamp("waktu_tap_in")) / 60)
                           .cast(ShortType())))
          .withColumn("tanggal_perjalanan", to_date(col("waktu_tap_in")))
          .withColumn("is_sukses",
                      when(col("status") == "Sukses", lit(1)).otherwise(lit(0))))
    return df


def kolom_turunan_pengguna(df):
    """Tambah kelompok_usia, segment_pengguna, wilayah."""
    usia = (datediff(current_date(), to_date(col("tanggal_lahir"))) / 365.25)
    df = (df
          .withColumn("usia", usia.cast(IntegerType()))
          .withColumn("kelompok_usia",
                      when(col("usia") < 20,  lit("< 20 Tahun"))
                      .when(col("usia") < 31, lit("20-30 Tahun"))
                      .when(col("usia") < 41, lit("31-40 Tahun"))
                      .when(col("usia") < 55, lit("41-54 Tahun"))
                      .otherwise(lit(">= 55 Tahun")))
          .withColumn("segment_pengguna",
                      when(col("usia") < 20, lit("Pelajar"))
                      .when(col("usia") < 25, lit("Mahasiswa"))
                      .when(col("usia") < 55, lit("Pekerja"))
                      .otherwise(lit("Lansia")))
          .withColumn("wilayah",
                      when(col("kota_domisili").isin(
                          ["Jakarta Pusat", "Jakarta Selatan", "Jakarta Barat",
                           "Jakarta Utara", "Jakarta Timur", "Bogor", "Depok",
                           "Tangerang", "Bekasi"]), lit("Jabodetabek"))
                      .otherwise(lit("Lainnya")))
          .drop("usia"))
    return df


def perkaya_halte(df):
    """Tambah kota dan wilayah ke tabel halte."""
    return (df
            .withColumn("kota",
                        when(col("halte_id").isin(["H006"]),             lit("Jakarta Utara"))
                        .when(col("halte_id").isin(["H001","H002","H007"]), lit("Jakarta Selatan"))
                        .when(col("halte_id").isin(["H005","H008","H010"]), lit("Jakarta Pusat"))
                        .when(col("halte_id").isin(["H009"]),             lit("Jakarta Timur"))
                        .otherwise(lit("Jakarta Pusat")))
            .withColumn("wilayah", lit("DKI Jakarta")))


# ── 5c. Generate Dimensi Waktu ───────────────────────────────────────────────
def buat_dim_waktu(spark, tgl_awal="2024-01-01", tgl_akhir="2024-12-31"):
    """Buat date spine programatik tanpa butuh tabel sumber."""
    date_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{tgl_awal}'), to_date('{tgl_akhir}'),
            interval 1 day)) AS tanggal
    """)
    dim_w = (date_df
             .withColumn("tahun",              year("tanggal").cast(ShortType()))
             # BUG-1 FIX: cast("tinyint") tidak valid → ByteType()
             .withColumn("kuartal",            quarter("tanggal").cast(ByteType()))
             .withColumn("bulan",              month("tanggal").cast(ByteType()))
             .withColumn("nama_bulan",         date_format("tanggal", "MMMM"))
             .withColumn("minggu_dalam_tahun", weekofyear("tanggal").cast(ByteType()))
             .withColumn("hari_dalam_bulan",   F.dayofmonth("tanggal").cast(ByteType()))
             .withColumn("nama_hari",          date_format("tanggal", "EEEE"))
             .withColumn("is_weekend",
                         when(dayofweek("tanggal").isin([1, 7]), lit(1)).otherwise(lit(0)))
             .withColumn("is_hari_libur",    lit(0))
             .withColumn("keterangan_libur", lit(None).cast("string"))
             .withColumn("waktu_key",
                         row_number().over(Window.orderBy("tanggal"))))
    return dim_w


# ── 5d. Surrogate Key ─────────────────────────────────────────────────────────
def tambah_sk(df, nama_sk, urut_by):
    w = Window.orderBy(urut_by)
    return df.withColumn(nama_sk, row_number().over(w))


# ── 5e. Dimensi Feedback Kategori ─────────────────────────────────────────────
def siapkan_dim_feedback_kategori(spark, df_feedback):
    """
    BUG-8 FIX (Python 3.13 + PySpark incompatibility):
    ─────────────────────────────────────────────────────
    Dua operasi sebelumnya menyebabkan Python worker crash di Python 3.13:

      (a) .collect() pada df_feedback (JDBC DataFrame)
          → PySpark mengirim data dari JVM ke Python driver via Python worker.
            Python 3.13 mengubah internal serialization protocol sehingga
            worker subprocess crash dengan EOFException.

      (b) spark.createDataFrame(python_list, schema)
          → Internally menggunakan sparkContext.parallelize() yang membuat
            Python RDD. Eksekusi Python RDD memerlukan Python worker.
            Worker crash dengan alasan yang sama.

    Solusi: gunakan spark.sql() dengan UNION ALL.
    ─────────────────────────────────────────────
    spark.sql() dieksekusi SEPENUHNYA di JVM (Catalyst optimizer + Tungsten).
    Tidak ada Python worker yang di-spawn sama sekali.
    Hasilnya adalah DataFrame JVM biasa yang bisa di-join, di-broadcast,
    dan ditulis ke JDBC tanpa menyentuh Python worker.

    Key mapping (sesuai seed DWH mysql_dwh_star_schema.sql):
      1 → Aplikasi Jaklingko
      2 → Fasilitas Busway
      3 → Umum
    """
    dim_fk = spark.sql("""
        SELECT CAST(1 AS INT)  AS feedback_kategori_key,
               CAST('Aplikasi Jaklingko' AS STRING) AS kategori_nama,
               CAST('Feedback tentang aplikasi mobile JakLingko' AS STRING) AS deskripsi
        UNION ALL
        SELECT CAST(2 AS INT),
               CAST('Fasilitas Busway' AS STRING),
               CAST('Feedback tentang fasilitas halte dan armada' AS STRING)
        UNION ALL
        SELECT CAST(3 AS INT),
               CAST('Umum' AS STRING),
               CAST('Feedback umum tentang layanan secara keseluruhan' AS STRING)
    """)
    # Hindari .count() di sini — hasilnya sudah diketahui (3 baris statis)
    print("  [OK] dim_feedback_kategori disiapkan: 3 kategori (pure JVM SQL)")
    return dim_fk


# ── 5f. Join Lintas Sumber -> Fact ────────────────────────────────────────────
def bangun_fact(df_trx, df_halte_dim, df_rute_dim,
                df_pengguna_dim, df_dim_waktu, df_feedback, dim_feedback_kategori):
    """
    Gabungkan transaksi (PostgreSQL) dengan semua dimensi + feedback.

    BUG-2 FIX: Joining DataFrame dim_halte dua kali → prefix kolom hn_/ht_.

    BUG-6 FIX (feedback_kategori_key selalu NULL):
    ─────────────────────────────────────────────
    Penyebab: join dim_feedback_kategori dilakukan di dalam chained-join utama
    dengan kondisi col("fb.kategori") == col("dk.kategori_nama").
    Setelah beberapa join dirantai, PySpark tidak bisa me-resolve alias "fb"
    pada join berikutnya → kondisi tidak pernah terpenuhi → LEFT JOIN
    menghasilkan NULL untuk semua kolom dim_feedback_kategori.

    Solusi: PRE-JOIN feedback dengan dim_feedback_kategori SEBELUM chain utama.
    Hasilnya (df_fb_with_key) sudah mengandung feedback_kategori_key yang
    terisi dengan benar. df_fb_with_key kemudian di-join sekali ke chain utama
    menggunakan transaksi_id, sehingga tidak ada lagi referensi alias ambigu.
    """

    # ── BUG-2 FIX: prefix semua kolom halte ──────────────────────────────────
    kolom_halte    = df_halte_dim.columns
    df_halte_naik  = df_halte_dim.select([col(c).alias(f"hn_{c}") for c in kolom_halte])
    df_halte_turun = df_halte_dim.select([col(c).alias(f"ht_{c}") for c in kolom_halte])

    # ── Pilih kolom feedback yang dibutuhkan ─────────────────────────────────
    fb_kolom = ["transaksi_id", "rating", "komentar"]
    if "kategori" in df_feedback.columns:
        fb_kolom.append("kategori")
    df_fb_raw = df_feedback.select([col(c) for c in fb_kolom])

    # ── BUG-6 FIX: Pre-join feedback × dim_feedback_kategori ─────────────────
    # Join dilakukan di sini, di luar chain utama, sehingga referensi kolom
    # tidak ambigu. Hasilnya adalah df_fb_with_key yang memiliki kolom:
    #   transaksi_id | rating | komentar | feedback_kategori_key
    if "kategori" in df_feedback.columns:
        # Gunakan referensi DataFrame langsung (bukan alias string) agar aman
        df_fb_with_key = (
            df_fb_raw
            .join(
                broadcast(dim_feedback_kategori),
                df_fb_raw["kategori"] == dim_feedback_kategori["kategori_nama"],
                "left"
            )
            .select(
                df_fb_raw["transaksi_id"],
                df_fb_raw["rating"],
                df_fb_raw["komentar"],
                dim_feedback_kategori["feedback_kategori_key"]   # ← terisi dengan benar
            )
        )
    else:
        df_fb_with_key = (df_fb_raw
                          .withColumn("feedback_kategori_key",
                                      lit(None).cast(IntegerType())))

    print(f"  [PRE-JOIN] feedback × dim_feedback_kategori: "
          f"{df_fb_with_key.filter(col('feedback_kategori_key').isNotNull()).count()} "
          f"baris dengan key terisi")

    # ── Chain join utama ──────────────────────────────────────────────────────
    fact = (
        df_trx.alias("t")

        # dim_waktu
        .join(broadcast(df_dim_waktu.alias("w")),
              col("t.tanggal_perjalanan") == col("w.tanggal"), "left")

        # dim_halte naik (BUG-2 FIX: gunakan df yang sudah di-prefix)
        .join(broadcast(df_halte_naik),
              col("t.halte_naik_id") == col("hn_halte_id"), "left")

        # dim_halte turun (BUG-2 FIX: idem)
        .join(broadcast(df_halte_turun),
              col("t.halte_turun_id") == col("ht_halte_id"), "left")

        # dim_rute
        .join(broadcast(df_rute_dim.alias("r")),
              col("t.rute_id") == col("r.rute_id"), "left")

        # dim_pengguna
        .join(df_pengguna_dim.alias("p"),
              col("t.pengguna_id") == col("p.pengguna_id"), "left")

        # feedback + feedback_kategori_key (BUG-6 FIX: sudah pre-joined)
        .join(df_fb_with_key.alias("fb"),
              col("t.transaksi_id") == col("fb.transaksi_id"), "left")

        .select(
            col("w.waktu_key"),
            col("hn_halte_key").alias("halte_naik_key"),
            col("ht_halte_key").alias("halte_turun_key"),
            col("r.rute_key"),
            col("p.pengguna_key"),
            # BUG-6 FIX: feedback_kategori_key sekarang terisi dari pre-join
            col("fb.feedback_kategori_key"),
            col("t.transaksi_id"),
            col("t.armada_id"),
            col("t.waktu_tap_in"),
            col("t.waktu_tap_out"),
            col("t.jam_tap_in"),
            col("t.sesi_hari"),
            col("t.tarif"),
            col("t.diskon"),
            col("t.total_bayar"),
            col("t.durasi_menit"),
            col("r.jarak_km").alias("jarak_tempuh_km"),
            col("t.metode_bayar"),
            col("t.status").alias("status_transaksi"),
            col("t.is_sukses"),
            col("fb.rating"),
            col("fb.komentar"),
            when(col("fb.rating").isNotNull(), lit(1)).otherwise(lit(0)).alias("has_feedback"),
            lit("postgresql").alias("sumber_data"),
            lit(BATCH_ID).alias("etl_batch_id"),
            current_timestamp().alias("etl_loaded_at"),
        )
    )

    # Hanya simpan baris yang berhasil di-lookup ke semua dimensi kritis
    fact = fact.filter(
        col("waktu_key").isNotNull() &
        col("rute_key").isNotNull() &
        col("pengguna_key").isNotNull()
    )
    return fact


# ============================================================
# 6. LOAD KE DWH (MySQL)
# ============================================================
def load(dim_waktu, df_halte_dim, df_rute_dim,
         df_pengguna_dim, dim_feedback_kategori, df_fact):
    """
    Urutan load yang benar (fact dihapus dulu, dimensi di-overwrite,
    dim_feedback_kategori ditulis, baru fact di-append):

      DELETE fact_transaksi_perjalanan
      1. dim_waktu               (overwrite)
      2. dim_halte               (overwrite)
      3. dim_rute                (overwrite)
      4. dim_pengguna            (overwrite)
      5. dim_feedback_kategori   (overwrite) ← BUG-4 FIX
      6. fact_transaksi_perjalanan (append)
    """
    print("\n[FASE 3] LOAD -> jaklingko_dwh (MySQL Workbench)")

    # Hapus fact dulu agar dimensi bisa di-overwrite tanpa FK violation
    print("  [STEP] Delete fact table dulu (FK safety)")
    jalankan_sql(MYSQL_DWH, "DELETE FROM fact_transaksi_perjalanan")

    kolom_dim_waktu = [
        "waktu_key", "tanggal", "tahun", "kuartal", "bulan", "nama_bulan",
        "minggu_dalam_tahun", "hari_dalam_bulan", "nama_hari",
        "is_weekend", "is_hari_libur", "keterangan_libur"
    ]
    kolom_dim_halte = [
        "halte_key", "halte_id", "nama_halte", "alamat",
        "latitude", "longitude", "fasilitas", "kota", "wilayah", "status"
    ]
    kolom_dim_rute = [
        "rute_key", "rute_id", "nama_rute", "jenis_moda", "asal", "tujuan",
        "jarak_km", "tarif_dasar", "status"
    ]
    kolom_dim_pengguna = [
        "pengguna_key", "pengguna_id", "nama_lengkap", "gender",
        "kelompok_usia", "kota_domisili", "wilayah", "segment_pengguna", "tanggal_daftar"
    ]
    kolom_dim_feedback_kategori = [
        "feedback_kategori_key", "kategori_nama", "deskripsi"
    ]

    # 1. dim_waktu
    tulis_jdbc(
        dim_waktu.select(kolom_dim_waktu),
        MYSQL_DWH, "dim_waktu", mode="overwrite"
    )

    # 2. dim_halte (+ SCD Type 2 kolom)
    tulis_jdbc(
        df_halte_dim
            .withColumn("valid_dari",   current_date())
            .withColumn("valid_sampai", lit(None).cast("date"))
            .withColumn("is_current",   lit(1))
            .select(kolom_dim_halte + ["valid_dari", "valid_sampai", "is_current"]),
        MYSQL_DWH, "dim_halte", mode="overwrite"
    )

    # 3. dim_rute (+ SCD Type 2 kolom + koridor)
    tulis_jdbc(
        df_rute_dim
            .withColumn("koridor",      lit(None).cast("string"))
            .withColumn("valid_dari",   current_date())
            .withColumn("valid_sampai", lit(None).cast("date"))
            .withColumn("is_current",   lit(1))
            .select(kolom_dim_rute + ["koridor", "valid_dari", "valid_sampai", "is_current"]),
        MYSQL_DWH, "dim_rute", mode="overwrite"
    )

    # 4. dim_pengguna (+ SCD Type 2 kolom)
    tulis_jdbc(
        df_pengguna_dim
            .withColumn("valid_dari",   current_date())
            .withColumn("valid_sampai", lit(None).cast("date"))
            .withColumn("is_current",   lit(1))
            .select(kolom_dim_pengguna + ["valid_dari", "valid_sampai", "is_current"]),
        MYSQL_DWH, "dim_pengguna", mode="overwrite"
    )

    # 5. dim_feedback_kategori — BUG-4 FIX: ditulis sebelum fact
    #    Tulis dengan explicit key (feedback_kategori_key) agar FK dari fact
    #    terpenuhi. MySQL menerima INSERT dengan nilai AUTO_INCREMENT eksplisit.
    tulis_jdbc(
        dim_feedback_kategori.select(kolom_dim_feedback_kategori),
        MYSQL_DWH, "dim_feedback_kategori", mode="overwrite"
    )

    # 6. fact_transaksi_perjalanan (terakhir, karena punya FK ke semua dimensi)
    tulis_jdbc(df_fact, MYSQL_DWH, "fact_transaksi_perjalanan", mode="append")


# ============================================================
# 7. PIPELINE UTAMA
# ============================================================
def jalankan():
    print("=" * 60)
    print(f" JakLingko ETL PySpark | Batch: {BATCH_ID}")
    print("=" * 60)

    spark = buat_spark()

    # ── EXTRACT ──────────────────────────────────────────────────────────────
    df_rute, df_halte, df_pengguna, df_transaksi, df_feedback = extract(spark)

    # ── TRANSFORM ────────────────────────────────────────────────────────────
    print("\n[FASE 2] TRANSFORM")

    df_rute      = bersihkan(df_rute,     ["nama_rute", "asal", "tujuan"])
    df_halte     = bersihkan(df_halte,    ["nama_halte", "alamat"])
    df_halte     = perkaya_halte(df_halte)
    df_pengguna  = bersihkan(df_pengguna, ["nama_lengkap", "email", "kota_domisili"])

    df_transaksi = tangani_missing(df_transaksi)
    # BUG-5 FIX: fallback kolom urutan jika created_at tidak ada
    df_transaksi = deduplikasi(df_transaksi, "transaksi_id", "created_at")
    df_transaksi = kolom_turunan_transaksi(df_transaksi)
    df_pengguna  = kolom_turunan_pengguna(df_pengguna)

    # Buat dimensi waktu
    dim_waktu = buat_dim_waktu(spark, "2024-01-01", "2024-12-31")

    # Surrogate key untuk dimensi
    df_halte_dim    = tambah_sk(df_halte,    "halte_key",    "halte_id")
    df_rute_dim     = tambah_sk(df_rute,     "rute_key",     "rute_id")
    df_pengguna_dim = tambah_sk(df_pengguna, "pengguna_key", "pengguna_id")

    # BUG-3 + BUG-6 FIX: bangun dim_feedback_kategori dari daftar default
    # (bukan dari DWH) agar key stabil dan konsisten untuk join di bangun_fact
    dim_feedback_kategori = siapkan_dim_feedback_kategori(spark, df_feedback)

    # Bangun fact table
    # BUG-6 FIX: feedback_kategori_key di-resolve via pre-join di dalam bangun_fact
    df_fact = bangun_fact(
        df_transaksi, df_halte_dim, df_rute_dim,
        df_pengguna_dim, dim_waktu, df_feedback, dim_feedback_kategori
    )
    print(f"  [FACT] Siap dimuat: {df_fact.count()} baris")

    # ── LOAD ─────────────────────────────────────────────────────────────────
    load(dim_waktu, df_halte_dim, df_rute_dim,
         df_pengguna_dim, dim_feedback_kategori, df_fact)

    print("\n" + "=" * 60)
    print(f" Pipeline selesai: {BATCH_ID}")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    jalankan()