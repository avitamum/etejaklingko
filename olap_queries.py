"""
============================================================
 JAKLINGKO DATA PIPELINE - UTS
 FILE  : olap_queries.py
 DESC  : Query OLAP terhadap Data Warehouse MySQL (jaklingko_dwh)
         menggunakan PySpark SQL
         + Visualisasi dengan matplotlib

 Jalankan:
   spark-submit --packages mysql:mysql-connector-java:8.0.33 \
     olap_queries.py
   ATAU (jika sudah download JAR):
   python olap_queries.py
============================================================
"""

import os

# ─── Setup Environment Variables ──
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, round as _round
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

OUTPUT_DIR = "output_grafik"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MYSQL_DWH = {
    "url"      : "jdbc:mysql://localhost:3306/jaklingko_dwh?useSSL=false&serverTimezone=Asia/Jakarta",
    "user"     : "root",
    "password" : "avitam346",
    "driver"   : "com.mysql.cj.jdbc.Driver",
}

WARNA = ["#1a6bb5","#2eb87e","#e85d2e","#f5a623","#9b59b6","#34495e","#1abc9c","#e74c3c"]


def buat_spark():
    s = (SparkSession.builder.appName("JakLingko-OLAP")
         .config("spark.jars", "C:\\mysql-connector-j-9.6.0.jar")
         .master("local[*]").getOrCreate())
    s.sparkContext.setLogLevel("WARN")
    return s


def baca(spark, tabel):
    return (spark.read.format("jdbc")
            .option("url",      MYSQL_DWH["url"])
            .option("dbtable",  f"jaklingko_dwh.{tabel}")
            .option("user",     MYSQL_DWH["user"])
            .option("password", MYSQL_DWH["password"])
            .option("driver",   MYSQL_DWH["driver"])
            .load())


def to_pandas_float(spark_df):
    """Convert Spark DataFrame to pandas, casting all numeric columns to float."""
    from decimal import Decimal
    pdf = spark_df.toPandas()
    for col in pdf.columns:
        if pdf[col].dtype == 'object':
            # Check if column contains Decimal values
            try:
                if any(isinstance(x, Decimal) for x in pdf[col] if x is not None):
                    pdf[col] = pdf[col].astype(float)
            except:
                pass
    return pdf


def simpan(nama):
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"{nama}.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[VIZ] {nama}.png tersimpan")


# ============================================================
# QUERY 1 – Pendapatan & Volume Transaksi per Bulan
#            Operasi OLAP: Roll-up (Tahun → Bulan)
# ============================================================
def q1_pendapatan_per_bulan(spark):
    print("\n=== Q1: Pendapatan & Volume per Bulan ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dw   = baca(spark, "dim_waktu")

    hasil = (
        fact.filter(col("status_transaksi") == "Sukses")
        .join(dw, "waktu_key")
        .groupBy("tahun", "bulan", "nama_bulan")
        .agg(
            _sum("total_bayar").alias("total_pendapatan"),
            count("transaksi_key").alias("jml_transaksi"),
            _round(avg("total_bayar"), 0).alias("rata_bayar"),
        )
        .orderBy("tahun", "bulan")
    )
    hasil.show(12)

    # Contoh output yang dihasilkan:
    # +------+------+-----------+------------------+---------------+----------+
    # |tahun |bulan |nama_bulan |total_pendapatan  |jml_transaksi  |rata_bayar|
    # +------+------+-----------+------------------+---------------+----------+
    # |2024  |1     |January    |87450000          |18320          |4773      |
    # |2024  |2     |February   |79800000          |16450          |4852      |
    # |2024  |3     |March      |95100000          |20110          |4729      |
    # |2024  |4     |April      |91250000          |19350          |4716      |
    # |2024  |5     |May        |88700000          |18900          |4693      |
    # |2024  |6     |June       |104300000         |22540          |4627      |
    # +------+------+-----------+------------------+---------------+----------+

    pd_h = to_pandas_float(hasil)
    pd_h["label"] = pd_h["nama_bulan"].str[:3]

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.bar(pd_h["label"], pd_h["total_pendapatan"] / 1e6,
            color=WARNA[0], alpha=0.8, label="Pendapatan (Juta Rp)")
    ax1.set_ylabel("Pendapatan (Juta Rp)", color=WARNA[0])
    ax2 = ax1.twinx()
    ax2.plot(pd_h["label"], pd_h["jml_transaksi"],
             color=WARNA[2], marker="o", linewidth=2, label="Volume Transaksi")
    ax2.set_ylabel("Jumlah Transaksi", color=WARNA[2])
    ax1.set_title("Pendapatan & Volume Transaksi JakLingko per Bulan (2024)",
                  fontweight="bold")
    lines = ax1.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0]
    labels = ax1.get_legend_handles_labels()[1] + ax2.get_legend_handles_labels()[1]
    ax1.legend(lines, labels)
    simpan("q1_pendapatan_per_bulan")
    return hasil


# ============================================================
# QUERY 2 – Volume & Pendapatan per Rute
#            Operasi OLAP: Slice (status=Sukses) + Group By
# ============================================================
def q2_per_rute(spark):
    print("\n=== Q2: Volume & Pendapatan per Rute ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dr   = baca(spark, "dim_rute")

    hasil = (
        fact.filter(col("status_transaksi") == "Sukses")
        .join(dr, "rute_key")
        .groupBy("rute_id", "nama_rute", "jenis_moda")
        .agg(
            count("transaksi_key").alias("jml_transaksi"),
            _sum("total_bayar").alias("total_pendapatan"),
            _round(avg("durasi_menit"), 1).alias("avg_durasi"),
        )
        .orderBy(col("jml_transaksi").desc())
    )
    hasil.show(10)

    # Contoh output:
    # +---------+-----------------------------+-----------+-------------+------------------+-----------+
    # |rute_id  |nama_rute                    |jenis_moda |jml_transaksi|total_pendapatan  |avg_durasi |
    # +---------+-----------------------------+-----------+-------------+------------------+-----------+
    # |R002     |Lebak Bulus - Bundaran HI    |MRT        |45210        |633740000         |34.5       |
    # |R001     |Blok M - Kota                |BRT        |38450        |134575000         |42.1       |
    # |R003     |Cibubur - Dukuh Atas         |LRT        |27300        |136500000         |51.8       |
    # |R004     |Tanah Abang - Pulo Gadung    |Bus        |19800        |69300000          |55.2       |
    # |R005     |Kampung Rambutan - Blok M    |Mikrotrans |12400        |62000000          |38.7       |
    # +---------+-----------------------------+-----------+-------------+------------------+-----------+

    pd_h = to_pandas_float(hasil)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    pd_h.sort_values("jml_transaksi").plot(
        kind="barh", x="nama_rute", y="jml_transaksi",
        ax=axes[0], color=WARNA[1], legend=False
    )
    axes[0].set_title("Volume Transaksi per Rute", fontweight="bold")
    axes[0].set_xlabel("Jumlah Transaksi")

    moda = pd_h.groupby("jenis_moda")["total_pendapatan"].sum()
    axes[1].pie(moda, labels=moda.index, autopct="%1.1f%%",
                colors=WARNA[:len(moda)], startangle=140)
    axes[1].set_title("Proporsi Pendapatan per Moda", fontweight="bold")

    simpan("q2_per_rute")
    return hasil


# ============================================================
# QUERY 3 – Analisis Jam Sibuk (Peak Hour)
#            Operasi OLAP: Dice (hari kerja + BRT/MRT)
# ============================================================
def q3_jam_sibuk(spark):
    print("\n=== Q3: Distribusi Jam Sibuk (BRT & MRT, Hari Kerja) ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dw   = baca(spark, "dim_waktu")
    dr   = baca(spark, "dim_rute")

    hasil = (
        fact.filter(col("status_transaksi") == "Sukses")
        .join(dw.filter(col("is_weekend") == False), "waktu_key")
        .join(dr.filter(col("jenis_moda").isin(["BRT","MRT"])), "rute_key")
        .groupBy("jam_tap_in", "sesi_hari")
        .agg(
            count("transaksi_key").alias("jml_transaksi"),
            _round(avg("durasi_menit"), 1).alias("avg_durasi"),
        )
        .orderBy("jam_tap_in")
    )
    hasil.show(24)

    # Contoh output:
    # +-----------+----------+-------------+----------+
    # |jam_tap_in |sesi_hari |jml_transaksi|avg_durasi|
    # +-----------+----------+-------------+----------+
    # |6          |Pagi      |4870         |41.5      |
    # |7          |Pagi      |9450         |44.3      |  <- PEAK PAGI
    # |8          |Pagi      |8210         |46.7      |
    # |17         |Sore      |10120        |48.9      | <- PEAK SORE
    # |18         |Sore      |8750         |47.3      |
    # +-----------+----------+-------------+----------+

    pd_h = to_pandas_float(hasil)
    
    # Handle empty result
    if len(pd_h) == 0:
        print("[WARNING] Q3: Tidak ada data untuk jam sibuk BRT/MRT hari kerja. Skip visualization.")
        return hasil
    
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.fill_between(pd_h["jam_tap_in"], pd_h["jml_transaksi"],
                    alpha=0.35, color=WARNA[0])
    ax.plot(pd_h["jam_tap_in"], pd_h["jml_transaksi"],
            color=WARNA[0], linewidth=2.5, marker="o")

    pk = pd_h.loc[pd_h["jml_transaksi"].idxmax()]
    ax.annotate(
        f"Peak: {int(pk['jml_transaksi']):,} trx\nJam {int(pk['jam_tap_in']):02d}:00",
        xy=(pk["jam_tap_in"], pk["jml_transaksi"]),
        xytext=(pk["jam_tap_in"] + 1.5, pk["jml_transaksi"] * 0.92),
        arrowprops=dict(arrowstyle="->", color="red"), color="red", fontsize=9
    )
    ax.set_xticks(list(range(0, 24)))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(24)], rotation=45, ha="right")
    ax.set_ylabel("Jumlah Transaksi")
    ax.set_title("Distribusi Transaksi per Jam – Hari Kerja (BRT & MRT)",
                 fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    simpan("q3_jam_sibuk")
    return hasil


# ============================================================
# QUERY 4 – Metode Bayar per Segmen Pengguna
#            Operasi OLAP: Slice (bulan=6) + Dice (Jabodetabek)
# ============================================================
def q4_metode_bayar(spark):
    print("\n=== Q4: Metode Bayar per Segmen (Juni, Jabodetabek) ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dw   = baca(spark, "dim_waktu")
    dp   = baca(spark, "dim_pengguna")

    hasil = (
        fact.filter(col("status_transaksi") == "Sukses")
        .join(dw.filter(col("bulan") == 6), "waktu_key")
        .join(dp.filter(col("wilayah") == "Jabodetabek"), "pengguna_key")
        .groupBy("metode_bayar", "segment_pengguna")
        .agg(
            count("transaksi_key").alias("jml_transaksi"),
            _round(_sum("total_bayar") / 1e6, 2).alias("pendapatan_juta"),
        )
        .orderBy(col("jml_transaksi").desc())
    )
    hasil.show(20)

    # Contoh output:
    # +--------------+------------------+-------------+------------------+
    # |metode_bayar  |segment_pengguna  |jml_transaksi|pendapatan_juta   |
    # +--------------+------------------+-------------+------------------+
    # |GoPay         |Pekerja           |8420         |118.10            |
    # |JakCard       |Pekerja           |7910         |27.69             |
    # |OVO           |Mahasiswa         |5630         |19.71             |
    # |Dana          |Pekerja           |4200         |58.80             |
    # |ShopeePay     |Mahasiswa         |3810         |13.34             |
    # +--------------+------------------+-------------+------------------+

    pd_h = to_pandas_float(hasil)
    pivot = pd_h.pivot_table(
        index="metode_bayar", columns="segment_pengguna",
        values="jml_transaksi", aggfunc="sum", fill_value=0
    )

    fig, ax = plt.subplots(figsize=(11, 5))
    pivot.sort_values(pivot.columns[0], ascending=False).plot(
        kind="bar", ax=ax, color=WARNA[:len(pivot.columns)],
        edgecolor="white"
    )
    ax.set_title("Volume Transaksi per Metode Bayar & Segmen\n"
                 "(Jabodetabek, Juni 2024)", fontweight="bold")
    ax.set_xlabel("Metode Pembayaran")
    ax.set_ylabel("Jumlah Transaksi")
    ax.legend(title="Segmen", bbox_to_anchor=(1, 1))
    ax.tick_params(axis="x", rotation=30)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    simpan("q4_metode_bayar")
    return hasil


# ============================================================
# QUERY 5 – Top-10 Halte Tersibuk
#            Operasi OLAP: Ranking (RANK OVER)
# ============================================================
def q5_top_halte(spark, top_n=10):
    print(f"\n=== Q5: Top {top_n} Halte Tersibuk ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dh   = baca(spark, "dim_halte")

    agg = (
        fact.filter(col("status_transaksi") == "Sukses")
        .join(dh.alias("hn"), col("halte_naik_key") == col("hn.halte_key"))
        .groupBy(col("hn.halte_id"), col("hn.nama_halte"), col("hn.kota"))
        .agg(
            count("transaksi_key").alias("penumpang_naik"),
            _round(avg("total_bayar"), 0).alias("avg_tarif"),
        )
    )
    hasil = (
        agg.withColumn(
            "ranking",
            F.rank().over(Window.orderBy(col("penumpang_naik").desc()))
        )
        .filter(col("ranking") <= top_n)
        .orderBy("ranking")
    )
    hasil.show(top_n)

    # Contoh output:
    # +--------+---------------------------+------------------+-------------+----------+-------+
    # |halte_id|nama_halte                 |kota              |penumpang_naik|avg_tarif|ranking|
    # +--------+---------------------------+------------------+-------------+----------+-------+
    # |H008    |Stasiun Bundaran HI        |Jakarta Pusat     |28410        |14017     |1      |
    # |H007    |Stasiun Lebak Bulus        |Jakarta Selatan   |24730        |14017     |2      |
    # |H005    |Halte Dukuh Atas           |Jakarta Pusat     |19870        |5240      |3      |
    # |H001    |Halte Blok M               |Jakarta Selatan   |17650        |4105      |4      |
    # |H006    |Halte Kota                 |Jakarta Utara     |12400        |3500      |5      |
    # +--------+---------------------------+------------------+-------------+----------+-------+

    pd_h = to_pandas_float(hasil).sort_values("penumpang_naik")
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(pd_h["nama_halte"], pd_h["penumpang_naik"],
                   color=[WARNA[0] if i == len(pd_h) - 1 else WARNA[1]
                          for i in range(len(pd_h))])
    for b in bars:
        ax.text(b.get_width() + 100, b.get_y() + b.get_height() / 2,
                f"{int(b.get_width()):,}", va="center", fontsize=8)
    ax.set_title(f"Top {top_n} Halte Tersibuk – Penumpang Naik (2024)",
                 fontweight="bold")
    ax.set_xlabel("Jumlah Penumpang Naik")
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    simpan("q5_top_halte")
    return hasil


# ============================================================
# QUERY 6 – Distribusi Rating & Kepuasan per Kategori Feedback
#            Operasi OLAP: Slice (has_feedback=1) + Group By Kategori
# ============================================================
def q6_rating_kategori(spark):
    print("\n=== Q6: Distribusi Rating per Kategori Feedback ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dk   = baca(spark, "dim_feedback_kategori")

    hasil = (
        fact.filter(col("has_feedback") == True)
        .join(dk, "feedback_kategori_key")
        .groupBy("kategori_nama")
        .agg(
            count("transaksi_key").alias("jml_feedback"),
            _round(avg("rating"), 2).alias("avg_rating"),
            F.min("rating").alias("min_rating"),
            F.max("rating").alias("max_rating"),
            _round(
                _sum(F.when(col("rating") >= 4, 1).otherwise(0)) / count("transaksi_key") * 100,
                1
            ).alias("pct_satisfied"),
        )
        .orderBy(col("avg_rating").desc())
    )
    hasil.show()

    # Contoh output:
    # +----------------------+-------------+----------+-----------+-----------+---------------+
    # |kategori_nama         |jml_feedback |avg_rating|min_rating |max_rating |pct_satisfied  |
    # +----------------------+-------------+----------+-----------+-----------+---------------+
    # |Aplikasi Jaklingko    |4280         |4.15      |1          |5          |82.5           |
    # |Fasilitas Busway      |3650         |4.02      |1          |5          |78.9           |
    # |Umum                  |2180         |3.98      |1          |5          |76.2           |
    # +----------------------+-------------+----------+-----------+-----------+---------------+

    pd_h = to_pandas_float(hasil)
    
    # Handle empty result
    if len(pd_h) == 0:
        print("[WARNING] Q6: Tidak ada data feedback. Skip visualization.")
        return hasil
    
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    # Bar chart: Average Rating per Kategori
    pd_h_sorted = pd_h.sort_values("avg_rating")
    axes[0].barh(pd_h_sorted["kategori_nama"], pd_h_sorted["avg_rating"],
                 color=WARNA[:len(pd_h_sorted)], edgecolor="black", linewidth=1.5)
    axes[0].set_xlim(0, 5.5)
    axes[0].set_xlabel("Rata-rata Rating (1-5)")
    axes[0].set_title("Rata-rata Rating per Kategori Feedback", fontweight="bold")
    axes[0].axvline(x=4, color="green", linestyle="--", alpha=0.7, label="Target (4.0)")
    axes[0].legend()
    axes[0].grid(axis="x", linestyle="--", alpha=0.3)

    # Bar chart: Persentase Satisfied (Rating >= 4)
    axes[1].bar(pd_h["kategori_nama"], pd_h["pct_satisfied"],
                color=WARNA[:len(pd_h)], edgecolor="black", linewidth=1.5)
    axes[1].set_ylabel("Persentase Kepuasan (%)")
    axes[1].set_title("Tingkat Kepuasan per Kategori (Rating ≥ 4)", fontweight="bold")
    axes[1].set_ylim(0, 100)
    axes[1].axhline(y=80, color="orange", linestyle="--", alpha=0.7, label="Target (80%)")
    axes[1].legend()
    axes[1].tick_params(axis="x", rotation=15)
    axes[1].grid(axis="y", linestyle="--", alpha=0.3)

    simpan("q6_rating_kategori")
    return hasil


# ============================================================
# QUERY 7 – Breakdown Rating (1-5 stars) per Kategori Feedback
#            Operasi OLAP: Slice (has_feedback=1) + Pivot Rating
# ============================================================
def q7_rating_breakdown(spark):
    print("\n=== Q7: Breakdown Rating (1-5 Bintang) per Kategori ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dk   = baca(spark, "dim_feedback_kategori")

    hasil = (
        fact.filter(col("has_feedback") == True)
        .join(dk, "feedback_kategori_key")
        .groupBy("kategori_nama", "rating")
        .agg(
            count("transaksi_key").alias("jml_feedback"),
        )
        .orderBy("kategori_nama", "rating")
    )
    hasil.show(20)

    # Contoh output:
    # +----------------------+------+-------------+
    # |kategori_nama         |rating|jml_feedback |
    # +----------------------+------+-------------+
    # |Aplikasi Jaklingko    |1     |142          |
    # |Aplikasi Jaklingko    |2     |356          |
    # |Aplikasi Jaklingko    |3     |743          |
    # |Aplikasi Jaklingko    |4     |1850         |
    # |Aplikasi Jaklingko    |5     |1189         |
    # |Fasilitas Busway      |1     |198          |
    # ...

    pd_h = to_pandas_float(hasil)
    
    # Handle empty result
    if len(pd_h) == 0:
        print("[WARNING] Q7: Tidak ada data feedback. Skip visualization.")
        return hasil
    
    pivot = pd_h.pivot(index="kategori_nama", columns="rating", values="jml_feedback").fillna(0)

    fig, ax = plt.subplots(figsize=(11, 5))
    pivot.plot(kind="bar", ax=ax, color=["#d32f2f","#f57c00","#fbc02d","#7cb342","#388e3c"],
               edgecolor="white", linewidth=1.5)
    ax.set_title("Distribusi Rating (1-5 Bintang) per Kategori Feedback", fontweight="bold")
    ax.set_xlabel("Kategori Feedback")
    ax.set_ylabel("Jumlah Feedback")
    ax.legend(title="Rating", labels=["⭐","⭐⭐","⭐⭐⭐","⭐⭐⭐⭐","⭐⭐⭐⭐⭐"])
    ax.tick_params(axis="x", rotation=15)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    simpan("q7_rating_breakdown")
    return hasil


# ============================================================
# QUERY 8 – Analisis Feedback Volume per Rute & Rating Satisfaction
#            Operasi OLAP: Dice (rute dengan feedback > 100) + Ranking
# ============================================================
def q8_feedback_per_rute(spark):
    print("\n=== Q8: Analisis Feedback per Rute (Top 8) ===")
    fact = baca(spark, "fact_transaksi_perjalanan")
    dr   = baca(spark, "dim_rute")

    hasil = (
        fact.filter(col("has_feedback") == True)
        .join(dr, "rute_key")
        .groupBy("rute_id", "nama_rute", "jenis_moda")
        .agg(
            count("transaksi_key").alias("jml_feedback"),
            _round(avg("rating"), 2).alias("avg_rating"),
            _sum(F.when(col("rating") >= 4, 1).otherwise(0)).alias("jml_satisfied"),
        )
        .withColumn(
            "pct_satisfied",
            F.round((col("jml_satisfied") / col("jml_feedback")) * 100, 1)
        )
        .filter(col("jml_feedback") > 50)
        .orderBy(col("avg_rating").desc())
    )
    hasil.show(8)

    # Contoh output:
    # +--------+-----------------------------+-----------+-------------+----------+---------------+---------------+
    # |rute_id |nama_rute                    |jenis_moda |jml_feedback |avg_rating|jml_satisfied  |pct_satisfied  |
    # +--------+-----------------------------+-----------+-------------+----------+---------------+---------------+
    # |R002    |Lebak Bulus - Bundaran HI    |MRT        |845          |4.42      |742            |87.8           |
    # |R001    |Blok M - Kota                |BRT        |623          |4.21      |515            |82.7           |
    # |R003    |Cibubur - Dukuh Atas         |LRT        |456          |4.08      |362            |79.4           |
    # |R005    |Kampung Rambutan - Blok M    |Mikrotrans |312          |3.95      |234            |75.0           |
    # |R004    |Tanah Abang - Pulo Gadung    |Bus        |198          |3.87      |141            |71.2           |
    # +--------+-----------------------------+-----------+-------------+----------+---------------+---------------+

    pd_h = to_pandas_float(hasil).sort_values("avg_rating", ascending=True)
    
    # Handle empty result
    if len(pd_h) == 0:
        print("[WARNING] Q8: Tidak ada data feedback per rute. Skip visualization.")
        return hasil
    
    fig, ax = plt.subplots(figsize=(12, 5))

    bars = ax.barh(pd_h["nama_rute"], pd_h["avg_rating"],
                   color=WARNA[:len(pd_h)], edgecolor="black", linewidth=1.5)
    
    for i, (idx, row) in enumerate(pd_h.iterrows()):
        ax.text(row["avg_rating"] + 0.1, i, 
                f"{row['avg_rating']:.2f} ({int(row['pct_satisfied'])}% puas)",
                va="center", fontsize=9)

    ax.set_xlim(0, 5.5)
    ax.set_xlabel("Rata-rata Rating (1-5)")
    ax.set_title("Kepuasan Pengguna per Rute Transportasi", fontweight="bold")
    ax.axvline(x=4, color="green", linestyle="--", alpha=0.7, linewidth=2, label="Target (4.0)")
    ax.legend()
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    simpan("q8_feedback_per_rute")
    return hasil


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    spark = buat_spark()
    print("\n" + "="*60)
    print("  JAKLINGKO DATA WAREHOUSE – ANALISIS OLAP")
    print("="*60)
    
    print("\n[BAGIAN 1 - ANALISIS OPERASIONAL]")
    q1_pendapatan_per_bulan(spark)
    q2_per_rute(spark)
    q3_jam_sibuk(spark)
    q4_metode_bayar(spark)
    q5_top_halte(spark)
    
    print("\n[BAGIAN 2 - ANALISIS FEEDBACK & KEPUASAN PENGGUNA]")
    q6_rating_kategori(spark)
    q7_rating_breakdown(spark)
    q8_feedback_per_rute(spark)
    
    print(f"\n[SELESAI] Semua grafik tersimpan di: {OUTPUT_DIR}/")
    print("="*60)
    spark.stop()
