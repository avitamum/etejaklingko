"""
============================================================
 JAKLINGKO DATA PIPELINE - UTS
 FILE  : ingest_sumber_csv.py
 DESC  : Ingest CSV ke database OPERASIONAL
         - rute.csv, halte.csv, armada.csv  → MySQL  (jaklingko_mysql)
         - pengguna.csv, transaksi.csv       → PostgreSQL (jaklingko_pg)

 Install: pip install pandas sqlalchemy pymysql psycopg2-binary openpyxl
============================================================

CONTOH FORMAT CSV:
----------------------------------------------------------
# rute.csv
rute_id,nama_rute,jenis_moda,asal,tujuan,jarak_km,tarif_dasar,status
R001,Blok M - Kota,BRT,Blok M,Kota,14.50,3500,Aktif
R002,Lebak Bulus - Bundaran HI,MRT,Lebak Bulus,Bundaran HI,15.70,14000,Aktif

# halte.csv
halte_id,nama_halte,rute_id,urutan_halte,alamat,latitude,longitude,fasilitas,status
H001,Halte Blok M,R001,1,Jl. Melawai Raya,-6.2437622,106.7993430,"Toilet,Kursi Tunggu,WiFi",Aktif

# armada.csv
armada_id,nomor_polisi,rute_id,jenis_armada,kapasitas,tahun_buat,kondisi
A001,B 1234 TRA,R001,Transjakarta Feeder,85,2020,Baik

# pengguna.csv
pengguna_id,nama_lengkap,email,no_telepon,gender,tanggal_lahir,saldo_jakcard,kota_domisili,tanggal_daftar
a1b2c3d4-0001-0001-0001-000000000001,Andi Pratama,andi@email.com,081234567001,Laki-laki,1995-03-12,125000,Jakarta Selatan,2023-01-10

# transaksi.csv
transaksi_id,pengguna_id,rute_id,halte_naik_id,halte_turun_id,armada_id,waktu_tap_in,waktu_tap_out,tarif,diskon,total_bayar,metode_bayar,status
b1c2d3e4-0001-...,a1b2c3d4-0001-...,R001,H001,H006,A001,2024-06-01 07:10:00,2024-06-01 07:55:00,3500,0,3500,JakCard,Sukses
----------------------------------------------------------
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ─── Koneksi ──────────────────────────────────────────────
def engine_mysql_sumber():
    url = ("mysql+pymysql://jaklingko_user:jaklingko_pass"
           "@localhost:3306/jaklingko_mysql?charset=utf8mb4")
    return create_engine(url, pool_pre_ping=True)


def engine_postgres_sumber():
    url = ("postgresql+psycopg2://jaklingko_user:jaklingko_pass"
           "@localhost:5432/jaklingko_pg")
    return create_engine(url, pool_pre_ping=True)


# ─── Utilitas ─────────────────────────────────────────────
def baca_dan_validasi(path, kolom_wajib, parse_dates=None, dtype=None):
    log.info(f"Membaca: {path}")
    df = pd.read_csv(path, parse_dates=parse_dates, dtype=dtype, encoding="utf-8")
    hilang = [k for k in kolom_wajib if k not in df.columns]
    if hilang:
        raise ValueError(f"Kolom wajib tidak ada: {hilang}")
    awal = len(df)
    df = df.drop_duplicates().dropna(subset=kolom_wajib)
    for c in df.select_dtypes("object").columns:
        df[c] = df[c].str.strip()
    log.info(f"  {awal} → {len(df)} baris (setelah validasi)")
    return df


def muat_ke_db(df, engine, tabel, if_exists="append"):
    try:
        df.to_sql(tabel, con=engine, if_exists=if_exists,
                  index=False, chunksize=500, method="multi")
        log.info(f"  ✓ {len(df)} baris → {tabel}")
    except SQLAlchemyError as e:
        log.error(f"  ✗ Gagal → {tabel}: {e}")
        raise


# ─── Ingest MySQL Sumber ──────────────────────────────────
def ingest_mysql_sumber():
    eng = engine_mysql_sumber()

    df_rute = baca_dan_validasi("data/csv/rute.csv", ["rute_id","nama_rute","jenis_moda"])
    muat_ke_db(df_rute, eng, "rute")

    df_halte = baca_dan_validasi("data/csv/halte.csv", ["halte_id","nama_halte","rute_id"],
                                  dtype={"latitude":float,"longitude":float})
    muat_ke_db(df_halte, eng, "halte")

    df_armada = baca_dan_validasi("data/csv/armada.csv", ["armada_id","nomor_polisi","rute_id"])
    muat_ke_db(df_armada, eng, "armada")

    eng.dispose()
    log.info("=== Ingest MySQL sumber selesai ===")


# ─── Ingest PostgreSQL Sumber ─────────────────────────────
def ingest_postgres_sumber():
    eng = engine_postgres_sumber()

    df_p = baca_dan_validasi("data/csv/pengguna.csv",
                              ["pengguna_id","email","no_telepon"],
                              parse_dates=["tanggal_lahir","tanggal_daftar"])
    muat_ke_db(df_p, eng, "pengguna")

    df_t = baca_dan_validasi("data/csv/transaksi.csv",
                              ["transaksi_id","pengguna_id","rute_id","waktu_tap_in"],
                              parse_dates=["waktu_tap_in","waktu_tap_out"])
    muat_ke_db(df_t, eng, "transaksi")

    eng.dispose()
    log.info("=== Ingest PostgreSQL sumber selesai ===")


if __name__ == "__main__":
    ingest_mysql_sumber()
    ingest_postgres_sumber()
