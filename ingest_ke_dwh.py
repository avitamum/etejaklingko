"""
============================================================
 JAKLINGKO DATA PIPELINE - UTS
 FILE  : ingest_ke_dwh.py
 DESC  : Ingest data LANGSUNG ke Data Warehouse (jaklingko_dwh)
         dari berbagai format file:
           - CSV   (.csv)
           - JSON  (.json)
           - Excel (.xlsx)

         Proses: baca file → validasi → transform → load ke DWH
         Target tabel DWH: fact_transaksi_perjalanan (via lookup dimensi)

 Teknologi: pandas + SQLAlchemy (lebih ringan dari PySpark)
 Install  : pip install pandas sqlalchemy pymysql openpyxl

 BUG YANG DIPERBAIKI:
   BUG-6 : Dict sumber_label memakai key "csv" (tanpa titik) padahal
            os.path.splitext selalu mengembalikan ekstensi dengan titik
            (misalnya ".csv"). Akibatnya .get(ext, ...) selalu miss dan
            sumber_label selalu jatuh ke fallback yang salah.
            Fix: semua key diawali titik → ".csv", ".json", dll.
============================================================

CONTOH FORMAT FILE YANG DIDUKUNG:
----------------------------------------------------------

1. CSV  → data/ingest/transaksi_baru.csv
transaksi_id,pengguna_id,rute_id,halte_naik_id,halte_turun_id,armada_id,waktu_tap_in,waktu_tap_out,tarif,diskon,total_bayar,metode_bayar,status
TRX-9001,a1b2c3d4-0001-0001-0001-000000000001,R001,H001,H006,A001,2024-07-01 07:15:00,2024-07-01 08:00:00,3500,0,3500,JakCard,Sukses
TRX-9002,a1b2c3d4-0002-0002-0002-000000000002,R002,H007,H008,A003,2024-07-01 08:05:00,2024-07-01 08:40:00,14000,1000,13000,GoPay,Sukses

2. JSON → data/ingest/transaksi_baru.json
[
  {
    "transaksi_id": "TRX-9003",
    "pengguna_id": "a1b2c3d4-0003-0003-0003-000000000003",
    "rute_id": "R003",
    "halte_naik_id": "H009",
    "halte_turun_id": "H005",
    "armada_id": "A004",
    "waktu_tap_in": "2024-07-01 09:00:00",
    "waktu_tap_out": "2024-07-01 09:50:00",
    "tarif": 5000,
    "diskon": 0,
    "total_bayar": 5000,
    "metode_bayar": "OVO",
    "status": "Sukses"
  }
]

3. Excel → data/ingest/transaksi_baru.xlsx
   (sheet pertama, header di baris 1, kolom sama dengan CSV)
----------------------------------------------------------
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, date
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

BATCH_ID = datetime.now().strftime("INGEST_%Y%m%d_%H%M%S")

# ============================================================
# KONEKSI KE DWH (MySQL Workbench)
# ============================================================
def engine_dwh():
    url = ("mysql+pymysql://dwh_user:dwh_pass"
           "@localhost:3306/jaklingko_dwh?charset=utf8mb4")
    return create_engine(url, pool_pre_ping=True)


# ============================================================
# BACA FILE – MULTI FORMAT
# ============================================================
def baca_file(path: str) -> pd.DataFrame:
    """
    Baca file berdasarkan ekstensi: .csv | .json | .xlsx
    Semua dikembalikan sebagai DataFrame dengan kolom seragam.
    """
    ext = os.path.splitext(path)[1].lower()
    log.info(f"Membaca file: {path} (format: {ext})")

    if ext == ".csv":
        df = pd.read_csv(path, encoding="utf-8")

    elif ext == ".json":
        # Dukung dua bentuk JSON: array of objects, atau JSON Lines
        try:
            df = pd.read_json(path, orient="records")
        except ValueError:
            df = pd.read_json(path, lines=True)   # JSON Lines (satu objek per baris)

    elif ext in (".xlsx", ".xls"):
        # Baca sheet pertama; skip baris kosong di awal
        df = pd.read_excel(path, sheet_name=0, engine="openpyxl", skiprows=0)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    else:
        raise ValueError(f"Format tidak didukung: {ext}. Gunakan .csv / .json / .xlsx")

    log.info(f"  → {len(df)} baris, {len(df.columns)} kolom")
    return df


# ============================================================
# VALIDASI & CLEANING
# ============================================================
KOLOM_WAJIB_TRANSAKSI = [
    "transaksi_id", "pengguna_id", "rute_id",
    "halte_naik_id", "waktu_tap_in", "total_bayar", "metode_bayar", "status"
]

TIPE_KOLOM = {
    "tarif"      : "float",
    "diskon"     : "float",
    "total_bayar": "float",
}

STATUS_VALID    = {"Sukses", "Gagal", "Pending", "Refund"}
METODE_VALID    = {"JakCard","GoPay","OVO","Dana","ShopeePay","LinkAja","Tunai","Kartu Kredit"}


def validasi_dan_bersihkan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validasi kolom wajib, tipe data, nilai enum, dan duplikat.
    Mengembalikan DataFrame yang sudah bersih.
    """
    # 1. Cek kolom wajib
    hilang = [k for k in KOLOM_WAJIB_TRANSAKSI if k not in df.columns]
    if hilang:
        raise ValueError(f"Kolom wajib tidak ditemukan: {hilang}")

    awal = len(df)

    # 2. Trim whitespace pada semua kolom string
    for c in df.select_dtypes("object").columns:
        df[c] = df[c].astype(str).str.strip()

    # 3. Drop baris tanpa FK kritis atau transaksi_id
    df = df.dropna(subset=KOLOM_WAJIB_TRANSAKSI)

    # 4. Isi nilai hilang non-kritis
    df["halte_turun_id"] = df.get("halte_turun_id", pd.Series(dtype=str)).fillna("UNKNOWN")
    df["armada_id"]      = df.get("armada_id",      pd.Series(dtype=str)).fillna("UNKNOWN")
    df["diskon"]         = pd.to_numeric(df.get("diskon", 0), errors="coerce").fillna(0)

    # 5. Konversi tipe
    for kolom, tipe in TIPE_KOLOM.items():
        if kolom in df.columns:
            df[kolom] = pd.to_numeric(df[kolom], errors="coerce")

    # 6. Parse datetime
    for dt_col in ["waktu_tap_in", "waktu_tap_out"]:
        if dt_col in df.columns:
            df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")

    # 7. Validasi enum status & metode_bayar
    mask_status = ~df["status"].isin(STATUS_VALID)
    if mask_status.any():
        log.warning(f"  {mask_status.sum()} baris dibuang: status tidak valid")
        df = df[~mask_status]

    mask_metode = ~df["metode_bayar"].isin(METODE_VALID)
    if mask_metode.any():
        log.warning(f"  {mask_metode.sum()} baris dibuang: metode_bayar tidak valid")
        df = df[~mask_metode]

    # 8. Deduplikasi berdasarkan transaksi_id
    df = df.drop_duplicates(subset=["transaksi_id"])

    log.info(f"  Validasi: {awal} → {len(df)} baris bersih")
    return df.reset_index(drop=True)


# ============================================================
# LOOKUP DIMENSI (Ambil Surrogate Key dari DWH)
# ============================================================
def ambil_dim_lookup(engine) -> dict:
    """
    Ambil peta Natural Key → Surrogate Key dari semua tabel dimensi DWH.
    Returns dict berisi DataFrame lookup per dimensi.
    """
    with engine.connect() as conn:
        dim_waktu = pd.read_sql(
            "SELECT waktu_key, tanggal FROM dim_waktu WHERE is_hari_libur IS NOT NULL",
            conn
        )
        dim_halte = pd.read_sql(
            "SELECT halte_key, halte_id FROM dim_halte WHERE is_current = 1",
            conn
        )
        dim_rute = pd.read_sql(
            "SELECT rute_key, rute_id FROM dim_rute WHERE is_current = 1",
            conn
        )
        dim_pengguna = pd.read_sql(
            "SELECT pengguna_key, pengguna_id FROM dim_pengguna WHERE is_current = 1",
            conn
        )

    # Konversi tanggal ke tipe date untuk merge
    dim_waktu["tanggal"] = pd.to_datetime(dim_waktu["tanggal"]).dt.date

    log.info(f"  Lookup dim_waktu    : {len(dim_waktu)} baris")
    log.info(f"  Lookup dim_halte    : {len(dim_halte)} baris")
    log.info(f"  Lookup dim_rute     : {len(dim_rute)} baris")
    log.info(f"  Lookup dim_pengguna : {len(dim_pengguna)} baris")

    return {
        "waktu"    : dim_waktu,
        "halte"    : dim_halte,
        "rute"     : dim_rute,
        "pengguna" : dim_pengguna,
    }


# ============================================================
# TRANSFORM: Bangun Baris Fact dari Data Mentah
# ============================================================
def transform_ke_fact(df: pd.DataFrame, lookup: dict,
                      sumber: str) -> pd.DataFrame:
    """
    Gabungkan data transaksi bersih dengan surrogate key dimensi.

    Parameters
    ----------
    df      : DataFrame transaksi yang sudah bersih
    lookup  : Dict lookup dimensi (dari ambil_dim_lookup)
    sumber  : Label sumber data ('csv', 'json', 'excel')
    """
    # Kolom turunan
    df["tanggal_perjalanan"] = df["waktu_tap_in"].dt.date
    df["jam_tap_in"] = df["waktu_tap_in"].dt.hour
    df["sesi_hari"] = df["jam_tap_in"].apply(
        lambda j: "Pagi"  if 5 <= j <= 9 else
                  "Siang" if 10 <= j <= 14 else
                  "Sore"  if 15 <= j <= 18 else "Malam"
    )
    df["durasi_menit"] = (
        (df["waktu_tap_out"] - df["waktu_tap_in"])
        .dt.total_seconds()
        .div(60)
        .round(0)
        .astype("Int64")
    )
    df["is_sukses"] = (df["status"] == "Sukses").astype(int)

    # ── Join ke dim_waktu ────────────────────────────────
    df = df.merge(
        lookup["waktu"].rename(columns={"tanggal": "tanggal_perjalanan",
                                         "waktu_key": "waktu_key"}),
        on="tanggal_perjalanan", how="left"
    )

    # ── Join ke dim_halte (naik) ─────────────────────────
    df = df.merge(
        lookup["halte"].rename(columns={"halte_id": "halte_naik_id",
                                         "halte_key": "halte_naik_key"}),
        on="halte_naik_id", how="left"
    )

    # ── Join ke dim_halte (turun) ────────────────────────
    df = df.merge(
        lookup["halte"].rename(columns={"halte_id": "halte_turun_id",
                                         "halte_key": "halte_turun_key"}),
        on="halte_turun_id", how="left"
    )

    # ── Join ke dim_rute ─────────────────────────────────
    df = df.merge(
        lookup["rute"].rename(columns={"rute_id": "rute_id",
                                        "rute_key": "rute_key"}),
        on="rute_id", how="left"
    )

    # ── Join ke dim_pengguna ─────────────────────────────
    df = df.merge(
        lookup["pengguna"].rename(columns={"pengguna_id": "pengguna_id",
                                            "pengguna_key": "pengguna_key"}),
        on="pengguna_id", how="left"
    )

    # Drop baris yang gagal lookup dimensi kritis
    sebelum = len(df)
    df = df.dropna(subset=["waktu_key", "rute_key", "pengguna_key", "halte_naik_key"])
    if len(df) < sebelum:
        log.warning(f"  {sebelum - len(df)} baris dibuang: gagal lookup dimensi")

    # ── Susun kolom sesuai skema fact table ──────────────
    kolom_fact = [
        "waktu_key", "halte_naik_key", "halte_turun_key",
        "rute_key", "pengguna_key",
        "transaksi_id", "armada_id",
        "waktu_tap_in", "waktu_tap_out",
        "jam_tap_in", "sesi_hari",
        "tarif", "diskon", "total_bayar", "durasi_menit",
        "metode_bayar", "status_transaksi", "is_sukses",
        "sumber_data", "etl_batch_id",
    ]

    df["status_transaksi"] = df["status"]
    df["sumber_data"]      = sumber
    df["etl_batch_id"]     = BATCH_ID

    # Konversi tipe agar kompatibel MySQL
    for k in ["waktu_key","halte_naik_key","rute_key","pengguna_key"]:
        df[k] = df[k].astype("Int64")
    df["halte_turun_key"] = df["halte_turun_key"].astype("Int64")

    df_fact = df[[c for c in kolom_fact if c in df.columns]].copy()
    log.info(f"  Fact siap: {len(df_fact)} baris (sumber: {sumber})")
    return df_fact


# ============================================================
# LOAD KE DWH
# ============================================================
def cek_duplikat_dwh(df_fact: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Cek apakah transaksi_id sudah ada di DWH.
    Buang baris yang sudah ada (hindari duplicate key error).
    """
    ids = tuple(df_fact["transaksi_id"].tolist())
    if not ids:
        return df_fact

    # Jika hanya 1 element, hindari syntax SQL yang salah
    placeholder = f"('{ids[0]}')" if len(ids) == 1 else str(ids)

    with engine.connect() as conn:
        existing = pd.read_sql(
            f"SELECT transaksi_id FROM fact_transaksi_perjalanan "
            f"WHERE transaksi_id IN {placeholder}",
            conn
        )

    sudah_ada = set(existing["transaksi_id"])
    df_baru = df_fact[~df_fact["transaksi_id"].isin(sudah_ada)]
    if len(df_baru) < len(df_fact):
        log.info(f"  {len(df_fact) - len(df_baru)} transaksi sudah ada di DWH, di-skip")
    return df_baru


def muat_ke_dwh(df_fact: pd.DataFrame, engine):
    """Tulis DataFrame fact ke tabel DWH MySQL."""
    if df_fact.empty:
        log.info("  Tidak ada data baru untuk dimuat.")
        return

    try:
        df_fact.to_sql(
            name="fact_transaksi_perjalanan",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=500,
            method="multi"
        )
        log.info(f"  ✓ {len(df_fact)} baris berhasil dimuat ke DWH")
    except SQLAlchemyError as e:
        log.error(f"  ✗ Gagal muat ke DWH: {e}")
        raise


# ============================================================
# FUNGSI PUBLIK UTAMA
# ============================================================
def ingest_ke_dwh(path_file: str):
    """
    Pipeline lengkap ingest satu file (CSV/JSON/Excel) ke DWH.

    Parameters
    ----------
    path_file : Path lengkap ke file input
    """
    ext = os.path.splitext(path_file)[1].lower()

    # BUG-6 FIX: key dict harus pakai titik karena os.path.splitext
    # mengembalikan ekstensi dengan titik (".csv", ".json", ".xlsx").
    # Sebelumnya key pertama adalah "csv" (tanpa titik) sehingga
    # .get(".csv", ...) selalu miss dan sumber_label selalu salah.
    sumber_label = {
        ".csv" : "csv",
        ".json": "json",
        ".xlsx": "excel",
        ".xls" : "excel",
    }.get(ext, ext.lstrip("."))

    log.info("=" * 55)
    log.info(f" INGEST KE DWH  | File: {os.path.basename(path_file)}")
    log.info(f" Batch ID       : {BATCH_ID}")
    log.info("=" * 55)

    engine = engine_dwh()

    # 1. Baca file
    df_raw = baca_file(path_file)

    # 2. Validasi & cleaning
    df_bersih = validasi_dan_bersihkan(df_raw)

    # 3. Ambil lookup dimensi dari DWH
    log.info("Mengambil lookup dimensi dari DWH...")
    lookup = ambil_dim_lookup(engine)

    # 4. Transform → bangun baris fact
    df_fact = transform_ke_fact(df_bersih, lookup, sumber_label)

    # 5. Cek duplikat dengan data yang sudah ada di DWH
    df_fact = cek_duplikat_dwh(df_fact, engine)

    # 6. Load ke DWH
    muat_ke_dwh(df_fact, engine)

    engine.dispose()
    log.info(f" Ingest selesai: {path_file}")
    log.info("=" * 55)


# ============================================================
# INGEST BATCH: Semua file dalam satu folder
# ============================================================
def ingest_folder(folder: str, ekstensi=(".csv", ".json", ".xlsx")):
    """
    Ingest semua file dalam folder dengan ekstensi yang cocok.
    Berguna untuk memproses banyak file sekaligus.

    Parameters
    ----------
    folder    : Path folder yang berisi file data
    ekstensi  : Tuple ekstensi file yang akan diproses
    """
    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith(ekstensi)
    ]

    if not files:
        log.info(f"Tidak ada file ditemukan di: {folder}")
        return

    log.info(f"Ditemukan {len(files)} file di {folder}")
    berhasil, gagal = 0, 0

    for f in sorted(files):
        try:
            ingest_ke_dwh(f)
            berhasil += 1
        except Exception as e:
            log.error(f"  ✗ Gagal proses {f}: {e}")
            gagal += 1

    log.info(f"\nRingkasan: {berhasil} berhasil, {gagal} gagal dari {len(files)} file")


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Mode CLI: python ingest_ke_dwh.py <path_file_atau_folder>
        target = sys.argv[1]
        if os.path.isdir(target):
            ingest_folder(target)
        elif os.path.isfile(target):
            ingest_ke_dwh(target)
        else:
            log.error(f"Path tidak ditemukan: {target}")
    else:
        # Demo: ingest dari semua format contoh
        contoh_files = [
            "data/ingest/transaksi_baru.csv",
            "data/ingest/transaksi_tambahan.json",
            "data/ingest/transaksi_excel.xlsx",
        ]
        for f in contoh_files:
            if os.path.exists(f):
                ingest_ke_dwh(f)
            else:
                log.warning(f"File contoh tidak ditemukan: {f}")
