-- ============================================================
--  JAKLINGKO DATA PIPELINE - UTS
--  FILE   : mysql_dwh_star_schema.sql
--  DATABASE: jaklingko_dwh  (Target DWH - MySQL Workbench)
--  DESC   : Data Warehouse – Star Schema
--           Semua sintaks kompatibel 100% dengan MySQL
--
--  STAR SCHEMA:
--    FACT : fact_transaksi_perjalanan
--    DIM  : dim_waktu | dim_halte | dim_rute | dim_pengguna
-- ============================================================

CREATE DATABASE IF NOT EXISTS jaklingko_dwh
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE jaklingko_dwh;

-- ============================================================
-- DIMENSI 1 : dim_waktu
-- Grain      : 1 baris per tanggal kalender
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_waktu (
    waktu_key            INT          NOT NULL AUTO_INCREMENT,
    tanggal              DATE         NOT NULL,
    tahun                SMALLINT     NOT NULL,
    kuartal              TINYINT      NOT NULL COMMENT '1-4',
    bulan                TINYINT      NOT NULL COMMENT '1-12',
    nama_bulan           VARCHAR(15)  NOT NULL,
    minggu_dalam_tahun   TINYINT      NOT NULL,
    hari_dalam_bulan     TINYINT      NOT NULL,
    nama_hari            VARCHAR(15)  NOT NULL,
    is_weekend           TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '1=Ya 0=Tidak',
    is_hari_libur        TINYINT(1)   NOT NULL DEFAULT 0,
    keterangan_libur     VARCHAR(100),
    PRIMARY KEY (waktu_key),
    UNIQUE KEY uq_tanggal (tanggal),
    INDEX idx_tahun_bulan (tahun, bulan)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimensi waktu – satu baris per hari kalender';

-- ============================================================
-- DIMENSI 2 : dim_halte
-- SCD Type 2 : lacak perubahan atribut halte
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_halte (
    halte_key     INT           NOT NULL AUTO_INCREMENT,
    halte_id      VARCHAR(10)   NOT NULL COMMENT 'Natural Key dari MySQL sumber',
    nama_halte    VARCHAR(150)  NOT NULL,
    alamat        VARCHAR(255),
    latitude      DECIMAL(10,7),
    longitude     DECIMAL(10,7),
    fasilitas     VARCHAR(200),
    kota          VARCHAR(100),
    wilayah       VARCHAR(100),
    status        VARCHAR(20)   NOT NULL DEFAULT 'Aktif',
    -- SCD Type 2
    valid_dari    DATE          NOT NULL,
    valid_sampai  DATE,
    is_current    TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (halte_key),
    INDEX idx_halte_id  (halte_id),
    INDEX idx_is_current(is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimensi halte (SCD Type 2)';

-- ============================================================
-- DIMENSI 3 : dim_rute
-- SCD Type 2 : lacak perubahan tarif / atribut rute
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_rute (
    rute_key      INT           NOT NULL AUTO_INCREMENT,
    rute_id       VARCHAR(10)   NOT NULL COMMENT 'Natural Key dari MySQL sumber',
    nama_rute     VARCHAR(100)  NOT NULL,
    jenis_moda    VARCHAR(20)   NOT NULL,
    asal          VARCHAR(100)  NOT NULL,
    tujuan        VARCHAR(100)  NOT NULL,
    jarak_km      DECIMAL(6,2)  NOT NULL,
    koridor       VARCHAR(50),
    tarif_dasar   INT           NOT NULL,
    status        VARCHAR(20)   NOT NULL DEFAULT 'Aktif',
    -- SCD Type 2
    valid_dari    DATE          NOT NULL,
    valid_sampai  DATE,
    is_current    TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (rute_key),
    INDEX idx_rute_id   (rute_id),
    INDEX idx_is_current(is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimensi rute transportasi (SCD Type 2)';

-- ============================================================
-- DIMENSI 4 : dim_pengguna
-- SCD Type 2 : lacak perubahan domisili / segmen pengguna
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_pengguna (
    pengguna_key     INT           NOT NULL AUTO_INCREMENT,
    pengguna_id      VARCHAR(36)   NOT NULL COMMENT 'UUID dari PostgreSQL sumber',
    nama_lengkap     VARCHAR(150)  NOT NULL,
    gender           VARCHAR(20)   NOT NULL,
    kelompok_usia    VARCHAR(20)   NOT NULL COMMENT '< 20, 20-30, 31-40, > 40',
    kota_domisili    VARCHAR(100),
    wilayah          VARCHAR(50)   NOT NULL COMMENT 'Jabodetabek / Lainnya',
    segment_pengguna VARCHAR(30)   NOT NULL COMMENT 'Pelajar, Mahasiswa, Pekerja, Lansia',
    tanggal_daftar   DATE          NOT NULL,
    -- SCD Type 2
    valid_dari       DATE          NOT NULL,
    valid_sampai     DATE,
    is_current       TINYINT(1)    NOT NULL DEFAULT 1,
    PRIMARY KEY (pengguna_key),
    INDEX idx_pengguna_id (pengguna_id),
    INDEX idx_is_current  (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimensi pengguna JakLingko (SCD Type 2)';

-- ============================================================
-- DIMENSI 5 : dim_feedback_kategori
-- Daftar kategori feedback untuk dimensionalitas
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_feedback_kategori (
    feedback_kategori_key  INT          NOT NULL AUTO_INCREMENT,
    kategori_nama          VARCHAR(50)  NOT NULL UNIQUE,
    deskripsi              VARCHAR(255),
    PRIMARY KEY (feedback_kategori_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimensi kategori feedback';

-- ============================================================
-- FACT : fact_transaksi_perjalanan
-- Grain: 1 baris = 1 perjalanan (tap-in s.d. tap-out)
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_transaksi_perjalanan (
    transaksi_key       BIGINT        NOT NULL AUTO_INCREMENT,

    -- Foreign Keys ke Dimensi
    waktu_key           INT           NOT NULL,
    halte_naik_key      INT           NOT NULL,
    halte_turun_key     INT,
    rute_key            INT           NOT NULL,
    pengguna_key        INT           NOT NULL,
    feedback_kategori_key INT,

    -- Degenerate Dimension (ID asli dari sumber)
    transaksi_id        VARCHAR(36)   NOT NULL COMMENT 'UUID dari PostgreSQL',
    armada_id           VARCHAR(10),

    -- Waktu detail (granularitas lebih halus dari dim_waktu)
    waktu_tap_in        DATETIME      NOT NULL,
    waktu_tap_out       DATETIME,
    jam_tap_in          TINYINT       NOT NULL COMMENT '0-23',
    sesi_hari           VARCHAR(10)   NOT NULL COMMENT 'Pagi/Siang/Sore/Malam',

    -- Measures (additive)
    tarif               DECIMAL(10,2) NOT NULL,
    diskon              DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    total_bayar         DECIMAL(10,2) NOT NULL,
    durasi_menit        SMALLINT,
    jarak_tempuh_km     DECIMAL(6,2),

    -- Feedback measures
    rating              TINYINT       COMMENT '1-5, NULL jika tidak ada feedback',
    komentar            TEXT,

    -- Atribut non-additive
    metode_bayar        VARCHAR(30)   NOT NULL,
    status_transaksi    VARCHAR(20)   NOT NULL,
    is_sukses           TINYINT(1)    NOT NULL DEFAULT 0,
    has_feedback        TINYINT(1)    NOT NULL DEFAULT 0,

    -- Metadata ETL
    sumber_data         VARCHAR(30)   NOT NULL COMMENT 'postgresql / csv / excel / json',
    etl_batch_id        VARCHAR(60)   NOT NULL,
    etl_loaded_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (transaksi_key),
    UNIQUE KEY uq_transaksi_id (transaksi_id),
    FOREIGN KEY (waktu_key)           REFERENCES dim_waktu(waktu_key),
    FOREIGN KEY (halte_naik_key)      REFERENCES dim_halte(halte_key),
    FOREIGN KEY (rute_key)            REFERENCES dim_rute(rute_key),
    FOREIGN KEY (pengguna_key)        REFERENCES dim_pengguna(pengguna_key),
    FOREIGN KEY (feedback_kategori_key) REFERENCES dim_feedback_kategori(feedback_kategori_key),
    INDEX idx_waktu_key     (waktu_key),
    INDEX idx_rute_key      (rute_key),
    INDEX idx_pengguna_key  (pengguna_key),
    INDEX idx_tap_in        (waktu_tap_in),
    INDEX idx_sumber_data   (sumber_data),
    INDEX idx_rating        (rating),
    INDEX idx_has_feedback  (has_feedback)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Fact table perjalanan + feedback – grain: 1 baris per perjalanan';

-- ============================================================
-- TABEL AUDIT ETL
-- Melacak setiap eksekusi batch pipeline
-- ============================================================
CREATE TABLE IF NOT EXISTS etl_log (
    log_id         INT          NOT NULL AUTO_INCREMENT,
    batch_id       VARCHAR(60)  NOT NULL,
    nama_proses    VARCHAR(100) NOT NULL,
    sumber         VARCHAR(50)  NOT NULL COMMENT 'mysql/postgresql/csv/excel/json',
    tabel_target   VARCHAR(60)  NOT NULL,
    jumlah_baris   INT          NOT NULL DEFAULT 0,
    status         ENUM('Sukses','Gagal','Partial') NOT NULL,
    pesan_error    TEXT,
    mulai_at       DATETIME     NOT NULL,
    selesai_at     DATETIME,
    PRIMARY KEY (log_id),
    INDEX idx_batch_id (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Log audit setiap eksekusi ETL batch';

-- ============================================================
-- SEED DATA - KATEGORI FEEDBACK
-- ============================================================
INSERT INTO dim_feedback_kategori (kategori_nama, deskripsi) VALUES
  ('Umum', 'Feedback umum tentang layanan secara keseluruhan'),
  ('Fasilitas Busway', 'Feedback tentang fasilitas halte dan armada'),
  ('Aplikasi Jaklingko', 'Feedback tentang aplikasi mobile JakLingko');
