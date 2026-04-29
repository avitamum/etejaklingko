-- ============================================================
--  JAKLINGKO DATA PIPELINE - UTS
--  FILE   : postgresql_sumber.sql
--  DATABASE: jaklingko_pg  (Sumber 2 - PostgreSQL)
--  DESC   : DDL database operasional PostgreSQL
--           Menyimpan data transaksional: Pengguna & Transaksi
-- ============================================================

-- Jalankan sebagai superuser:
-- CREATE DATABASE jaklingko_pg;
-- \c jaklingko_pg

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE metode_bayar_enum AS ENUM
  ('JakCard','GoPay','OVO','Dana','ShopeePay','LinkAja','Tunai','Kartu Kredit');

CREATE TYPE status_trx_enum AS ENUM
  ('Sukses','Gagal','Pending','Refund');

CREATE TYPE gender_enum AS ENUM
  ('Laki-laki','Perempuan','Tidak Disebutkan');

CREATE TYPE kategori_feedback_enum AS ENUM
  ('Umum','Fasilitas Busway','Aplikasi Jaklingko');

-- ------------------------------------------------------------
-- Tabel 1: pengguna
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pengguna (
    pengguna_id    UUID          NOT NULL DEFAULT uuid_generate_v4(),
    nama_lengkap   VARCHAR(150)  NOT NULL,
    email          VARCHAR(200)  NOT NULL UNIQUE,
    no_telepon     VARCHAR(20)   NOT NULL UNIQUE,
    gender         gender_enum   NOT NULL DEFAULT 'Tidak Disebutkan',
    tanggal_lahir  DATE,
    saldo_jakcard  NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    kota_domisili  VARCHAR(100),
    tanggal_daftar TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    terakhir_login TIMESTAMPTZ,
    is_aktif       BOOLEAN       NOT NULL DEFAULT TRUE,
    PRIMARY KEY (pengguna_id)
);

CREATE INDEX idx_pengguna_email ON pengguna(email);
CREATE INDEX idx_pengguna_kota  ON pengguna(kota_domisili);

-- ------------------------------------------------------------
-- Tabel 2: transaksi
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transaksi (
    transaksi_id   UUID              NOT NULL DEFAULT uuid_generate_v4(),
    pengguna_id    UUID              NOT NULL,
    rute_id        VARCHAR(10)       NOT NULL,
    halte_naik_id  VARCHAR(10)       NOT NULL,
    halte_turun_id VARCHAR(10),
    armada_id      VARCHAR(10),
    waktu_tap_in   TIMESTAMPTZ       NOT NULL,
    waktu_tap_out  TIMESTAMPTZ,
    durasi_menit   SMALLINT
        GENERATED ALWAYS AS (
            EXTRACT(EPOCH FROM (waktu_tap_out - waktu_tap_in))::INT / 60
        ) STORED,
    tarif          NUMERIC(10,2)     NOT NULL,
    diskon         NUMERIC(10,2)     NOT NULL DEFAULT 0.00,
    total_bayar    NUMERIC(10,2)     NOT NULL,
    metode_bayar   metode_bayar_enum NOT NULL,
    status         status_trx_enum   NOT NULL DEFAULT 'Sukses',
    device_id      VARCHAR(100),
    created_at     TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaksi_id),
    FOREIGN KEY (pengguna_id) REFERENCES pengguna(pengguna_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX idx_trx_pengguna ON transaksi(pengguna_id);
CREATE INDEX idx_trx_rute     ON transaksi(rute_id);
CREATE INDEX idx_trx_waktu    ON transaksi(waktu_tap_in);
CREATE INDEX idx_trx_status   ON transaksi(status);

-- ------------------------------------------------------------
-- Tabel 3: feedback
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS feedback (
    feedback_id    UUID                      NOT NULL DEFAULT uuid_generate_v4(),
    transaksi_id   UUID                      NOT NULL UNIQUE,
    pengguna_id    UUID                      NOT NULL,
    rating         SMALLINT                  NOT NULL CHECK (rating BETWEEN 1 AND 5),
    komentar       TEXT,
    kategori       kategori_feedback_enum    NOT NULL DEFAULT 'Umum',
    created_at     TIMESTAMPTZ               NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feedback_id),
    FOREIGN KEY (transaksi_id) REFERENCES transaksi(transaksi_id),
    FOREIGN KEY (pengguna_id)  REFERENCES pengguna(pengguna_id)
);

-- ============================================================
-- SEED DATA
-- ============================================================
INSERT INTO pengguna
  (pengguna_id, nama_lengkap, email, no_telepon, gender,
   tanggal_lahir, saldo_jakcard, kota_domisili, tanggal_daftar)
VALUES
  ('a1b2c3d4-0001-0001-0001-000000000001','Andi Pratama',
   'andi.pratama@email.com','081234567001','Laki-laki',
   '1995-03-12',125000,'Jakarta Selatan','2023-01-10'),
  ('a1b2c3d4-0002-0002-0002-000000000002','Budi Santoso',
   'budi.santoso@email.com','081234567002','Laki-laki',
   '1990-07-22',50000,'Jakarta Barat','2022-11-05'),
  ('a1b2c3d4-0003-0003-0003-000000000003','Citra Dewi',
   'citra.dewi@email.com','081234567003','Perempuan',
   '1998-12-01',200000,'Jakarta Timur','2023-03-20'),
  ('a1b2c3d4-0004-0004-0004-000000000004','Diana Kusuma',
   'diana.k@email.com','081234567004','Perempuan',
   '1993-09-15',75000,'Depok','2023-06-15'),
  ('a1b2c3d4-0005-0005-0005-000000000005','Eko Wibowo',
   'eko.wibowo@email.com','081234567005','Laki-laki',
   '1985-04-30',0,'Bekasi','2024-01-01');

INSERT INTO transaksi
  (transaksi_id, pengguna_id, rute_id, halte_naik_id, halte_turun_id,
   armada_id, waktu_tap_in, waktu_tap_out, tarif, diskon, total_bayar,
   metode_bayar, status)
VALUES
  ('b1c2d3e4-0001-0001-0001-000000000001',
   'a1b2c3d4-0001-0001-0001-000000000001',
   'R001','H001','H006','A001',
   '2024-06-01 07:10:00+07','2024-06-01 07:55:00+07',3500,0,3500,'JakCard','Sukses'),
  ('b1c2d3e4-0002-0002-0002-000000000002',
   'a1b2c3d4-0002-0002-0002-000000000002',
   'R002','H007','H008','A003',
   '2024-06-01 08:00:00+07','2024-06-01 08:35:00+07',14000,0,14000,'GoPay','Sukses'),
  ('b1c2d3e4-0003-0003-0003-000000000003',
   'a1b2c3d4-0003-0003-0003-000000000003',
   'R001','H002','H005','A002',
   '2024-06-01 07:30:00+07','2024-06-01 08:05:00+07',3500,500,3000,'OVO','Sukses'),
  ('b1c2d3e4-0004-0004-0004-000000000004',
   'a1b2c3d4-0004-0004-0004-000000000004',
   'R003','H009','H005','A004',
   '2024-06-01 09:00:00+07','2024-06-01 09:45:00+07',5000,0,5000,'Dana','Gagal'),
  ('b1c2d3e4-0005-0005-0005-000000000005',
   'a1b2c3d4-0005-0005-0005-000000000005',
   'R004','H010','H005','A005',
   '2024-06-01 06:45:00+07','2024-06-01 07:30:00+07',3500,0,3500,'Tunai','Sukses'),
  -- === TRANSAKSI TAMBAHAN untuk feedback (6-10) ===
  ('b1c2d3e4-0006-0006-0006-000000000006',
   'a1b2c3d4-0001-0001-0001-000000000001',
   'R002','H007','H008','A003',
   '2024-06-02 08:15:00+07','2024-06-02 08:50:00+07',14000,0,14000,'GoPay','Sukses'),
  ('b1c2d3e4-0007-0007-0007-000000000007',
   'a1b2c3d4-0002-0002-0002-000000000002',
   'R001','H001','H006','A001',
   '2024-06-02 07:20:00+07','2024-06-02 08:00:00+07',3500,0,3500,'JakCard','Sukses'),
  ('b1c2d3e4-0008-0008-0008-000000000008',
   'a1b2c3d4-0003-0003-0003-000000000003',
   'R003','H009','H005','A004',
   '2024-06-02 09:10:00+07','2024-06-02 10:00:00+07',5000,0,5000,'OVO','Sukses'),
  ('b1c2d3e4-0009-0009-0009-000000000009',
   'a1b2c3d4-0004-0004-0004-000000000004',
   'R004','H010','H005','A005',
   '2024-06-02 07:00:00+07','2024-06-02 07:45:00+07',3500,0,3500,'Dana','Sukses'),
  ('b1c2d3e4-0010-0010-0010-000000000010',
   'a1b2c3d4-0005-0005-0005-000000000005',
   'R002','H007','H008','A003',
   '2024-06-02 08:30:00+07','2024-06-02 09:10:00+07',14000,1000,13000,'ShopeePay','Sukses');

-- ============================================================
-- SEED DATA - FEEDBACK
-- ============================================================
INSERT INTO feedback
  (feedback_id, transaksi_id, pengguna_id, rating, komentar, kategori, created_at)
VALUES
  -- === KATEGORI: UMUM (TRX 1-3) ===
  ('f1a2b3c4-0001-0001-0001-000000000001',
   'b1c2d3e4-0001-0001-0001-000000000001',
   'a1b2c3d4-0001-0001-0001-000000000001',
   5,
   'Layanan sangat memuaskan, sopir ramah dan tepat waktu!',
   'Umum',
   '2024-06-01 08:00:00+07'),

  ('f1a2b3c4-0002-0002-0002-000000000002',
   'b1c2d3e4-0002-0002-0002-000000000002',
   'a1b2c3d4-0002-0002-0002-000000000002',
   4,
   'Pada umumnya bagus, tapi agak lama di halte terakhir',
   'Umum',
   '2024-06-01 08:45:00+07'),

  ('f1a2b3c4-0003-0003-0003-000000000003',
   'b1c2d3e4-0003-0003-0003-000000000003',
   'a1b2c3d4-0003-0003-0003-000000000003',
   3,
   'Layanan biasa saja, tidak ada yang istimewa',
   'Umum',
   '2024-06-01 08:15:00+07'),

  -- === KATEGORI: FASILITAS BUSWAY (TRX 4-5) ===
  ('f1a2b3c4-0004-0004-0004-000000000004',
   'b1c2d3e4-0004-0004-0004-000000000004',
   'a1b2c3d4-0004-0004-0004-000000000004',
   4,
   'Kursi tunggu nyaman dan bersih, AC berfungsi baik',
   'Fasilitas Busway',
   '2024-06-01 09:50:00+07'),

  ('f1a2b3c4-0005-0005-0005-000000000005',
   'b1c2d3e4-0005-0005-0005-000000000005',
   'a1b2c3d4-0005-0005-0005-000000000005',
   2,
   'Toilet di halte kotor dan tidak terawat dengan baik',
   'Fasilitas Busway',
   '2024-06-01 07:45:00+07'),

  -- === KATEGORI: APLIKASI JAKLINGKO (TRX 6-10) ===
  ('f1a2b3c4-0006-0006-0006-000000000006',
   'b1c2d3e4-0006-0006-0006-000000000006',
   'a1b2c3d4-0001-0001-0001-000000000001',
   5,
   'Aplikasi sangat user-friendly, booking mudah dan cepat!',
   'Aplikasi Jaklingko',
   '2024-06-02 08:30:00+07'),

  ('f1a2b3c4-0007-0007-0007-000000000007',
   'b1c2d3e4-0007-0007-0007-000000000007',
   'a1b2c3d4-0002-0002-0002-000000000002',
   3,
   'Aplikasi kadang crash ketika lagi rush hour',
   'Aplikasi Jaklingko',
   '2024-06-02 08:15:00+07'),

  ('f1a2b3c4-0008-0008-0008-000000000008',
   'b1c2d3e4-0008-0008-0008-000000000008',
   'a1b2c3d4-0003-0003-0003-000000000003',
   4,
   'Fitur notifikasi real-time sangat membantu, tapi boros kuota',
   'Aplikasi Jaklingko',
   '2024-06-02 10:15:00+07'),

  ('f1a2b3c4-0009-0009-0009-000000000009',
   'b1c2d3e4-0009-0009-0009-000000000009',
   'a1b2c3d4-0004-0004-0004-000000000004',
   1,
   'Aplikasi error, tidak bisa bayar melalui GoPay',
   'Aplikasi Jaklingko',
   '2024-06-02 07:50:00+07'),

  ('f1a2b3c4-0010-0010-0010-000000000010',
   'b1c2d3e4-0010-0010-0010-000000000010',
   'a1b2c3d4-0005-0005-0005-000000000005',
   5,
   'Update terbaru tambah fitur lokasi real-time, sangat bagus!',
   'Aplikasi Jaklingko',
   '2024-06-02 09:20:00+07');
