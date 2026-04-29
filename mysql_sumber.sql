-- ============================================================
--  JAKLINGKO DATA PIPELINE - UTS
--  FILE   : mysql_sumber.sql
--  DATABASE: jaklingko_mysql  (Sumber 1 - MySQL Workbench)
--  DESC   : DDL database operasional MySQL
--           Menyimpan data master: Rute, Halte, Armada
-- ============================================================

CREATE DATABASE IF NOT EXISTS jaklingko_mysql
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE jaklingko_mysql;

-- ------------------------------------------------------------
-- Tabel 1: rute
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rute (
    rute_id       VARCHAR(10)   NOT NULL,
    nama_rute     VARCHAR(100)  NOT NULL,
    jenis_moda    ENUM('BRT','MRT','LRT','Mikrotrans','Bus') NOT NULL DEFAULT 'Bus',
    asal          VARCHAR(100)  NOT NULL,
    tujuan        VARCHAR(100)  NOT NULL,
    jarak_km      DECIMAL(6,2)  NOT NULL,
    tarif_dasar   INT           NOT NULL COMMENT 'Rupiah',
    status        ENUM('Aktif','Nonaktif') NOT NULL DEFAULT 'Aktif',
    created_at    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (rute_id),
    INDEX idx_jenis_moda (jenis_moda),
    INDEX idx_status     (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------
-- Tabel 2: halte
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS halte (
    halte_id       VARCHAR(10)   NOT NULL,
    nama_halte     VARCHAR(150)  NOT NULL,
    rute_id        VARCHAR(10)   NOT NULL,
    urutan_halte   TINYINT       NOT NULL,
    alamat         VARCHAR(255),
    latitude       DECIMAL(10,7) NOT NULL,
    longitude      DECIMAL(10,7) NOT NULL,
    fasilitas      VARCHAR(200)  NOT NULL DEFAULT '',
    status         ENUM('Aktif','Nonaktif') NOT NULL DEFAULT 'Aktif',
    created_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (halte_id),
    FOREIGN KEY (rute_id) REFERENCES rute(rute_id)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    INDEX idx_rute_id   (rute_id),
    INDEX idx_koordinat (latitude, longitude)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ------------------------------------------------------------
-- Tabel 3: armada
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS armada (
    armada_id    VARCHAR(10)  NOT NULL,
    nomor_polisi VARCHAR(15)  NOT NULL UNIQUE,
    rute_id      VARCHAR(10)  NOT NULL,
    jenis_armada VARCHAR(50)  NOT NULL,
    kapasitas    SMALLINT     NOT NULL,
    tahun_buat   YEAR         NOT NULL,
    kondisi      ENUM('Baik','Sedang','Rusak') NOT NULL DEFAULT 'Baik',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (armada_id),
    FOREIGN KEY (rute_id) REFERENCES rute(rute_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- SEED DATA
-- ============================================================
INSERT INTO rute VALUES
  ('R001','Blok M - Kota','BRT','Blok M','Kota',14.50,3500,'Aktif',NOW(),NOW()),
  ('R002','Lebak Bulus - Bundaran HI','MRT','Lebak Bulus','Bundaran HI',15.70,14000,'Aktif',NOW(),NOW()),
  ('R003','Cibubur - Dukuh Atas','LRT','Cibubur','Dukuh Atas',24.30,5000,'Aktif',NOW(),NOW()),
  ('R004','Tanah Abang - Pulo Gadung','Bus','Tanah Abang','Pulo Gadung',18.20,3500,'Aktif',NOW(),NOW()),
  ('R005','Kampung Rambutan - Blok M','Mikrotrans','Kampung Rambutan','Blok M',12.10,5000,'Aktif',NOW(),NOW());

INSERT INTO halte VALUES
  ('H001','Halte Blok M','R001',1,'Jl. Melawai Raya',-6.2437622,106.7993430,'Toilet,Kursi Tunggu,WiFi','Aktif',NOW()),
  ('H002','Halte Masjid Agung','R001',2,'Jl. Sisingamangaraja',-6.2380000,106.8010000,'Kursi Tunggu','Aktif',NOW()),
  ('H003','Halte Polda Metro Jaya','R001',3,'Jl. Jend. Sudirman',-6.2300000,106.8050000,'Kursi Tunggu,WiFi','Aktif',NOW()),
  ('H004','Halte Karet','R001',4,'Jl. Jend. Sudirman Karet',-6.2100000,106.8200000,'Kursi Tunggu','Aktif',NOW()),
  ('H005','Halte Dukuh Atas','R001',5,'Jl. Jend. Sudirman',-6.2008000,106.8230000,'Toilet,Kursi Tunggu,WiFi','Aktif',NOW()),
  ('H006','Halte Kota','R001',6,'Jl. Lada, Kota Tua',-6.1375000,106.8132000,'Toilet,Kursi Tunggu,WiFi,Lift','Aktif',NOW()),
  ('H007','Stasiun Lebak Bulus','R002',1,'Jl. Raya Ciputat',-6.2894000,106.7746000,'Toilet,Kursi Tunggu,WiFi,Parkir,Lift','Aktif',NOW()),
  ('H008','Stasiun Bundaran HI','R002',13,'Jl. MH Thamrin',-6.1935000,106.8227000,'Toilet,Kursi Tunggu,WiFi,Lift','Aktif',NOW()),
  ('H009','Stasiun Cibubur','R003',1,'Jl. Raya Cibubur',-6.3615000,106.8772000,'Toilet,Kursi Tunggu,WiFi,Parkir','Aktif',NOW()),
  ('H010','Halte Tanah Abang','R004',1,'Jl. Jati Baru',-6.1864000,106.8117000,'Kursi Tunggu','Aktif',NOW());

INSERT INTO armada VALUES
  ('A001','B 1234 TRA','R001','Transjakarta Feeder',85,2020,'Baik',NOW()),
  ('A002','B 5678 TRB','R001','Transjakarta Reguler',100,2019,'Baik',NOW()),
  ('A003','B 9999 MRT','R002','MRT Jakarta KCI',300,2021,'Baik',NOW()),
  ('A004','B 4321 LRT','R003','LRT Jakarta',270,2021,'Baik',NOW()),
  ('A005','B 1111 BUS','R004','Bus Kota AC',60,2018,'Sedang',NOW());
