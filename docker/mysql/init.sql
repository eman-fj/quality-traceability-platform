-- QTP ERP simulation (MySQL)
-- Holds supplier + material-batch traceability data the defect path joins against.
-- Batch IDs below MUST match the set the inspection_simulator emits.

CREATE DATABASE IF NOT EXISTS erp CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE erp;

-- This file is UTF-8; tell the client so accented supplier names (Précision, OÜ)
-- aren't double-encoded into mojibake on load.
SET NAMES utf8mb4;

CREATE TABLE suppliers (
    supplier_id          VARCHAR(16) PRIMARY KEY,
    name                 VARCHAR(128) NOT NULL,
    country              VARCHAR(64),
    accreditation_status VARCHAR(32)        -- ACCREDITED | PROVISIONAL | SUSPENDED
);

CREATE TABLE material_batches (
    batch_id        VARCHAR(16) PRIMARY KEY,
    supplier_id     VARCHAR(16) NOT NULL,
    material_grade  VARCHAR(32),
    certificate_url VARCHAR(255),
    CONSTRAINT fk_batch_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers (supplier_id)
);

INSERT INTO suppliers (supplier_id, name, country, accreditation_status) VALUES
    ('SUP-01', 'NordStahl Components GmbH', 'Germany',  'ACCREDITED'),
    ('SUP-02', 'Lyon Précision Métaux',     'France',   'ACCREDITED'),
    ('SUP-03', 'Iberia Forge S.A.',         'Spain',    'PROVISIONAL'),
    ('SUP-04', 'Baltic Alloy OÜ',           'Estonia',  'ACCREDITED'),
    ('SUP-05', 'Adriatic Metalworks d.o.o', 'Croatia',  'SUSPENDED');

INSERT INTO material_batches (batch_id, supplier_id, material_grade, certificate_url) VALUES
    ('BATCH-1001', 'SUP-01', 'S355J2',   'https://certs.example/erp/BATCH-1001.pdf'),
    ('BATCH-1002', 'SUP-01', 'S355J2',   'https://certs.example/erp/BATCH-1002.pdf'),
    ('BATCH-1003', 'SUP-02', '42CrMo4',  'https://certs.example/erp/BATCH-1003.pdf'),
    ('BATCH-1004', 'SUP-02', '42CrMo4',  'https://certs.example/erp/BATCH-1004.pdf'),
    ('BATCH-1005', 'SUP-03', 'C45E',     'https://certs.example/erp/BATCH-1005.pdf'),
    ('BATCH-1006', 'SUP-03', 'C45E',     'https://certs.example/erp/BATCH-1006.pdf'),
    ('BATCH-1007', 'SUP-04', 'AlSi10Mg', 'https://certs.example/erp/BATCH-1007.pdf'),
    ('BATCH-1008', 'SUP-04', 'AlSi10Mg', 'https://certs.example/erp/BATCH-1008.pdf'),
    ('BATCH-1009', 'SUP-05', 'EN-GJL250','https://certs.example/erp/BATCH-1009.pdf'),
    ('BATCH-1010', 'SUP-05', 'EN-GJL250','https://certs.example/erp/BATCH-1010.pdf');
