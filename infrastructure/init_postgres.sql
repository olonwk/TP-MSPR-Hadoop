-- =============================================================================
-- init_postgres.sql
-- Initialisation de la base bankdb pour FinData Solutions
-- Compatible avec le TP NiFi (postgres:16, user=admin, db=bankdb)
--
-- Ce script est exécuté automatiquement au premier démarrage du conteneur
-- postgres via le volume /docker-entrypoint-initdb.d/
-- =============================================================================

-- ── Table principale : transactions bancaires ────────────────────────────────

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  VARCHAR(36)     PRIMARY KEY,
    client_id       VARCHAR(20)     NOT NULL,
    iban            VARCHAR(34)     NOT NULL,
    montant         NUMERIC(15, 2)  NOT NULL,
    devise          CHAR(3)         NOT NULL DEFAULT 'EUR',
    date_heure      TIMESTAMP       NOT NULL,
    code_banque     VARCHAR(10)     NOT NULL,
    statut          VARCHAR(10)     NOT NULL CHECK (statut IN ('OK','PENDING','FRAUD'))
);

-- Index pour accélérer les requêtes de NiFi (filtre par date)
CREATE INDEX IF NOT EXISTS idx_transactions_date
    ON transactions (DATE(date_heure));

CREATE INDEX IF NOT EXISTS idx_transactions_code_banque
    ON transactions (code_banque);

-- ── Table de référence : cours boursiers ────────────────────────────────────

CREATE TABLE IF NOT EXISTS cours_boursiers (
    id              SERIAL          PRIMARY KEY,
    devise          CHAR(3)         NOT NULL,
    taux_eur        NUMERIC(12, 6)  NOT NULL,
    horodatage      TIMESTAMP       NOT NULL,
    source          VARCHAR(20)     NOT NULL CHECK (source IN ('ECB','Bloomberg','Reuters'))
);

CREATE INDEX IF NOT EXISTS idx_cours_date
    ON cours_boursiers (DATE(horodatage));

-- ── Données de démonstration : transactions ─────────────────────────────────

INSERT INTO transactions (transaction_id, client_id, iban, montant, devise, date_heure, code_banque, statut) VALUES
  ('TXN-0001', 'CLI-001', 'FR7630006000011234567890189', 1200.50, 'EUR', NOW() - INTERVAL '1 day', 'BNP', 'OK'),
  ('TXN-0002', 'CLI-002', 'FR7614508010000012345678907', 85.00,   'EUR', NOW() - INTERVAL '1 day', 'SG',  'OK'),
  ('TXN-0003', 'CLI-003', 'FR7611315000010000456789012', 3500.00, 'USD', NOW() - INTERVAL '1 day', 'CA',  'OK'),
  ('TXN-0004', 'CLI-004', 'FR7617569000400444975680000', 250.75,  'GBP', NOW() - INTERVAL '1 day', 'BNP', 'PENDING'),
  ('TXN-0005', 'CLI-005', 'FR7614508010000098765432109', 9999.99, 'EUR', NOW() - INTERVAL '1 day', 'SG',  'FRAUD'),
  ('TXN-0006', 'CLI-001', 'FR7630006000011234567890189', 450.00,  'EUR', NOW() - INTERVAL '1 day', 'BNP', 'OK'),
  ('TXN-0007', 'CLI-006', 'FR7611315000010000112233445', 1800.00, 'JPY', NOW() - INTERVAL '1 day', 'LCL', 'OK'),
  ('TXN-0008', 'CLI-007', 'FR7630004000031234567890143', 320.00,  'EUR', NOW() - INTERVAL '1 day', 'BP',  'OK'),
  ('TXN-0009', 'CLI-002', 'FR7614508010000012345678907', 75.50,   'CHF', NOW() - INTERVAL '1 day', 'SG',  'OK'),
  ('TXN-0010', 'CLI-008', 'FR7614508010000055566677788', 15000.00,'EUR', NOW() - INTERVAL '1 day', 'CA',  'FRAUD');

-- ── Données de démonstration : cours boursiers ──────────────────────────────

INSERT INTO cours_boursiers (devise, taux_eur, horodatage, source) VALUES
  ('USD', 1.085000, NOW() - INTERVAL '1 day', 'ECB'),
  ('GBP', 0.856000, NOW() - INTERVAL '1 day', 'ECB'),
  ('JPY', 162.450000, NOW() - INTERVAL '1 day', 'Bloomberg'),
  ('CHF', 0.946000, NOW() - INTERVAL '1 day', 'Reuters'),
  ('CAD', 1.462000, NOW() - INTERVAL '1 day', 'ECB'),
  ('AUD', 1.654000, NOW() - INTERVAL '1 day', 'Bloomberg'),
  ('USD', 1.083500, NOW() - INTERVAL '2 days', 'ECB'),
  ('GBP', 0.854200, NOW() - INTERVAL '2 days', 'ECB');

-- ── Vue utile pour NiFi (filtre automatique par date) ───────────────────────

CREATE OR REPLACE VIEW v_transactions_today AS
    SELECT *
    FROM transactions
    WHERE DATE(date_heure) = CURRENT_DATE;

-- ── Résumé ───────────────────────────────────────────────────────────────────

DO $$
BEGIN
    RAISE NOTICE 'Base bankdb initialisée : % transactions, % cours boursiers',
        (SELECT COUNT(*) FROM transactions),
        (SELECT COUNT(*) FROM cours_boursiers);
END $$;
