-- =====================================================================
-- Stripe : système transactionnel (OLTP)
-- Livrable 1 : script DDL PostgreSQL (Aurora PostgreSQL Multi-AZ)
-- Version cible : PostgreSQL 14 ou plus
-- ---------------------------------------------------------------------
-- Schémas
-- ---------------------------------------------------------------------
CREATE SCHEMA reference;
CREATE SCHEMA actor;
CREATE SCHEMA payment;
CREATE SCHEMA [audit];

-- ---------------------------------------------------------------------
-- Enums
-- ---------------------------------------------------------------------
CREATE TYPE payment.transaction_type AS ENUM (
  'payment', 
  'refund'
);

CREATE TYPE payment.transaction_status AS ENUM (
  'approved', 
  'rejected_by_fraud', 
  'rejected_by_bank', 
  'failed'
);

CREATE TYPE payment.device_type AS ENUM (
  'mobile', 
  'desktop', 
  'tablet'
);

CREATE TYPE payment.payment_method_type AS ENUM (
  'card', 
  'sepa', 
  'bank_transfer'
);

CREATE TYPE payment.fraud_decision AS ENUM (
  'accepted', 
  'rejected'
);

CREATE TYPE payment.outbox_aggregate_type AS ENUM (
  'payment', 
  'refund', 
  'ip_blocklist'
);

CREATE TYPE payment.outbox_event_type AS ENUM (
  'payment.created',
  'refund.created',
  'ip_blocklist.added', 
  'ip_blocklist.removed'
);

CREATE TYPE actor.ip_blocklist_source AS ENUM (
  'manual', 
  'external', 
  'model'
);

CREATE TYPE actor.data_subject_request_type AS ENUM (
  'access', 
  'erasure'
);

CREATE TYPE actor.data_subject_request_status AS ENUM (
  'received', 
  'in_progress', 
  'completed', 
  'rejected'
);

CREATE TYPE audit.audit_action AS ENUM (
  'insert', 
  'update', 
  'delete'
);

-- ---------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------

-- ===== Schéma reference (1/2) =====

CREATE TABLE reference.currency (
  currency_id  integer                GENERATED ALWAYS AS IDENTITY,
  name         varchar(50)            NOT NULL,
  code_iso     char(3)                NOT NULL,
  symbol       varchar(5),
  change_rate  numeric(18,7)          NOT NULL,
  change_date  timestamptz            NOT NULL,
  created_at   timestamptz            NOT NULL DEFAULT now(),
  modified_at  timestamptz,
  -- clé primaire
  CONSTRAINT pk_currency              PRIMARY KEY (currency_id),
  -- Unicité
  CONSTRAINT uq_currency_code_iso     UNIQUE (code_iso),
  -- Check
  CONSTRAINT ck_currency_change_rate  CHECK (change_rate > 0)
);

CREATE TABLE reference.country (
  country_id   integer                GENERATED ALWAYS AS IDENTITY,
  name         varchar(100)           NOT NULL,
  code_iso     char(2)                NOT NULL,
  created_at   timestamptz            NOT NULL DEFAULT now(),
  modified_at  timestamptz,
  -- clé primaire
  CONSTRAINT pk_country               PRIMARY KEY (country_id),
  -- Unicité
  CONSTRAINT uq_country_code_iso      UNIQUE (code_iso)
);

CREATE TABLE reference.analytic_category (
  analytic_category_id  integer         GENERATED ALWAYS AS IDENTITY,
  code                  varchar(10)     NOT NULL,
  name                  varchar(100)    NOT NULL,
  created_at            timestamptz     NOT NULL DEFAULT now(),
  modified_at           timestamptz,
  -- clé primaire
  CONSTRAINT pk_analytic_category       PRIMARY KEY (analytic_category_id),
  -- Unicité
  CONSTRAINT uq_analytic_category_code  UNIQUE (code),
  -- Check
  CONSTRAINT ck_analytic_category_code  CHECK (code <> '')
);

-- ===== Schéma actor =====

CREATE TABLE actor.merchant (
  merchant_id  integer                GENERATED ALWAYS AS IDENTITY,
  name         varchar(250)           NOT NULL,
  email        varchar(250)           NOT NULL,
  country_id   integer                NOT NULL,
  created_at   timestamptz            NOT NULL DEFAULT now(),
  modified_at  timestamptz,
  -- clé primaire
  CONSTRAINT pk_merchant              PRIMARY KEY (merchant_id),
  -- Unicité
  CONSTRAINT uq_merchant_email        UNIQUE (email),
  -- Clé étrangère
  CONSTRAINT fk_merchant_country      FOREIGN KEY (country_id)    REFERENCES reference.country      (country_id)
);

CREATE TABLE actor.customer (
  customer_id  integer                GENERATED ALWAYS AS IDENTITY,
  merchant_id  integer                NOT NULL,
  email        varchar(250)           NOT NULL,
  full_name    varchar(250),
  country_id   integer                NOT NULL,
  created_at   timestamptz            NOT NULL DEFAULT now(),
  modified_at  timestamptz,
  -- clé primaire
  CONSTRAINT pk_customer              PRIMARY KEY (customer_id),
  -- unicité
  CONSTRAINT uq_customer_email        UNIQUE (merchant_id, email),
  -- clés étrangères
  CONSTRAINT fk_customer_merchant     FOREIGN KEY (merchant_id)   REFERENCES actor.merchant (merchant_id),
  CONSTRAINT fk_customer_country      FOREIGN KEY (country_id)    REFERENCES reference.country (country_id)
);

CREATE TABLE actor.data_subject_request (
  data_subject_request_id  integer                            GENERATED ALWAYS AS IDENTITY,
  customer_id              integer                            NOT NULL,
  request_type             actor.data_subject_request_type    NOT NULL,
  status                   actor.data_subject_request_status  NOT NULL DEFAULT 'received',
  requested_at             timestamptz                        NOT NULL DEFAULT now(),
  due_date                 date                               NOT NULL DEFAULT ((now() + interval '1 month')::date),
  completed_at             timestamptz,
  handled_by               varchar(100),
  created_at               timestamptz                        NOT NULL DEFAULT now(),
  modified_at              timestamptz,
  -- clé primaire
  CONSTRAINT pk_data_subject_request                          PRIMARY KEY (data_subject_request_id),
  -- Check
  CONSTRAINT ck_dsr_due_date         CHECK (due_date >= requested_at::date),
  CONSTRAINT ck_dsr_completed_at     CHECK ((status IN ('completed', 'rejected')) = (completed_at IS NOT NULL)),
  -- clé étrangère
  CONSTRAINT fk_dsr_customer         FOREIGN KEY (customer_id)    REFERENCES actor.customer (customer_id)
);

CREATE TABLE actor.ip_blocklist (
  ip_blocklist_id       integer                   GENERATED ALWAYS AS IDENTITY,
  merchant_id           integer,
  ip_range              cidr                      NOT NULL,
  reason                varchar(250),
  source                actor.ip_blocklist_source NOT NULL,
  expires_at            timestamptz,
  created_at            timestamptz               NOT NULL DEFAULT now(),
  modified_at           timestamptz,
  -- clé primaire
  CONSTRAINT pk_ip_blocklist                      PRIMARY KEY (ip_blocklist_id),
  -- check
  CONSTRAINT ck_ip_blocklist_expires  CHECK (expires_at IS NULL OR expires_at > created_at),
  -- clé étrangère
  CONSTRAINT fk_ip_blocklist_merchant FOREIGN KEY (merchant_id)   REFERENCES actor.merchant (merchant_id)
);

-- ===== Schéma reference (2/2) / apres actor pour les FK =====

CREATE TABLE reference.product (
  product_id            integer             GENERATED ALWAYS AS IDENTITY,
  merchant_id           integer             NOT NULL,
  name                  varchar(250)        NOT NULL,
  description           varchar(500),
  unit_price            numeric(18,3)       NOT NULL,
  currency_id           integer             NOT NULL,
  analytic_category_id  integer             NOT NULL,
  end_of_life           date,
  created_at            timestamptz         NOT NULL DEFAULT now(),
  modified_at           timestamptz,
  -- clé primaire
  CONSTRAINT pk_product                    PRIMARY KEY (product_id),
  -- check
  CONSTRAINT ck_product_unit_price         CHECK (unit_price >= 0),
  -- clés étrangères
  CONSTRAINT fk_product_merchant           FOREIGN KEY (merchant_id)            REFERENCES actor.merchant (merchant_id),
  CONSTRAINT fk_product_currency           FOREIGN KEY (currency_id)            REFERENCES reference.currency (currency_id),
  CONSTRAINT fk_product_analytic_category  FOREIGN KEY (analytic_category_id)   REFERENCES reference.analytic_category (analytic_category_id)
);

-- ===== Schéma payment =====

CREATE TABLE payment.payment_method (
  payment_method_id  integer                     GENERATED ALWAYS AS IDENTITY,
  customer_id        integer                     NOT NULL,
  type               payment.payment_method_type NOT NULL,
  brand              varchar(30),
  last4              char(4),
  exp_month          smallint,
  exp_year           smallint,
  token              varchar(100)                NOT NULL,
  fingerprint        varchar(64),
  created_at         timestamptz                 NOT NULL DEFAULT now(),
  modified_at        timestamptz,
  -- clé primaire
  CONSTRAINT pk_payment_method                  PRIMARY KEY (payment_method_id),
  -- unicité
  CONSTRAINT uq_payment_method_token    UNIQUE (token),
  --ccheck
  CONSTRAINT ck_payment_method_exp_month    CHECK (exp_month IS NULL OR exp_month BETWEEN 1 AND 12),
  CONSTRAINT ck_payment_method_exp_year     CHECK (exp_year  IS NULL OR exp_year  BETWEEN 2000 AND 2100),
  -- clés étrangères
  CONSTRAINT fk_payment_method_customer FOREIGN KEY (customer_id)    REFERENCES actor.customer (customer_id)
);


CREATE SEQUENCE payment.transaction_id_seq AS bigint;
--partitionnée par mois. Insertion seule.
CREATE TABLE payment.transaction (
  transaction_id                 bigint                      NOT NULL DEFAULT nextval('payment.transaction_id_seq'), 
  created_at                     timestamptz                 NOT NULL DEFAULT now(),
  transaction_type               payment.transaction_type    NOT NULL,
  source_transaction_id          bigint,
  source_transaction_created_at  timestamptz,
  merchant_id                    integer                     NOT NULL,
  customer_id                    integer                     NOT NULL,
  payment_method_id              integer                     NOT NULL,
  amount                         numeric(18,3)               NOT NULL,
  currency_id                    integer                     NOT NULL,
  change_rate                    numeric(18,7)               NOT NULL,
  country_id                     integer                     NOT NULL,
  status                         payment.transaction_status  NOT NULL,
  bank_decline_code              varchar(50),
  ip_address                     inet,
  ip_country_id                  integer,
  device_type                    payment.device_type,
  -- clé primaire
  CONSTRAINT pk_transaction PRIMARY KEY (transaction_id, created_at),
  -- check
  CONSTRAINT ck_transaction_amount      CHECK (amount > 0),
  CONSTRAINT ck_transaction_change_rate CHECK (change_rate > 0),
  -- clés étrangères
  CONSTRAINT fk_transaction_merchant       FOREIGN KEY (merchant_id)        REFERENCES actor.merchant (merchant_id),
  CONSTRAINT fk_transaction_customer       FOREIGN KEY (customer_id)        REFERENCES actor.customer (customer_id),
  CONSTRAINT fk_transaction_payment_method FOREIGN KEY (payment_method_id)  REFERENCES payment.payment_method (payment_method_id),
  CONSTRAINT fk_transaction_currency       FOREIGN KEY (currency_id)        REFERENCES reference.currency (currency_id),
  CONSTRAINT fk_transaction_country        FOREIGN KEY (country_id)         REFERENCES reference.country (country_id),
  CONSTRAINT fk_transaction_ip_country     FOREIGN KEY (ip_country_id)      REFERENCES reference.country (country_id),
) PARTITION BY RANGE (created_at);

ALTER SEQUENCE payment.transaction_id_seq OWNED BY payment.transaction.transaction_id;

-- Lien d'un remboursement vers son paiement d'origine
ALTER TABLE payment.transaction
  ADD CONSTRAINT fk_transaction_source  FOREIGN KEY (source_transaction_id, source_transaction_created_at) REFERENCES payment.transaction (transaction_id, created_at);


CREATE TABLE payment.idempotency_key (
  merchant_id             integer      NOT NULL,
  idempotency_key         varchar(100) NOT NULL,
  transaction_id          bigint       NOT NULL,
  transaction_created_at  timestamptz  NOT NULL,
  created_at              timestamptz  NOT NULL DEFAULT now(),
  -- clé primaire
  CONSTRAINT pk_idempotency_key PRIMARY KEY (merchant_id, idempotency_key),
  -- checks
  CONSTRAINT ck_idempotency_key CHECK (idempotency_key <> ''),
  -- clés étrangères
  CONSTRAINT fk_idempotency_merchant    FOREIGN KEY (merchant_id)                             REFERENCES actor.merchant (merchant_id),
  CONSTRAINT fk_idempotency_transaction FOREIGN KEY (transaction_id, transaction_created_at)  REFERENCES payment.transaction (transaction_id, created_at)
);

CREATE TABLE payment.payment_item (
  payment_item_id         bigint        GENERATED ALWAYS AS IDENTITY,
  transaction_id          bigint        NOT NULL,
  transaction_created_at  timestamptz   NOT NULL,
  product_id              integer       NOT NULL,
  product_name            varchar(250)  NOT NULL,
  analytic_category_id    integer       NOT NULL,
  quantity                integer       NOT NULL,
  unit_price              numeric(18,3) NOT NULL,
  created_at              timestamptz   NOT NULL DEFAULT now(),
  -- clé primaire
  CONSTRAINT pk_payment_item            PRIMARY KEY (payment_item_id),
  -- unicité
  CONSTRAINT uq_payment_item_product    UNIQUE (transaction_id, product_id),
  -- check
  CONSTRAINT ck_payment_item_quantity   CHECK (quantity > 0),
  CONSTRAINT ck_payment_item_unit_price CHECK (unit_price >= 0),
  -- clés étrangères
  CONSTRAINT fk_payment_item_transaction        FOREIGN KEY (transaction_id, transaction_created_at) REFERENCES payment.transaction (transaction_id, created_at),
  CONSTRAINT fk_payment_item_product            FOREIGN KEY (product_id)                             REFERENCES reference.product (product_id),
  CONSTRAINT fk_payment_item_analytic_category FOREIGN KEY (analytic_category_id)                    REFERENCES reference.analytic_category (analytic_category_id)
);

CREATE TABLE payment.fraud_assessment (
  fraud_assessment_id     uuid                   NOT NULL DEFAULT gen_random_uuid(),
  transaction_id          bigint                 NOT NULL,
  transaction_created_at  timestamptz            NOT NULL,
  score                   numeric(5,4)           NOT NULL,
  decision                payment.fraud_decision NOT NULL,
  reasons                 jsonb,
  model_version           varchar(50)            NOT NULL,
  created_at              timestamptz            NOT NULL DEFAULT now(),
  -- clé primaire
  CONSTRAINT pk_fraud_assessment                PRIMARY KEY (fraud_assessment_id),
  -- unicité
  CONSTRAINT uq_fraud_assessment_transaction    UNIQUE (transaction_id, transaction_created_at),
  -- check
  CONSTRAINT ck_fraud_assessment_score  CHECK (score BETWEEN 0 AND 1),
  CONSTRAINT ck_fraud_assessment_model  CHECK (model_version <> ''),
  -- clé étrangère
  CONSTRAINT fk_fraud_assessment_transaction FOREIGN KEY (transaction_id, transaction_created_at) REFERENCES payment.transaction (transaction_id, created_at)
);

-- Pas de clé étrangère vers transaction car aggregate_id désigne plusieurs types d'objets / denormalisation pour perf
CREATE TABLE payment.outbox (
  event_id        uuid                          NOT NULL DEFAULT gen_random_uuid(),
  aggregate_type  payment.outbox_aggregate_type NOT NULL,
  aggregate_id    bigint                        NOT NULL,
  event_type      payment.outbox_event_type     NOT NULL,
  payload         jsonb                         NOT NULL,
  schema_version  smallint                      NOT NULL DEFAULT 1,
  created_at      timestamptz                   NOT NULL DEFAULT now(),
  -- clé primaire
  CONSTRAINT pk_outbox PRIMARY KEY (event_id),
  -- check
  CONSTRAINT ck_outbox_payload_object  CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT ck_outbox_schema_version  CHECK (schema_version > 0),
);

-- ===== Schéma audit =====

CREATE TABLE audit.audit_log (
  audit_id         bigint             GENERATED ALWAYS AS IDENTITY,
  table_name       varchar(100)       NOT NULL,
  record_id        varchar(64)        NOT NULL,
  action           audit.audit_action NOT NULL,
  changed_columns  jsonb,
  changed_by       varchar(100)       NOT NULL,
  created_at       timestamptz        NOT NULL DEFAULT now(),
  -- clé primaire
  CONSTRAINT pk_audit_log           PRIMARY KEY (audit_id),
);

-- ---------------------------------------------------------------------
-- Index / proposition en fonction des recherches les plus utiles sauf sur la table des transactions pour ne pas dégrader les perfs
-- ---------------------------------------------------------------------
CREATE INDEX ix_data_subject_request_customer     ON actor.data_subject_request (customer_id, requested_at);

CREATE INDEX ix_ip_blocklist_merchant             ON actor.ip_blocklist (merchant_id);

CREATE INDEX ix_ip_blocklist_range                ON actor.ip_blocklist USING gist (ip_range inet_ops);

CREATE INDEX ix_product_merchant_name             ON reference.product (merchant_id, name);

CREATE INDEX ix_payment_method_customer           ON payment.payment_method (customer_id);
CREATE INDEX ix_payment_method_fingerprint        ON payment.payment_method (fingerprint)
  WHERE fingerprint IS NOT NULL;

CREATE INDEX ix_transaction_merchant_created      ON payment.transaction (merchant_id, created_at);
CREATE INDEX ix_transaction_customer_created      ON payment.transaction (customer_id, created_at);
CREATE INDEX ix_transaction_source                ON payment.transaction
  (source_transaction_id, source_transaction_created_at)
  WHERE source_transaction_id IS NOT NULL;

CREATE INDEX ix_idempotency_key_created         ON payment.idempotency_key (created_at);
CREATE INDEX ix_idempotency_key_transaction     ON payment.idempotency_key (transaction_id, transaction_created_at);

CREATE INDEX ix_payment_item_product            ON payment.payment_item (product_id);

CREATE INDEX ix_outbox_created                ON payment.outbox (created_at);

CREATE INDEX ix_audit_log_record              ON audit.audit_log (table_name, record_id);
CREATE INDEX ix_audit_log_created             ON audit.audit_log (created_at);



-- ---------------------------------------------------------------------
-- Partitions de payment.transaction (une par mois)
-- Aucune partition par défaut : une écriture hors des mois créés échoue
-- > Création de la partition du mois suivant avant la fin du mois par tâche planifiée.
-- ---------------------------------------------------------------------
CREATE FUNCTION payment.create_transaction_partition(p_month date) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  v_start date := date_trunc('month', p_month)::date;
  v_end   date := (date_trunc('month', p_month) + interval '1 month')::date;
  v_name  text := format('transaction_%s', to_char(v_start, 'YYYY_MM'));
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS payment.%I PARTITION OF payment.transaction FOR VALUES FROM (%L) TO (%L)',
    v_name,
    v_start::text || ' 00:00:00+00',
    v_end::text   || ' 00:00:00+00'
  );
  RETURN v_name;
END;
$$;

SELECT payment.create_transaction_partition(m::date)
FROM generate_series(date '2026-10-01', date '2027-03-01', interval '1 month') AS m;
