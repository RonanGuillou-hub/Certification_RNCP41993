-- =====================================================================
-- Stripe : système analytique (OLAP), couche gold, schéma en étoile
-- Livrable 3 : script DDL Databricks / Delta Lake (Unity Catalog)
-- Cible : Databricks Free Edition (calcul serverless)
-- =====================================================================

USE CATALOG workspace;   -- à adapter si votre catalogue porte un autre nom

CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------------------
-- Dimensions
-- en Delta, garantie par le MERGE du chargement.
-- ---------------------------------------------------------------------

CREATE TABLE gold.dim_date (
  date_key      INT          NOT NULL,   -- format yyyymmdd
  full_date     DATE         NOT NULL,
  year          SMALLINT     NOT NULL,
  month         SMALLINT     NOT NULL,
  month_name    VARCHAR(10)  NOT NULL,
  day_of_month  SMALLINT     NOT NULL,
  day_of_year   SMALLINT     NOT NULL,
  quarter       SMALLINT     NOT NULL,
  year_quarter  CHAR(7)      NOT NULL,   -- ex. 2026-Q4
  year_month    CHAR(7)      NOT NULL,   -- ex. 2026-10
  iso_year      SMALLINT     NOT NULL,   -- année de la semaine ISO 8601
  week_number   SMALLINT     NOT NULL,   -- semaine ISO 8601 (semaine 1 = au moins 4 jours)
  day_of_week   SMALLINT     NOT NULL,   -- 1 = lundi ... 7 = dimanche
  day_name      VARCHAR(10)  NOT NULL,
  is_weekend    BOOLEAN      NOT NULL,
  is_month_end  BOOLEAN      NOT NULL,
  CONSTRAINT pk_dim_date              PRIMARY KEY (date_key),
  CONSTRAINT ck_dim_date_quarter      CHECK (quarter BETWEEN 1 AND 4),
  CONSTRAINT ck_dim_date_week_number  CHECK (week_number BETWEEN 1 AND 53),
  CONSTRAINT ck_dim_date_day_of_week  CHECK (day_of_week BETWEEN 1 AND 7)
) USING DELTA;

CREATE TABLE gold.dim_country (
  country_key  BIGINT        GENERATED ALWAYS AS IDENTITY,
  code_iso     CHAR(2)       NOT NULL,
  name         VARCHAR(100)  NOT NULL,
  CONSTRAINT pk_dim_country PRIMARY KEY (country_key)
) USING DELTA;

CREATE TABLE gold.dim_analytic_category (
  analytic_category_key  BIGINT        GENERATED ALWAYS AS IDENTITY,
  code                   VARCHAR(10)   NOT NULL,
  name                   VARCHAR(100)  NOT NULL,
  CONSTRAINT pk_dim_analytic_category       PRIMARY KEY (analytic_category_key),
  CONSTRAINT ck_dim_analytic_category_code  CHECK (code <> '')
) USING DELTA;

CREATE TABLE gold.dim_merchant (
  merchant_key        BIGINT        GENERATED ALWAYS AS IDENTITY,
  source_merchant_id  INT           NOT NULL,
  name                VARCHAR(250)  NOT NULL,
  country_code        CHAR(2)       NOT NULL,
  CONSTRAINT pk_dim_merchant PRIMARY KEY (merchant_key)
) USING DELTA;

CREATE TABLE gold.dim_customer (
  customer_key        BIGINT   GENERATED ALWAYS AS IDENTITY,
  source_customer_id  INT      NOT NULL,
  country_code        CHAR(2)  NOT NULL,
  CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
) USING DELTA;

CREATE TABLE gold.dim_product (
  product_key        BIGINT        GENERATED ALWAYS AS IDENTITY,
  source_product_id  INT           NOT NULL,
  name               VARCHAR(250)  NOT NULL,
  currency_code      CHAR(3)       NOT NULL,
  end_of_life        DATE,
  CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
) USING DELTA;

-- ---------------------------------------------------------------------
-- Faits (insertion seule)
-- Règle de signe : + paiement approuvé, - remboursement approuvé, 0 sinon.
-- Montants de base toujours positifs. Euro arrondi à 2 décimales (half up),
-- calculé une seule fois au chargement : amount_eur = amount x change_rate (pas de colonne calculé car immuable)
-- ---------------------------------------------------------------------

CREATE TABLE gold.fact_transaction (
  transaction_id                 BIGINT         NOT NULL,
  created_at                     TIMESTAMP      NOT NULL,
  date_key                       INT            NOT NULL,
  merchant_key                   BIGINT         NOT NULL,
  customer_key                   BIGINT         NOT NULL,
  country_key                    BIGINT         NOT NULL,   -- pays du paiement
  ip_country_key                 BIGINT,                    -- pays de l'IP
  currency_code                  CHAR(3)        NOT NULL,
  payment_method_type            STRING         NOT NULL,
  payment_method_brand           VARCHAR(30),
  transaction_type               STRING         NOT NULL,
  status                         STRING         NOT NULL,
  device_type                    STRING,
  bank_decline_code              VARCHAR(50),
  source_transaction_id          BIGINT,                    -- paiement d'origine d'un remboursement
  source_transaction_created_at  TIMESTAMP,
  fraud_score                    DECIMAL(5,4),
  fraud_decision                 STRING,
  model_version                  VARCHAR(50),
  amount                         DECIMAL(18,3)  NOT NULL,   -- devise d'origine
  change_rate                    DECIMAL(18,7)  NOT NULL,   -- 1 unité de devise = x EUR
  amount_eur                     DECIMAL(18,2)  NOT NULL,
  signed_amount_eur              DECIMAL(18,2)  NOT NULL,

  CONSTRAINT pk_fact_transaction              PRIMARY KEY (transaction_id, created_at),

  CONSTRAINT fk_fact_transaction_date         FOREIGN KEY (date_key)        REFERENCES gold.dim_date (date_key),
  CONSTRAINT fk_fact_transaction_merchant     FOREIGN KEY (merchant_key)    REFERENCES gold.dim_merchant (merchant_key),
  CONSTRAINT fk_fact_transaction_customer     FOREIGN KEY (customer_key)    REFERENCES gold.dim_customer (customer_key),
  CONSTRAINT fk_fact_transaction_country      FOREIGN KEY (country_key)     REFERENCES gold.dim_country (country_key),
  CONSTRAINT fk_fact_transaction_ip_country   FOREIGN KEY (ip_country_key)  REFERENCES gold.dim_country (country_key),

  CONSTRAINT ck_fact_transaction_type         CHECK (transaction_type IN ('payment', 'refund')),
  CONSTRAINT ck_fact_transaction_status       CHECK (status IN ('approved', 'rejected_by_fraud', 'rejected_by_bank', 'failed')),
  CONSTRAINT ck_fact_transaction_device       CHECK (device_type IS NULL OR device_type IN ('mobile', 'desktop', 'tablet')),
  CONSTRAINT ck_fact_transaction_method       CHECK (payment_method_type IN ('card', 'sepa', 'bank_transfer')),
  CONSTRAINT ck_fact_transaction_fraud_dec    CHECK (fraud_decision IS NULL OR fraud_decision IN ('accepted', 'rejected')),
  CONSTRAINT ck_fact_transaction_fraud_score  CHECK (fraud_score IS NULL OR fraud_score BETWEEN 0 AND 1),
  CONSTRAINT ck_fact_transaction_amount       CHECK (amount > 0),
  CONSTRAINT ck_fact_transaction_change_rate  CHECK (change_rate > 0),
  CONSTRAINT ck_fact_transaction_amount_eur   CHECK (amount_eur >= 0),
  CONSTRAINT ck_fact_transaction_source       CHECK ((transaction_type = 'refund') = (source_transaction_id IS NOT NULL)),
  CONSTRAINT ck_fact_transaction_refund_fraud CHECK (transaction_type = 'payment'
                                                     OR (fraud_score IS NULL AND fraud_decision IS NULL AND model_version IS NULL)),
  CONSTRAINT ck_fact_transaction_signed       CHECK (
        (status <> 'approved'                                AND signed_amount_eur = 0)
     OR (status = 'approved' AND transaction_type = 'payment' AND signed_amount_eur = amount_eur)
     OR (status = 'approved' AND transaction_type = 'refund'  AND signed_amount_eur = -amount_eur))
) USING DELTA;

CREATE TABLE gold.fact_payment_item (
  transaction_id          BIGINT         NOT NULL,   -- celui du paiement ou du remboursement
  created_at              TIMESTAMP      NOT NULL,   -- date de la transaction
  product_key             BIGINT         NOT NULL,
  date_key                INT            NOT NULL,
  merchant_key            BIGINT         NOT NULL,
  customer_key            BIGINT         NOT NULL,
  analytic_category_key   BIGINT         NOT NULL,   -- catégorie à l'achat
  currency_code           CHAR(3)        NOT NULL,
  transaction_type        STRING         NOT NULL,
  status                  STRING         NOT NULL,
  quantity                INT            NOT NULL,
  unit_price              DECIMAL(18,3)  NOT NULL,   -- devise de la transaction
  line_amount             DECIMAL(18,3)  NOT NULL,   -- quantity x unit_price
  change_rate             DECIMAL(18,7)  NOT NULL,   -- repris de la transaction
  line_amount_eur         DECIMAL(18,2)  NOT NULL,
  signed_line_amount_eur  DECIMAL(18,2)  NOT NULL,

  CONSTRAINT pk_fact_payment_item               PRIMARY KEY (transaction_id, created_at, product_key),

  CONSTRAINT fk_fact_payment_item_date          FOREIGN KEY (date_key)               REFERENCES gold.dim_date (date_key),
  CONSTRAINT fk_fact_payment_item_merchant      FOREIGN KEY (merchant_key)           REFERENCES gold.dim_merchant (merchant_key),
  CONSTRAINT fk_fact_payment_item_customer      FOREIGN KEY (customer_key)           REFERENCES gold.dim_customer (customer_key),
  CONSTRAINT fk_fact_payment_item_product       FOREIGN KEY (product_key)            REFERENCES gold.dim_product (product_key),
  CONSTRAINT fk_fact_payment_item_category      FOREIGN KEY (analytic_category_key)  REFERENCES gold.dim_analytic_category (analytic_category_key),

  CONSTRAINT ck_fact_payment_item_type          CHECK (transaction_type IN ('payment', 'refund')),
  CONSTRAINT ck_fact_payment_item_status        CHECK (status IN ('approved', 'rejected_by_fraud', 'rejected_by_bank', 'failed')),
  CONSTRAINT ck_fact_payment_item_refund_status CHECK (transaction_type = 'payment' OR status = 'approved'),
  CONSTRAINT ck_fact_payment_item_quantity      CHECK (quantity > 0),
  CONSTRAINT ck_fact_payment_item_unit_price    CHECK (unit_price >= 0),
  CONSTRAINT ck_fact_payment_item_line_amount   CHECK (line_amount >= 0),
  CONSTRAINT ck_fact_payment_item_change_rate   CHECK (change_rate > 0),
  CONSTRAINT ck_fact_payment_item_amount_eur    CHECK (line_amount_eur >= 0),
  CONSTRAINT ck_fact_payment_item_signed        CHECK (
        (status <> 'approved'                                AND signed_line_amount_eur = 0)
     OR (status = 'approved' AND transaction_type = 'payment' AND signed_line_amount_eur = line_amount_eur)
     OR (status = 'approved' AND transaction_type = 'refund'  AND signed_line_amount_eur = -line_amount_eur))
) USING DELTA;

-- ---------------------------------------------------------------------
-- Alimentation de dim_date (2020-01-01 à 2035-12-31)
-- ---------------------------------------------------------------------
INSERT INTO gold.dim_date
  (date_key, full_date, year, month, month_name, day_of_month, day_of_year, quarter, year_quarter,
   year_month, iso_year, week_number, day_of_week, day_name, is_weekend, is_month_end)
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT),
       d,
       year(d),
       month(d),
       date_format(d, 'MMMM'),
       day(d),
       dayofyear(d),
       quarter(d),
       concat(year(d), '-Q', quarter(d)),
       date_format(d, 'yyyy-MM'),
       year(date_add(d, 4 - (((dayofweek(d) + 5) % 7) + 1))),
       weekofyear(d),
       ((dayofweek(d) + 5) % 7) + 1,
       date_format(d, 'EEEE'),
       ((dayofweek(d) + 5) % 7) + 1 >= 6,
       d = last_day(d)
FROM (SELECT explode(sequence(DATE'2020-01-01', DATE'2035-12-31', INTERVAL 1 DAY)) AS d);