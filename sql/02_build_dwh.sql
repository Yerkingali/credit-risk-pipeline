/* =========================================================
   1) DWH таблицы (идемпотентное создание)
   ========================================================= */
IF OBJECT_ID('dwh.clients','U') IS NULL
CREATE TABLE dwh.clients (
  client_id     INT PRIMARY KEY,
  gender        CHAR(1) NULL,
  income        DECIMAL(18,2) NULL,
  education     NVARCHAR(100) NULL,
  family_status NVARCHAR(100) NULL,
  region_rating INT NULL
);

IF OBJECT_ID('dwh.loans','U') IS NULL
CREATE TABLE dwh.loans (
  loan_id        INT PRIMARY KEY,
  client_id      INT NOT NULL REFERENCES dwh.clients(client_id),
  product_type   NVARCHAR(50) NULL,
  principal      DECIMAL(18,2) NULL,
  annuity        DECIMAL(18,2) NULL,
  goods_price    DECIMAL(18,2) NULL,
  status_default BIT NULL
);

IF OBJECT_ID('dwh.payments','U') IS NULL
CREATE TABLE dwh.payments (
  payment_id     BIGINT IDENTITY(1,1) PRIMARY KEY,
  loan_id        INT NOT NULL REFERENCES dwh.loans(loan_id),
  instalment_no  INT NULL,
  due_day_offset INT NULL,
  pay_day_offset INT NULL,
  due_amount     DECIMAL(18,2) NULL,
  paid_amount    DECIMAL(18,2) NULL,
  dpd_days       INT NULL
);

/* индексы (по желанию, ускоряют агрегации/соединения) */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_payments_loan' AND object_id=OBJECT_ID('dwh.payments'))
    CREATE INDEX IX_payments_loan ON dwh.payments(loan_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_loans_client' AND object_id=OBJECT_ID('dwh.loans'))
    CREATE INDEX IX_loans_client ON dwh.loans(client_id);

/* =========================================================
   2) Перелив из RAW в DWH (без дублей)
   ========================================================= */

-- clients
INSERT INTO dwh.clients (client_id, gender, income, education, family_status, region_rating)
SELECT DISTINCT
  TRY_CONVERT(INT, a.[SK_ID_CURR])                                  AS client_id,
  CASE WHEN a.[CODE_GENDER] IN ('M','F') THEN a.[CODE_GENDER] END    AS gender,
  TRY_CONVERT(DECIMAL(18,2), a.[AMT_INCOME_TOTAL])                   AS income,
  a.[NAME_EDUCATION_TYPE]                                            AS education,
  a.[NAME_FAMILY_STATUS]                                             AS family_status,
  TRY_CONVERT(INT, a.[REGION_RATING_CLIENT])                         AS region_rating
FROM raw.application_train a
WHERE TRY_CONVERT(INT, a.[SK_ID_CURR]) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dwh.clients c
      WHERE c.client_id = TRY_CONVERT(INT, a.[SK_ID_CURR])
  );

-- loans (по одному кредиту на клиента — достаточно для демо)
INSERT INTO dwh.loans (loan_id, client_id, product_type, principal, annuity, goods_price, status_default)
SELECT
  TRY_CONVERT(INT, a.[SK_ID_CURR])                                   AS loan_id,
  TRY_CONVERT(INT, a.[SK_ID_CURR])                                   AS client_id,
  a.[NAME_CONTRACT_TYPE]                                             AS product_type,
  TRY_CONVERT(DECIMAL(18,2), a.[AMT_CREDIT])                         AS principal,
  TRY_CONVERT(DECIMAL(18,2), a.[AMT_ANNUITY])                        AS annuity,
  TRY_CONVERT(DECIMAL(18,2), a.[AMT_GOODS_PRICE])                    AS goods_price,
  CASE WHEN TRY_CONVERT(INT, a.[TARGET]) = 1 THEN 1 ELSE 0 END       AS status_default
FROM raw.application_train a
WHERE TRY_CONVERT(INT, a.[SK_ID_CURR]) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dwh.loans l
      WHERE l.loan_id = TRY_CONVERT(INT, a.[SK_ID_CURR])
  );

-- payments (исправлено: только для существующих займов; защита от дублей)
INSERT INTO dwh.payments
    (loan_id, instalment_no, due_day_offset, pay_day_offset, due_amount, paid_amount, dpd_days)
SELECT
    l.loan_id,
    TRY_CONVERT(INT, p.[NUM_INSTALMENT_NUMBER])                       AS instalment_no,
    TRY_CONVERT(INT, p.[DAYS_INSTALMENT])                             AS due_day_offset,
    TRY_CONVERT(INT, p.[DAYS_ENTRY_PAYMENT])                          AS pay_day_offset,
    TRY_CONVERT(DECIMAL(18,2), p.[AMT_INSTALMENT])                    AS due_amount,
    TRY_CONVERT(DECIMAL(18,2), p.[AMT_PAYMENT])                       AS paid_amount,
    CASE
        WHEN TRY_CONVERT(INT, p.[DAYS_ENTRY_PAYMENT]) IS NULL
          OR TRY_CONVERT(INT, p.[DAYS_INSTALMENT])   IS NULL THEN NULL
        WHEN TRY_CONVERT(INT, p.[DAYS_ENTRY_PAYMENT]) - TRY_CONVERT(INT, p.[DAYS_INSTALMENT]) > 0
          THEN TRY_CONVERT(INT, p.[DAYS_ENTRY_PAYMENT]) - TRY_CONVERT(INT, p.[DAYS_INSTALMENT])
        ELSE 0
    END                                                               AS dpd_days
FROM raw.installments_payments p
JOIN dwh.loans l
  ON l.loan_id = TRY_CONVERT(INT, p.[SK_ID_CURR])                     -- ключевой JOIN, устраняет конфликт FK
WHERE TRY_CONVERT(INT, p.[SK_ID_CURR]) IS NOT NULL
  AND NOT EXISTS (                                                    -- анти-дубли
      SELECT 1
      FROM dwh.payments x
      WHERE x.loan_id        = l.loan_id
        AND x.instalment_no  = TRY_CONVERT(INT, p.[NUM_INSTALMENT_NUMBER])
        AND x.due_day_offset = TRY_CONVERT(INT, p.[DAYS_INSTALMENT])
        AND x.pay_day_offset = TRY_CONVERT(INT, p.[DAYS_ENTRY_PAYMENT])
  );

/* =========================================================
   3) Представления (features + финальный датасет)
   ========================================================= */

-- агрегаты по платежному поведению
IF OBJECT_ID('dwh.v_loan_pay_features','V') IS NOT NULL DROP VIEW dwh.v_loan_pay_features;
EXEC('CREATE VIEW dwh.v_loan_pay_features AS
  SELECT
    loan_id,
    COUNT(*)                                   AS n_instalments,
    AVG(COALESCE(dpd_days,0))                  AS avg_dpd,
    MAX(COALESCE(dpd_days,0))                  AS max_dpd,
    SUM(CASE WHEN dpd_days>=30 THEN 1 ELSE 0 END) AS cnt_dpd30p,
    SUM(CASE WHEN dpd_days>=60 THEN 1 ELSE 0 END) AS cnt_dpd60p,
    SUM(CASE WHEN dpd_days>=90 THEN 1 ELSE 0 END) AS cnt_dpd90p,
    SUM(CASE WHEN COALESCE(paid_amount,0) < COALESCE(due_amount,0) THEN 1 ELSE 0 END) AS cnt_underpay,
    SUM(CASE WHEN COALESCE(paid_amount,0) = 0 THEN 1 ELSE 0 END) AS cnt_missed
  FROM dwh.payments
  GROUP BY loan_id
');

-- финальная витрина для модели PD
IF OBJECT_ID('dwh.v_model_dataset','V') IS NOT NULL DROP VIEW dwh.v_model_dataset;
EXEC('CREATE VIEW dwh.v_model_dataset AS
  SELECT
    l.loan_id, l.client_id,
    c.gender, c.income, c.education, c.family_status, c.region_rating,
    l.product_type, l.principal, l.annuity, l.goods_price,
    f.n_instalments, f.avg_dpd, f.max_dpd, f.cnt_dpd30p, f.cnt_dpd60p, f.cnt_dpd90p,
    f.cnt_underpay, f.cnt_missed,
    l.status_default AS default_flag
  FROM dwh.loans l
  LEFT JOIN dwh.clients c          ON c.client_id = l.client_id
  LEFT JOIN dwh.v_loan_pay_features f ON f.loan_id  = l.loan_id
');
