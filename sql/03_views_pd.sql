/* ===========================================================================
   03_views_pd.sql
   Credit Risk Pipeline — Probability of Default (PD) и Expected Loss (EL)
   ---------------------------------------------------------------------------
   Этот файл создаёт:
     1) таблицу dwh.stg_pd_scores — куда Python заливает loan_id + pd_score
     2) вьюху dwh.v_pd_scores — объединяет признаки и прогноз PD
     3) вьюху dwh.v_expected_loss — считает EL = PD * LGD * EAD
   =========================================================================== */


/* 1️⃣ Стейджинговая таблица для PD-скорингов */
IF OBJECT_ID('dwh.stg_pd_scores', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.stg_pd_scores (
        loan_id  INT   PRIMARY KEY,
        pd_score FLOAT NULL
    );
END
GO


/* 2️⃣ Вьюха: модельные признаки + PD */
CREATE OR ALTER VIEW dwh.v_pd_scores AS
SELECT
    l.loan_id,
    l.client_id,
    l.gender,
    l.income,
    l.education,
    l.family_status,
    l.region_rating,
    l.product_type,
    l.principal      AS amt_credit,
    l.annuity        AS amt_annuity,
    l.goods_price    AS amt_goods_price,
    l.n_instalments,
    l.avg_dpd,
    l.max_dpd,
    l.cnt_dpd30p,
    l.cnt_dpd60p,
    l.cnt_dpd90p,
    l.cnt_underpay,
    l.cnt_missed,
    s.pd_score,                 -- скоринг, залитый Python
    l.default_flag              -- факт дефолта (таргет)
FROM dwh.v_model_dataset AS l
LEFT JOIN dwh.stg_pd_scores AS s
       ON s.loan_id = l.loan_id;
GO


/* 3️⃣ Вьюха: ожидаемый убыток (EL = PD * LGD * EAD)
   LGD (Loss Given Default) задаётся константой = 0.45 */
CREATE OR ALTER VIEW dwh.v_expected_loss AS
SELECT
    p.loan_id,
    p.client_id,
    ISNULL(p.pd_score, 0.0) AS PD,
    p.amt_credit            AS EAD,
    0.45                    AS LGD,
    ISNULL(p.pd_score, 0.0) * 0.45 * p.amt_credit AS EL
FROM dwh.v_pd_scores AS p;
GO
