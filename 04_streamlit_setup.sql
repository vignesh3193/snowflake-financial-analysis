CREATE OR REPLACE VIEW FINANCE.MART.MONTHLY_SPEND AS
SELECT
  DATE_TRUNC('month', post_date) AS month,
  SUM(IFF(amount_signed > 0 AND NOT is_payment, amount_signed, 0)) AS spend,
  SUM(IFF(amount_signed < 0, -amount_signed, 0)) AS credits
FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
GROUP BY 1
ORDER BY 1;


CREATE OR REPLACE VIEW FINANCE.MART.MERCHANT_SPEND AS
SELECT
  merchant_final,
  SUM(amount_signed) AS spend,
  COUNT(*) AS txn_count,
  MIN(post_date) AS first_seen,
  MAX(post_date) AS last_seen
FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
WHERE amount_signed > 0 AND NOT is_payment
GROUP BY 1
ORDER BY spend DESC;

CREATE OR REPLACE VIEW FINANCE.MART.SUBSCRIPTIONS_MONTHLY AS
WITH base AS (
  SELECT
    f.account_id,
    COALESCE(m.merchant_canonical, 'UNKNOWN') AS merchant,
    f.post_date,
    f.amount_signed AS amt,
    DAY(f.post_date) AS dom
  FROM FINANCE.CURATED.TXN_FEATURES f
  LEFT JOIN FINANCE.CURATED.MERCHANT_CANONICAL_MAP m
    ON f.merchant_hint = m.merchant_hint
  WHERE f.amount_signed > 0
    AND NOT f.is_payment
    AND f.post_date IS NOT NULL
    AND COALESCE(m.merchant_canonical, 'UNKNOWN') <> 'UNKNOWN'
),
seq AS (
  SELECT
    *,
    DATEDIFF('day',
      LAG(post_date) OVER (PARTITION BY account_id, merchant ORDER BY post_date),
      post_date
    ) AS gap_days
  FROM base
),
monthlyish AS (
  SELECT *
  FROM seq
  WHERE gap_days IS NOT NULL
    AND gap_days BETWEEN 25 AND 35
),
dom_mode AS (
  -- find the most common day-of-month per (account, merchant)
  SELECT
    account_id,
    merchant,
    dom AS mode_dom,
    COUNT(*) AS dom_ct
  FROM base
  GROUP BY 1,2,3
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY dom_ct DESC, dom) = 1
),
filtered AS (
  SELECT
    b.*
  FROM base b
  JOIN dom_mode d
    ON b.account_id = d.account_id
   AND b.merchant   = d.merchant
  -- allow ±1 day tolerance
  WHERE ABS(b.dom - d.mode_dom) <= 1
),
monthly_filtered AS (
  SELECT
    f.*,
    DATEDIFF('day',
      LAG(post_date) OVER (PARTITION BY account_id, merchant ORDER BY post_date),
      post_date
    ) AS gap_days2
  FROM filtered f
)
SELECT
  account_id,
  merchant,
  COUNT(*) AS count,
  ROUND(AVG(amt), 2) AS avg_charge_cost,
  ROUND(SUM(amt), 2) AS total_cost,
  MIN(post_date) AS first_seen,
  MAX(post_date) AS last_seen
FROM monthly_filtered
WHERE gap_days2 IS NULL OR gap_days2 BETWEEN 25 AND 35
GROUP BY 1,2
HAVING COUNT(*) >= 3
ORDER BY total_cost DESC;



CREATE OR REPLACE VIEW FINANCE.MART.UNKNOWN_SPEND AS
SELECT
  post_date,
  source,
  account_id,
  merchant_hint,
  description_raw,
  category_raw,
  amount_signed
FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
WHERE merchant_final = 'UNKNOWN'
  AND amount_signed > 0
  AND NOT is_payment;

Select * from FINANCE.CURATED.TRANSACTIONS_ENRICHED where TXN_DATE>'2025-01-01' and TXN_DATE<'2025-01-31' and IS_PAYMENT = FALSE
