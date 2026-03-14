SELECT 'DISCOVER' src, COUNT(*) ct FROM FINANCE.RAW.DISCOVER_RAW
UNION ALL SELECT 'CHASE',   COUNT(*) FROM FINANCE.RAW.CHASE_RAW
UNION ALL SELECT 'CAPONE',  COUNT(*) FROM FINANCE.RAW.CAPONE_RAW
UNION ALL SELECT 'AMEX',    COUNT(*) FROM FINANCE.RAW.AMEX_RAW;

SELECT table_name, column_name, data_type, ordinal_position
FROM FINANCE.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'RAW'
  AND table_name IN ('DISCOVER_RAW','CHASE_RAW','CAPONE_RAW','AMEX_RAW')
ORDER BY table_name, ordinal_position;

CREATE OR REPLACE VIEW FINANCE.STG.V_CAPONE AS
SELECT
  'CAPONE'::STRING                         AS source,
  TO_VARCHAR("Card No.")                   AS account_id,
  "Transaction Date"                       AS txn_date,
  "Posted Date"                            AS post_date,
  "Description"::STRING                    AS description_raw,
  "Category"::STRING                       AS category_raw,
  "Debit"                                  AS debit,
  "Credit"                                 AS credit,
  (COALESCE("Debit", 0) - COALESCE("Credit", 0)) AS amount_signed,  -- ✅ now works
  IFF("Category" ILIKE '%payment%', TRUE, FALSE) AS is_payment
FROM FINANCE.RAW.CAPONE_RAW;



CREATE OR REPLACE VIEW FINANCE.STG.V_DISCOVER AS
SELECT
  'DISCOVER'::STRING                        AS source,
  'discover'::STRING                        AS account_id,
  "Trans. Date"                             AS txn_date,
  "Post Date"                               AS post_date,
  "Description"::STRING                     AS description_raw,
  "Category"::STRING                        AS category_raw,
  NULL::NUMBER                              AS debit,
  NULL::NUMBER                              AS credit,
  IFF(
    "Category" ILIKE '%payment%' OR "Category" ILIKE '%credit%',
    -ABS("Amount"),
    ABS("Amount")
  )                                         AS amount_signed,
  IFF("Category" ILIKE '%payment%', TRUE, FALSE) AS is_payment
FROM FINANCE.RAW.DISCOVER_RAW;

CREATE OR REPLACE VIEW FINANCE.STG.V_CHASE AS
SELECT
  'CHASE'::STRING                           AS source,
  'chase'::STRING                           AS account_id,
  "Transaction Date"                        AS txn_date,
  "Post Date"                               AS post_date,
  COALESCE(NULLIF("Description"::STRING,''), "Memo"::STRING) AS description_raw,
  "Category"::STRING                        AS category_raw,
  NULL::NUMBER                              AS debit,
  NULL::NUMBER                              AS credit,
  CASE
    WHEN "Type" ILIKE '%payment%' OR "Category" ILIKE '%payment%' THEN -ABS("Amount")
    WHEN "Type" ILIKE '%sale%' THEN ABS("Amount")
    ELSE
      /* fallback: if Amount is negative for sales, flip it; if positive, keep it */
      IFF("Amount" < 0, ABS("Amount"), "Amount")
  END                                       AS amount_signed,
  IFF("Type" ILIKE '%payment%' OR "Category" ILIKE '%payment%', TRUE, FALSE) AS is_payment,
  "Type"::STRING                            AS chase_type
FROM FINANCE.RAW.CHASE_RAW;

CREATE OR REPLACE VIEW FINANCE.STG.V_AMEX AS
SELECT
  'AMEX'::STRING                            AS source,
  'amex'::STRING                            AS account_id,
  "Date"                                    AS txn_date,
  "Date"                                    AS post_date,
  "Description"::STRING                     AS description_raw,
  "Category"::STRING                        AS category_raw,
  NULL::NUMBER                              AS debit,
  NULL::NUMBER                              AS credit,
  "Amount"                                  AS amount_signed,
  IFF(
    UPPER("Description") LIKE '%PAYMENT%' OR UPPER("Description") LIKE '%AUTOPAY%',
    TRUE, FALSE
  )                                         AS is_payment,
  "Appears On Your Statement As"::STRING    AS statement_merchant,
  "Country"::STRING                         AS country
FROM FINANCE.RAW.AMEX_RAW;

CREATE OR REPLACE VIEW FINANCE.STG.TRANSACTIONS AS
SELECT
  source, account_id, txn_date, post_date,
  description_raw, category_raw,
  amount_signed, is_payment,
  debit, credit,
  NULL::STRING AS statement_merchant,
  NULL::STRING AS country,
  NULL::STRING AS chase_type
FROM FINANCE.STG.V_CAPONE

UNION ALL
SELECT
  source, account_id, txn_date, post_date,
  description_raw, category_raw,
  amount_signed, is_payment,
  debit, credit,
  NULL, NULL, NULL
FROM FINANCE.STG.V_DISCOVER

UNION ALL
SELECT
  source, account_id, txn_date, post_date,
  description_raw, category_raw,
  amount_signed, is_payment,
  debit, credit,
  NULL, NULL,
  chase_type
FROM FINANCE.STG.V_CHASE

UNION ALL
SELECT
  source, account_id, txn_date, post_date,
  description_raw, category_raw,
  amount_signed, is_payment,
  debit, credit,
  statement_merchant, country, NULL
FROM FINANCE.STG.V_AMEX;

//Amount sanity
SELECT source, account_id, post_date, amount_signed, description_raw, category_raw
FROM FINANCE.STG.TRANSACTIONS
WHERE amount_signed > 0 AND NOT is_payment
ORDER BY amount_signed DESC
LIMIT 10;

//Payment sanity
SELECT source, account_id, post_date, amount_signed, description_raw, category_raw
FROM FINFINANCE.STGANCE.STG.TRANSACTIONS
WHERE is_payment
ORDER BY amount_signed ASC
LIMIT 10;

//null checks
SELECT
  source,
  SUM(IFF(post_date IS NULL, 1, 0)) AS null_post_date,
  SUM(IFF(description_raw IS NULL, 1, 0)) AS null_desc,
  SUM(IFF(amount_signed IS NULL, 1, 0)) AS null_amt
FROM FINANCE.STG.TRANSACTIONS
GROUP BY 1
ORDER BY 1;

SELECT post_date, debit, credit, amount_signed, description_raw, category_raw
FROM FINANCE.STG.V_CAPONE
ORDER BY post_date DESC
LIMIT 20;

