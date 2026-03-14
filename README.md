# Snowflake Financial Analysis

Load raw credit-card CSV/TSV exports (Discover / Chase / Capital One / Amex) into Snowflake, normalize them into a canonical transactions model, auto-group merchants via clustering, derive subscription candidates, and explore everything in a Streamlit dashboard running **inside Snowflake**.

## What this repo contains

- `01_raw_data_load.sql`  
  Creates warehouse/db/schemas, file formats, stage, raw tables (via schema inference), and example `COPY INTO` loads.

- `02_staging.sql`  
  Builds normalized staging views (`FINANCE.STG.*`) that unify issuer formats and normalize `amount_signed` + `is_payment`.

- `03_merchant_canonicalization.ipynb`  
  A Snowflake Notebook (SQL + Python) that:
  - creates `TXN_FEATURES` with a stable `merchant_hint`
  - clusters similar merchant hints (TF-IDF + cosine similarity)
  - writes `MERCHANT_CLUSTER_SUGGESTIONS`
  - promotes suggestions into `MERCHANT_CANONICAL_MAP`
  - builds `FINANCE.CURATED.TRANSACTIONS_ENRICHED`

- `04_streamlit_setup.sql`  
  Creates `FINANCE.MART.*` views used by the Streamlit dashboard (monthly spend, merchant leaderboard, subscriptions, etc.)

- `streamlit/dashboard.py`  
  Streamlit app code intended to run in **Streamlit in Snowflake** (Snowsight).

- `streamlit/environment.yml`  
  Minimal environment declaration for the Streamlit app.

---

## Data upload folder structure (important)

The pipeline assumes your staged files are organized under a single Snowflake stage:

```
@FINANCE.RAW.STAGE_SPEND/
  amex/
  capone/
  chase/
  discover/
```

Put each issuer’s exported files into its folder:

- `discover/` → Discover export files  
- `chase/` → Chase export files (all Chase cards can share format)  
- `capone/` → Capital One export files  
- `amex/` → Amex export files  

> Tip: If you have multiple cards per issuer and want per-card `account_id`, the best approach is to split into subfolders (e.g., `chase/sapphire/`, `chase/freedom/`) and load into separate raw tables. This repo currently loads one table per issuer, but the staging layer can be extended.

---

## Prerequisites

1. A Snowflake account with access to:
   - Snowsight Worksheets (SQL)
   - Snowflake Notebooks (optional but recommended for the `.ipynb`)
   - Streamlit in Snowflake (for the dashboard)

2. Ability to create:
   - Warehouse
   - Database/schemas
   - Stage + file formats
   - Tables/views

---

## Setup and run (from scratch)

### Step 1 — Create core objects + raw tables

Open **Snowsight → Worksheets** and run:

1) `01_raw_data_load.sql`

This creates:
- Warehouse: `FINANCE_WH`
- Database: `FINANCE`
- Schemas: `RAW`, `STG`, `CURATED`, `MART`
- Stage: `FINANCE.RAW.STAGE_SPEND`
- File formats (CSV + TSV)
- Raw tables (via `INFER_SCHEMA`)

Notes:
- This repo uses `PARSE_HEADER=TRUE` in file formats so `COPY INTO ... MATCH_BY_COLUMN_NAME` works.
- CapOne exports sometimes need special handling (encoding / blank trailing line). See troubleshooting.

### Step 2 — Upload your raw files to the stage

You have two common options:

#### Option A: Snowsight UI upload (simple)

Use Snowsight data load tooling (or stage file upload) to upload files into the correct subfolders under the stage.

#### Option B: SnowSQL `PUT` (repeatable)

From a terminal with SnowSQL configured:

```sql
PUT file:///path/to/discover/* @FINANCE.RAW.STAGE_SPEND/discover AUTO_COMPRESS=TRUE;
PUT file:///path/to/chase/*    @FINANCE.RAW.STAGE_SPEND/chase    AUTO_COMPRESS=TRUE;
PUT file:///path/to/capone/*   @FINANCE.RAW.STAGE_SPEND/capone   AUTO_COMPRESS=TRUE;
PUT file:///path/to/amex/*     @FINANCE.RAW.STAGE_SPEND/amex     AUTO_COMPRESS=TRUE;
```

Confirm files are present:

```sql
LIST @FINANCE.RAW.STAGE_SPEND/discover/;
LIST @FINANCE.RAW.STAGE_SPEND/chase/;
LIST @FINANCE.RAW.STAGE_SPEND/capone/;
LIST @FINANCE.RAW.STAGE_SPEND/amex/;
```

### Step 3 — Load staged files into raw tables

Run the `COPY INTO` commands in `01_raw_data_load.sql` (or rerun just that section). Example:

```sql
COPY INTO FINANCE.RAW.DISCOVER_RAW
FROM @FINANCE.RAW.STAGE_SPEND/discover/
FILE_FORMAT = (FORMAT_NAME='FINANCE.RAW.FF_CSV')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

Repeat for each issuer folder/table.

Verify raw loads:

```sql
SELECT 'DISCOVER' src, COUNT(*) ct FROM FINANCE.RAW.DISCOVER_RAW
UNION ALL SELECT 'CHASE', COUNT(*) FROM FINANCE.RAW.CHASE_RAW
UNION ALL SELECT 'CAPONE', COUNT(*) FROM FINANCE.RAW.CAPONE_RAW
UNION ALL SELECT 'AMEX', COUNT(*) FROM FINANCE.RAW.AMEX_RAW;
```

---

## Normalize into a canonical transactions layer

### Step 4 — Build staging views

Run:

2) `02_staging.sql`

This creates:
- `FINANCE.STG.V_DISCOVER`
- `FINANCE.STG.V_CHASE`
- `FINANCE.STG.V_CAPONE`
- `FINANCE.STG.V_AMEX`
- `FINANCE.STG.TRANSACTIONS` (union of the above)

Key STG columns:
- `source`, `account_id`
- `txn_date`, `post_date`
- `description_raw`, `category_raw`
- `amount_signed` (normalized so spend is positive)
- `is_payment`

---

## Merchant canonicalization (clustering) + curated layer

### Step 5 — Run the notebook

Open:

3) `03_merchant_canonicalization.ipynb`

This notebook:
- creates `FINANCE.CURATED.TXN_FEATURES` with `merchant_hint`
- computes `MERCHANT_HINT_STATS`
- clusters merchant hints and writes:
  - `FINANCE.CURATED.MERCHANT_CLUSTER_SUGGESTIONS`
- merges clusters into:
  - `FINANCE.CURATED.MERCHANT_CANONICAL_MAP`
- creates:
  - `FINANCE.CURATED.TRANSACTIONS_ENRICHED` (adds `merchant_final`)

Verify merchant coverage:

```sql
SELECT merchant_final, COUNT(*) ct
FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
WHERE amount_signed > 0 AND NOT is_payment
GROUP BY 1
ORDER BY ct DESC
LIMIT 30;
```

If you re-run clustering later after adding more data, the map updates via `MERGE`.

---

## MART layer + Streamlit

### Step 6 — Create MART views

Run:

4) `04_streamlit_setup.sql`

This creates `FINANCE.MART.*` views used by Streamlit, including:
- monthly spend
- merchant leaderboard
- monthly subscription candidates
- any helper views used by the app

### Step 7 — Create Streamlit app in Snowflake

In Snowsight:
1. Go to **Projects → Streamlit**
2. Create a new Streamlit app
3. Choose:
   - Warehouse: `FINANCE_WH`
   - Database: `FINANCE`
   - Schema: (recommended) `MART` or a dedicated app schema
4. Paste in `streamlit/dashboard.py`

The dashboard shows:
- Monthly total spend (bar chart)
- Top merchants leaderboard
- Monthly subscription candidates
- Largest transactions leaderboard
- “Top transactions for selected merchant” (dropdown + top N)

---

## Re-running with new files

When you add new exports later:

1) Upload new files into the same stage folder structure  
2) Re-run the relevant `COPY INTO ...` statements for those folders  
3) Re-run:
- `02_staging.sql` (safe; recreates views)
- `03_merchant_canonicalization.ipynb` (refresh clusters and mappings)
- `04_streamlit_setup.sql` (safe; recreates views)

---

## Common gotchas / troubleshooting

### “All NULLs after COPY INTO”

Usually means `MATCH_BY_COLUMN_NAME` didn’t match header names to table columns. Ensure:
- file format has `PARSE_HEADER=TRUE`
- table was created from schema inference that used the same file format
- headers match column names

### “INFER_SCHEMA template must be a non-null JSON array”

Means `INFER_SCHEMA` returned 0 rows (often wrong path or wrong file format). Confirm:
- `LIST @stage/path/` shows files
- delimiter/encoding is correct
- there isn’t a structurally empty record (blank line) blocking inference

### CapOne: “header has 7 cols but row becomes 1 col”

Often a delimiter/encoding mismatch (or using TSV format on CSV). Preview using a named file format, and set `ENCODING` or `SKIP_BLANK_LINES` if needed.

---

## Relevant Snowflake documentation

- Stages (`CREATE STAGE`): https://docs.snowflake.com/en/sql-reference/sql/create-stage  
- File formats (`CREATE FILE FORMAT`): https://docs.snowflake.com/en/sql-reference/sql/create-file-format  
- Loading data (`COPY INTO <table>`): https://docs.snowflake.com/en/sql-reference/sql/copy-into-table  
- Uploading local files (`PUT`): https://docs.snowflake.com/en/sql-reference/sql/put  
- Schema inference (`INFER_SCHEMA`): https://docs.snowflake.com/en/sql-reference/functions/infer_schema  
- Snowflake Notebooks: https://docs.snowflake.com/en/user-guide/ui-snowsight/notebooks  
- Streamlit in Snowflake (overview): https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit  

---

## Security / privacy note

These CSV exports contain sensitive financial data.

- Do **not** commit raw CSVs to git.
- Prefer a private Snowflake account/role.
- Consider masking/tokenizing if you ever share screenshots or sample data.
