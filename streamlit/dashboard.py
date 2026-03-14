import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Spending Insights", layout="wide")
session = get_active_session()

@st.cache_data(ttl=300)
def read_df(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()

@st.cache_data(ttl=300)
def merchant_options(year: int):
    df = read_df(f"""
    SELECT merchant_final
    FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
    WHERE post_date >= '{year}-01-01'::DATE
      AND post_date <  '{year+1}-01-01'::DATE
      AND amount_signed > 0
      AND NOT is_payment
      AND merchant_final <> 'UNKNOWN'
    GROUP BY 1
    ORDER BY SUM(amount_signed) DESC
    """)
    return df["MERCHANT_FINAL"].tolist()

st.title("Personal Spending Insights (Snowflake)")

# ----------------------------
# Sidebar controls
# ----------------------------
st.sidebar.header("Filters")

year = st.sidebar.selectbox("Year", [2025, 2024, 2023, 2022], index=0)

start_date = f"{year}-01-01"
end_date = f"{year + 1}-01-01"
st.sidebar.caption(f"Date range: {start_date} → {year}-12-31")

# ----------------------------
# KPIs
# ----------------------------
kpi = read_df(f"""
SELECT
  COALESCE(SUM(IFF(amount_signed > 0 AND NOT is_payment, amount_signed, 0)), 0) AS total_spend,
  COUNT_IF(amount_signed > 0 AND NOT is_payment) AS spend_txns
FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
WHERE post_date >= '{start_date}'::DATE
  AND post_date <  '{end_date}'::DATE
""").iloc[0]

c1, c2 = st.columns(2)
c1.metric("Total spend", f"${float(kpi['TOTAL_SPEND']):,.2f}")
c2.metric("Spend transactions", int(kpi["SPEND_TXNS"]))

st.divider()

# ----------------------------
# Monthly spend (bar chart / histogram style) - Jan..Dec of selected year
# ----------------------------
st.subheader("Monthly total spend")

monthly = read_df(f"""
SELECT
  DATE_TRUNC('month', post_date) AS month_start,
  COALESCE(SUM(amount_signed), 0) AS spend
FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
WHERE post_date >= '{start_date}'::DATE
  AND post_date <  '{end_date}'::DATE
  AND amount_signed > 0
  AND NOT is_payment
GROUP BY 1
ORDER BY 1
""")

if monthly.empty:
    st.info("No spend data for this year.")
else:
    monthly["MONTH_START"] = pd.to_datetime(monthly["MONTH_START"])
    monthly["MONTH_LABEL"] = monthly["MONTH_START"].dt.strftime("%b")      # Jan, Feb, ...
    monthly["MONTH_ORDER"] = monthly["MONTH_START"].dt.month               # 1..12

    chart = (
        alt.Chart(monthly)
        .mark_bar(size=28)  # thicker bars
        .encode(
            x=alt.X(
                "MONTH_LABEL:N",
                sort=alt.SortField(field="MONTH_ORDER", order="ascending"),
                axis=alt.Axis(title=None)
            ),
            y=alt.Y("SPEND:Q", axis=alt.Axis(title="Spend")),
            tooltip=[
                alt.Tooltip("MONTH_START:T", title="Month", format="%b %Y"),
                alt.Tooltip("SPEND:Q", title="Spend", format=",.2f"),
            ],
        )
        .properties(height=350)
    )

    st.altair_chart(chart, use_container_width=True)


# ----------------------------
# Top merchants + Subscriptions
# ----------------------------
colA, colB = st.columns([1.2, 1.5])

with colA:
    st.subheader("Top merchants")
    top_merchants = read_df(f"""
    SELECT
      merchant_final,
      ROUND(SUM(amount_signed), 2) AS spend,
      COUNT(*) AS txn_count
    FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
    WHERE post_date >= '{start_date}'::DATE
      AND post_date <  '{end_date}'::DATE
      AND amount_signed > 0
      AND NOT is_payment
    GROUP BY 1
    ORDER BY spend DESC
    LIMIT 30
    """)
    st.dataframe(top_merchants, use_container_width=True, hide_index=True)

with colB:
    st.subheader("Subscription candidates (monthly)")
    subs = read_df(f"""
    SELECT merchant, avg_charge_cost, total_cost
    FROM FINANCE.MART.SUBSCRIPTIONS_MONTHLY
    WHERE last_seen >= '{start_date}'::DATE
      AND last_seen <  '{end_date}'::DATE
    ORDER BY total_cost DESC
    LIMIT 50
    """)
    st.dataframe(subs, use_container_width=True, hide_index=True)
    st.metric("Subscriptions (in year)", 0 if subs.empty else len(subs))


st.divider()

# ----------------------------
# Largest transactions leaderboard
# ----------------------------
st.subheader("Top transactions (spend)")
unknown_min = st.slider(
    "Threshold ($)",
    min_value=0.0,
    max_value=2000.0,
    value=50.0,
    step=5.0
)
largest = read_df(f"""
SELECT
  post_date, source, merchant_final, amount_signed, description_raw, category_raw
FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
WHERE post_date >= '{start_date}'::DATE
  AND post_date <  '{end_date}'::DATE
  AND amount_signed > {unknown_min}
  AND NOT is_payment
ORDER BY amount_signed DESC
LIMIT 30
""")
st.dataframe(largest, use_container_width=True, hide_index=True)

st.divider()

# ----------------------------
# Largest transactions within a merchant
# ----------------------------

st.subheader("Top transactions for selected merchant")
merchants = merchant_options(year)
selected_merchant = st.selectbox(
    "Merchant (top transactions)",
    options=merchants if merchants else ["(none)"],
    index=0
)
top_n = st.slider("Top transactions to show", 5, 50, 10, 5)

if selected_merchant == "(none)":
    st.info("No merchants found for the selected year.")
else:
    merchant_sql = selected_merchant.replace("'", "''")  # escape single quotes for SQL

    summary = read_df(f"""
    SELECT
      ROUND(SUM(amount_signed), 2) AS spend,
      COUNT(*) AS txn_count,
      MIN(post_date) AS first_seen,
      MAX(post_date) AS last_seen
    FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
    WHERE post_date >= '{start_date}'::DATE
      AND post_date <  '{end_date}'::DATE
      AND amount_signed > 0
      AND NOT is_payment
      AND TRIM(merchant_final) = TRIM('{merchant_sql}')
    """).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Spend", f"${float(summary['SPEND'] or 0):,.2f}")
    c2.metric("Transactions", int(summary["TXN_COUNT"] or 0))
    c3.metric("First seen", str(summary["FIRST_SEEN"]))
    c4.metric("Last seen", str(summary["LAST_SEEN"]))

    top_txns = read_df(f"""
    SELECT
      post_date, account_id,
      merchant_final,
      amount_signed,
      description_raw,
      category_raw
    FROM FINANCE.CURATED.TRANSACTIONS_ENRICHED
    WHERE post_date >= '{start_date}'::DATE
      AND post_date <  '{end_date}'::DATE
      AND amount_signed > 0
      AND NOT is_payment
      AND TRIM(merchant_final) = TRIM('{merchant_sql}')
    ORDER BY amount_signed DESC
    LIMIT {top_n}
    """)
    st.dataframe(top_txns, use_container_width=True, hide_index=True)

