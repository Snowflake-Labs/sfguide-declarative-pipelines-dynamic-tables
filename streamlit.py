import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

# Get the current Snowflake session
session = get_active_session()

# Set page config
st.set_page_config(page_title="Tasty Bytes Analytics", layout="wide")

# Title and description
st.title("🍔 Tasty Bytes Business Dashboard")

st.subheader("Top 10 Products by Revenue")

# Query product performance metrics using Snowpark
top_products = session.table("tasty_bytes_db.analytics.product_performance_metrics") \
    .select("MENU_ITEM_NAME", "TOTAL_REVENUE", "TOTAL_PROFIT", "AVG_PROFIT_MARGIN_PCT", "TOTAL_UNITS_SOLD") \
    .order_by("TOTAL_REVENUE", ascending=False) \
    .limit(10) \
    .to_pandas()

# Convert revenue to millions for cleaner axis labels
top_products['REVENUE_MILLIONS'] = top_products['TOTAL_REVENUE'] / 1_000_000

# Top products bar chart
products_chart = alt.Chart(top_products).mark_bar().encode(
    x=alt.X('REVENUE_MILLIONS:Q', title='Total Revenue (Millions $)', axis=alt.Axis(format='.0f')),
    y=alt.Y('MENU_ITEM_NAME:N', title='Product', sort='-x'),
    color=alt.Color('AVG_PROFIT_MARGIN_PCT:Q',
                   title='Profit Margin %',
                   scale=alt.Scale(scheme='viridis')),
    tooltip=[
        alt.Tooltip('MENU_ITEM_NAME:N', title='Product'),
        alt.Tooltip('TOTAL_REVENUE:Q', title='Revenue', format='$,.0f'),
        alt.Tooltip('TOTAL_PROFIT:Q', title='Profit', format='$,.0f'),
        alt.Tooltip('AVG_PROFIT_MARGIN_PCT:Q', title='Margin %', format='.1f'),
        alt.Tooltip('TOTAL_UNITS_SOLD:Q', title='Units Sold', format=',')
    ]
).properties(
    height=400
).interactive()

st.altair_chart(products_chart, use_container_width=True)

# Bottom section - Key metrics cards
st.markdown("---")
st.subheader("📊 Today's Key Metrics")

# Get today's metrics using Snowpark
today_metrics = session.table("tasty_bytes_db.analytics.daily_business_metrics") \
    .select("TOTAL_ORDERS", "TOTAL_REVENUE", "TOTAL_PROFIT", "AVG_PROFIT_MARGIN_PCT", "UNIQUE_CUSTOMERS", "TOTAL_ITEMS_SOLD") \
    .order_by("ORDER_DATE", ascending=False) \
    .limit(1) \
    .to_pandas()

if not today_metrics.empty:
    metric_cols = st.columns(6)

    with metric_cols[0]:
        st.metric("Total Orders", f"{today_metrics['TOTAL_ORDERS'].iloc[0]:,.0f}")

    with metric_cols[1]:
        st.metric("Revenue", f"${today_metrics['TOTAL_REVENUE'].iloc[0]:,.0f}")

    with metric_cols[2]:
        st.metric("Profit", f"${today_metrics['TOTAL_PROFIT'].iloc[0]:,.0f}")

    with metric_cols[3]:
        st.metric("Profit Margin", f"{today_metrics['AVG_PROFIT_MARGIN_PCT'].iloc[0]:.1f}%")

    with metric_cols[4]:
        st.metric("Customers", f"{today_metrics['UNIQUE_CUSTOMERS'].iloc[0]:,.0f}")

    with metric_cols[5]:
        st.metric("Items Sold", f"{today_metrics['TOTAL_ITEMS_SOLD'].iloc[0]:,.0f}")