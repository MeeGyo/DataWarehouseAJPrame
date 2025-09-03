import streamlit as st
import duckdb as dd
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

# -----------------------------
# ✅ Page Config & Theming
# -----------------------------
st.set_page_config(
    page_title="Bikestore Business Dashboard",
    layout="wide",
    page_icon="🚲"
)

# Minimal CSS ปรับให้ดูโล่ง อ่านง่าย
st.markdown(
    """
    <style>
    .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
    .metric-card {border-radius: 16px; padding: 18px 18px 8px 18px; box-shadow: 0 4px 20px rgba(0,0,0,0.06);}
    .section-title {margin-top: 8px;}
    .dataframe td {font-size: 0.92rem;}
    .stTabs [data-baseweb="tab-list"] {gap: 8px;}
    .stTabs [data-baseweb="tab"] {border-radius: 12px; padding: 8px 12px;}
    .footnote {color: #6b7280; font-size: 0.86rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# 🔧 Utils
# -----------------------------
@st.cache_data(show_spinner=False)
def load_tables(db_path: str):
    conn = dd.connect(db_path)
    try:
        dim_customers = conn.execute("SELECT * FROM dim_customers").fetchdf()
        dim_date      = conn.execute("SELECT * FROM dim_date").fetchdf()
        dim_staffs    = conn.execute("SELECT * FROM dim_staffs").fetchdf()
        dim_products  = conn.execute("SELECT * FROM dim_products").fetchdf()
        dim_brands    = conn.execute("SELECT * FROM dim_brands").fetchdf()
        dim_categories= conn.execute("SELECT * FROM dim_categories").fetchdf()
        dim_stores    = conn.execute("SELECT * FROM dim_stores").fetchdf()
        fact_sales    = conn.execute("SELECT * FROM fact_sales").fetchdf()
    finally:
        conn.close()
    return (
        dim_customers, dim_date, dim_staffs, dim_products,
        dim_brands, dim_categories, dim_stores, fact_sales
    )


def baht(x):
    try:
        return f"฿{x:,.0f}"
    except Exception:
        return "-"

def pct(x):
    try:
        return f"{x*100:.1f}%"
    except Exception:
        return "-"


def add_period_cols(df):
    df = df.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year']   = df['order_date'].dt.year
    df['quarter']= df['order_date'].dt.to_period('Q').astype(str)
    df['month']  = df['order_date'].dt.to_period('M').astype(str)
    df['date']   = df['order_date'].dt.date
    return df


def compute_net_sales(df):
    df = df.copy()
    # net_sales = quantity * list_price * (1 - discount)
    df['net_sales'] = df['quantity'] * df['list_price'] * (1 - df['discount'])
    return df


def growth_rate(series: pd.Series):
    if len(series) < 2:
        return 0.0
    prev, curr = series.iloc[-2], series.iloc[-1]
    if prev == 0:
        return np.nan
    return (curr - prev) / prev

# -----------------------------
# 🎛️ Sidebar – ฟิลเตอร์
# -----------------------------
st.sidebar.title("⚙️ ตัวกรองข้อมูล")

def_path = r"C:\Users\Lenovo\Desktop\bikestore\data_warehouse\bikestore.duckdb"
DB_PATH = st.sidebar.text_input("DuckDB path", value=def_path, help="ปรับ path ตามเครื่องของคุณ")

(
    dim_customers, dim_date, dim_staffs, dim_products,
    dim_brands, dim_categories, dim_stores, fact_sales
) = load_tables(DB_PATH)

# ทำงานด้วย pandas ทั้งหมดเพื่อความสม่ำเสมอ
customers = dim_customers.copy()
products  = dim_products.copy()
brands    = dim_brands.copy()
categories= dim_categories.copy()
stores    = dim_stores.copy()
staffs    = dim_staffs.copy()
sales     = fact_sales.copy()

# เตรียมข้อมูลหลัก
sales = compute_net_sales(sales)
sales = add_period_cols(sales)

# Join dims ที่จำเป็น
products_lite = products[['product_id','product_name','category_id','brand_id']]
sales = sales.merge(products_lite, on='product_id', how='left')
sales = sales.merge(categories[['category_id','category_name']], on='category_id', how='left')
sales = sales.merge(brands[['brand_id','brand_name']], on='brand_id', how='left')
sales = sales.merge(stores[['store_id','store_name']], on='store_id', how='left')
sales = sales.merge(customers[['customer_id','customer_city','customer_state']], on='customer_id', how='left')

# วันที่ min-max สำหรับฟิลเตอร์
min_date = pd.to_datetime(sales['order_date']).min()
max_date = pd.to_datetime(sales['order_date']).max()

# ---- Controls ----
period = st.sidebar.selectbox("หน่วยเวลา (สำหรับกราฟแนวโน้ม)", ["month","quarter","year"], index=0)

f_date = st.sidebar.date_input(
    "ช่วงวันสั่งซื้อ",
    value=(min_date.date(), max_date.date()),
    min_value=min_date.date(),
    max_value=max_date.date()
)

col_a, col_b = st.sidebar.columns(2)
with col_a:
    f_store = st.multiselect("สาขา", options=sorted(stores['store_name'].unique()))
with col_b:
    f_brand = st.multiselect("แบรนด์", options=sorted(brands['brand_name'].unique()))

f_category = st.sidebar.multiselect("หมวดหมู่สินค้า", options=sorted(categories['category_name'].unique()))

if st.sidebar.button("รีเซ็ตตัวกรอง"):
    st.experimental_rerun()

# Apply Filters
mask = (
    (sales['order_date'].dt.date >= f_date[0]) &
    (sales['order_date'].dt.date <= f_date[1])
)
if f_store:
    mask &= sales['store_name'].isin(f_store)
if f_brand:
    mask &= sales['brand_name'].isin(f_brand)
if f_category:
    mask &= sales['category_name'].isin(f_category)

f = sales.loc[mask].copy()

# -----------------------------
# 🧭 Header
# -----------------------------
st.title("🚲 Bikestore Business Dashboard")
st.caption(f"ช่วงวันที่ {f_date[0].strftime('%d %b %Y')} – {f_date[1].strftime('%d %b %Y')}")

# -----------------------------
# 📊 KPI Cards
# -----------------------------
# KPI หลัก
total_sales   = f['net_sales'].sum()
orders        = f['order_id'].nunique()
customers_cnt = f['customer_id'].nunique()
AOV           = total_sales / orders if orders else 0

# Growth เทียบกับงวดก่อน (ตาม period)
trend_df = (
    f.groupby(period)
     .agg(net_sales=('net_sales','sum'), orders=('order_id','nunique'))
     .reset_index()
     .sort_values(by=period)
)

sales_growth = growth_rate(trend_df['net_sales']) if len(trend_df) >= 2 else np.nan
orders_growth = growth_rate(trend_df['orders']) if len(trend_df) >= 2 else np.nan

c1,c2,c3,c4 = st.columns(4)
with c1:
    st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
    st.metric("ยอดขายรวม", baht(total_sales), ("+" if (sales_growth or 0) > 0 else "") + (pct(sales_growth) if pd.notna(sales_growth) else ""))
    st.markdown("</div>", unsafe_allow_html=True)
with c2:
    st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
    st.metric("จำนวนออเดอร์", f"{orders:,}", ("+" if (orders_growth or 0) > 0 else "") + (pct(orders_growth) if pd.notna(orders_growth) else ""))
    st.markdown("</div>", unsafe_allow_html=True)
with c3:
    st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
    st.metric("ลูกค้ายูนีค", f"{customers_cnt:,}")
    st.markdown("</div>", unsafe_allow_html=True)
with c4:
    st.markdown("<div class='metric-card'>", unsafe_allow_html=True)
    st.metric("ค่าเฉลี่ยต่อออเดอร์ (AOV)", baht(AOV))
    st.markdown("</div>", unsafe_allow_html=True)

# -----------------------------
# 📈 แนวโน้มยอดขาย & จำนวนออเดอร์
# -----------------------------
st.markdown("### 1) แนวโน้มยอดขายและออเดอร์ตามช่วงเวลา")
fig_trend = px.line(
    trend_df, x=period, y=['net_sales','orders'],
    markers=True,
    title=f"แนวโน้มยอดขาย (฿) และจำนวนออเดอร์ (by {period})"
)
fig_trend.update_layout(template="plotly_white", legend_title_text="ตัวชี้วัด")
st.plotly_chart(fig_trend, use_container_width=True)

# -----------------------------
# 🧱 สรุปยอดขายตามประเภทสินค้า & สินค้าขายดี
# -----------------------------
st.markdown("### 2) หมวดหมู่ & สินค้าขายดี")

colA, colB = st.columns([1.1, 1])

# 2.1 Category Sales
cat_sales = (
    f.groupby('category_name', as_index=False)['net_sales'].sum()
     .sort_values('net_sales', ascending=False)
)
fig_cat = px.bar(
    cat_sales.head(15), x='category_name', y='net_sales', text='net_sales',
    title="ยอดขายรวมแยกตามประเภทสินค้า"
)
fig_cat.update_traces(texttemplate='%{text:,.0f}', textposition='outside', cliponaxis=False)
fig_cat.update_layout(template="plotly_white", xaxis_title="ประเภทสินค้า", yaxis_title="ยอดขายรวม (฿)")

with colA:
    st.plotly_chart(fig_cat, use_container_width=True)

# 2.2 Top Products (Revenue & Qty)
prod_rev = (
    f.groupby(['product_id','product_name'], as_index=False)['net_sales'].sum()
     .sort_values('net_sales', ascending=False).head(10)
)
prod_qty = (
    f.groupby(['product_id','product_name'], as_index=False)['quantity'].sum()
     .sort_values('quantity', ascending=False).head(10)
)

with colB:
    tabs = st.tabs(["ตามรายได้", "ตามจำนวนชิ้น"])
    with tabs[0]:
        fig_rev = px.bar(prod_rev, x='product_name', y='net_sales', text='net_sales', title='Top 10 สินค้าขายดีตามรายได้')
        fig_rev.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig_rev.update_layout(template="plotly_white", xaxis_tickangle=-35, yaxis_title='รายได้สุทธิ (฿)', showlegend=False, margin=dict(t=60,b=80))
        st.plotly_chart(fig_rev, use_container_width=True)
    with tabs[1]:
        fig_qty = px.bar(prod_qty, x='product_name', y='quantity', text='quantity', title='Top 10 สินค้าขายดีตามจำนวนชิ้น')
        fig_qty.update_traces(texttemplate='%{text}', textposition='outside')
        fig_qty.update_layout(template="plotly_white", xaxis_tickangle=-35, yaxis_title='จำนวนชิ้น', showlegend=False, margin=dict(t=60,b=80))
        st.plotly_chart(fig_qty, use_container_width=True)

# -----------------------------
# 🧩 แบรนด์ × หมวดหมู่ (Treemap)
# -----------------------------
f2 = f.copy()
st.markdown("### 3) สัดส่วนยอดขายตามแบรนด์และหมวดหมู่สินค้า")
brand_cat = (
    f.groupby(['brand_name','category_name'], as_index=False)['net_sales'].sum()
)
fig_tree = px.treemap(brand_cat, path=['brand_name','category_name'], values='net_sales', title="Treemap: แบรนด์ × หมวดหมู่")
fig_tree.update_layout(margin=dict(t=50,l=0,r=0,b=0))
st.plotly_chart(fig_tree, use_container_width=True)

# -----------------------------
# 💸 ผลของส่วนลดต่อปริมาณ/รายได้
# -----------------------------
st.markdown("### 7) ผลของส่วนลดต่อปริมาณ/รายได้")
f2['discount_range'] = pd.cut(f2['discount'], bins=[-0.01, 0.1, 0.2, 1], labels=['0-10%','10-20%','>20%'])
disc = f2.groupby('discount_range', as_index=False).agg(total_qty=('quantity','sum'), total_sales=('net_sales','sum'))

tabD1, tabD2 = st.tabs(["ปริมาณ (ชิ้น)", "รายได้ (฿)"])
with tabD1:
    fig_dq = px.bar(disc, x='discount_range', y='total_qty', text='total_qty', title='ปริมาณที่ขายได้ตามช่วงส่วนลด')
    fig_dq.update_traces(textposition='outside')
    fig_dq.update_layout(template="plotly_white")
    st.plotly_chart(fig_dq, use_container_width=True)
with tabD2:
    fig_ds = px.bar(disc, x='discount_range', y='total_sales', text='total_sales', title='รายได้ตามช่วงส่วนลด')
    fig_ds.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig_ds.update_layout(template="plotly_white")
    st.plotly_chart(fig_ds, use_container_width=True)