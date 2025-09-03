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


# 🏬 ประสิทธิภาพสาขา & ส่วนแบ่งสาขา
# -----------------------------
st.markdown("### 4) ประสิทธิภาพของสาขา")
colS1, colS2 = st.columns([1.1, 1])

store_perf = (
    f.groupby('store_name', as_index=False)
     .agg(net_sales=('net_sales','sum'), orders=('order_id','nunique'))
     .sort_values('net_sales', ascending=False)
)

with colS1:
    fig_store_bar = px.bar(store_perf, x='store_name', y='net_sales', text='net_sales', title="ยอดขายสุทธิต่อสาขา")
    fig_store_bar.update_traces(texttemplate='%{text:,.0f}', textposition='outside', cliponaxis=False)
    fig_store_bar.update_layout(template="plotly_white", xaxis_title="สาขา", yaxis_title="ยอดขาย (฿)")
    st.plotly_chart(fig_store_bar, use_container_width=True)

with colS2:
    fig_store_pie = px.pie(store_perf, names='store_name', values='net_sales', title="สัดส่วนยอดขายแยกตามสาขา", hole=0.35)
    fig_store_pie.update_traces(textinfo='percent+label')
    fig_store_pie.update_layout(template="plotly_white")
    st.plotly_chart(fig_store_pie, use_container_width=True)

# -----------------------------
# 👤 ประสิทธิภาพพนักงานขาย
# -----------------------------
st.markdown("### 5) ประสิทธิภาพพนักงานขาย")
staff_perf = (
    f.merge(staffs[['staff_id','staff_fullname']], on='staff_id', how='left')
     .groupby('staff_fullname', as_index=False)
     .agg(net_sales=('net_sales','sum'), orders=('order_id','nunique'))
     .sort_values('net_sales', ascending=False)
)
fig_staff = px.bar(staff_perf, x='staff_fullname', y='net_sales', text='net_sales', title="ยอดขายสุทธิต่อพนักงาน")
fig_staff.update_traces(texttemplate='%{text:,.0f}', textposition='outside', cliponaxis=False)
fig_staff.update_layout(template="plotly_white", xaxis_tickangle=-20)
st.plotly_chart(fig_staff, use_container_width=True)

# -----------------------------
# 🚚 ความตรงเวลาในการส่ง (Order-to-Ship)
# -----------------------------
st.markdown("### 6) ความตรงเวลาในการส่ง (Order-to-Ship)")
f2 = f.copy()
f2['shipped_date'] = pd.to_datetime(f2['shipped_date'])
f2['order_to_ship_days'] = (f2['shipped_date'] - f2['order_date']).dt.days
# on_time: สามารถปรับ logic ได้ตาม SLA
f2['on_time'] = f2['order_to_ship_days'] <= 0
ship_perf = f2.groupby('store_name', as_index=False).agg(
    avg_days=('order_to_ship_days','mean'),
    on_time_rate=('on_time','mean')
)
colT1, colT2 = st.columns(2)
with colT1:
    st.dataframe(ship_perf, use_container_width=True)
with colT2:
    fig_ship = px.scatter(ship_perf, x='avg_days', y='on_time_rate', text='store_name', trendline='ols', title='เฉลี่ยวันจัดส่ง vs อัตราส่งตรงเวลา')
    fig_ship.update_traces(textposition='top center')
    fig_ship.update_layout(template="plotly_white", xaxis_title='เฉลี่ยวันจัดส่ง (วัน)', yaxis_title='อัตราส่งตรงเวลา')
    st.plotly_chart(fig_ship, use_container_width=True)