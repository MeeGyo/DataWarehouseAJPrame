import streamlit as st
import duckdb as dd
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
import plotly.graph_objects as go
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
# 🗺️ ลูกค้าอยู่รัฐไหนมากที่สุด + Repeat Rate
# -----------------------------
st.markdown("### 8) ภูมิศาสตร์ลูกค้า & ลูกค้าซื้อซ้ำ")

# นับลูกค้าต่อรัฐ
top_states = customers.groupby('customer_state').size().reset_index(name='count')

# แปลงชื่อรัฐ -> รหัส 2 ตัวอักษร (ถ้าเป็นตัวย่ออยู่แล้วจะคงเดิม)
us_state_abbrev = {
    'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA','Colorado':'CO',
    'Connecticut':'CT','Delaware':'DE','District of Columbia':'DC','Florida':'FL','Georgia':'GA',
    'Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA','Kansas':'KS','Kentucky':'KY',
    'Louisiana':'LA','Maine':'ME','Maryland':'MD','Massachusetts':'MA','Michigan':'MI','Minnesota':'MN',
    'Mississippi':'MS','Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV','New Hampshire':'NH',
    'New Jersey':'NJ','New Mexico':'NM','New York':'NY','North Carolina':'NC','North Dakota':'ND',
    'Ohio':'OH','Oklahoma':'OK','Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI','South Carolina':'SC',
    'South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT','Vermont':'VT','Virginia':'VA',
    'Washington':'WA','West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY','Puerto Rico':'PR'
}
ts = top_states.copy()
ts['customer_state'] = ts['customer_state'].astype(str).str.strip()
ts['state_code'] = ts['customer_state'].map(us_state_abbrev).fillna(ts['customer_state'].str.upper())

# ใช้เฉพาะค่าที่เป็นรหัสรัฐ 2 ตัวอักษร
ts_valid = ts[ts['state_code'].str.len() == 2].copy()

if ts_valid.empty:
    st.info("ไม่พบรหัสรัฐที่รองรับการทำแผนที่ (USA-states). โปรดตรวจสอบค่า customer_state")
else:
    fig_map = px.choropleth(
        ts_valid,
        locations='state_code',
        locationmode='USA-states',
        color='count',
        color_continuous_scale='Blues',
        scope='usa',
        labels={'count':'จำนวนลูกค้า'}
    )

    # ตั้งค่าป้ายชื่อบนแผนที่
    with st.expander("ตั้งค่าป้ายชื่อบนแผนที่"):
        show_labels = st.checkbox("แสดงป้ายชื่อบนแผนที่", value=True)
        label_mode = st.selectbox(
            "รูปแบบป้ายชื่อ",
            options=["ตัวย่อรัฐ", "ชื่อรัฐ", "ชื่อรัฐ + จำนวนลูกค้า"],
            index=2
        )
        label_font_size = st.slider("ขนาดตัวอักษร", min_value=8, max_value=20, value=11)

    if show_labels:
        # Centroid ของรัฐ (ตำแหน่งโดยประมาณสำหรับวางป้าย)
        STATE_CENTROIDS = {
            'AL': (32.806671, -86.791130), 'AK': (61.370716, -152.404419), 'AZ': (33.729759, -111.431221),
            'AR': (34.969704, -92.373123), 'CA': (36.116203, -119.681564), 'CO': (39.059811, -105.311104),
            'CT': (41.597782, -72.755371), 'DE': (39.318523, -75.507141), 'DC': (38.9072, -77.0369),
            'FL': (27.766279, -81.686783), 'GA': (33.040619, -83.643074), 'HI': (21.094318, -157.498337),
            'ID': (44.240459, -114.478828), 'IL': (40.349457, -88.986137), 'IN': (39.849426, -86.258278),
            'IA': (42.011539, -93.210526), 'KS': (38.5266, -96.726486), 'KY': (37.66814, -84.670067),
            'LA': (31.169546, -91.867805), 'ME': (44.693947, -69.381927), 'MD': (39.063946, -76.802101),
            'MA': (42.230171, -71.530106), 'MI': (43.326618, -84.536095), 'MN': (45.694454, -93.900192),
            'MS': (32.741646, -89.678696), 'MO': (38.456085, -92.288368), 'MT': (46.921925, -110.454353),
            'NE': (41.12537, -98.268082), 'NV': (38.313515, -117.055374), 'NH': (43.452492, -71.563896),
            'NJ': (40.298904, -74.521011), 'NM': (34.840515, -106.248482), 'NY': (42.165726, -74.948051),
            'NC': (35.630066, -79.806419), 'ND': (47.528912, -99.784012), 'OH': (40.388783, -82.764915),
            'OK': (35.565342, -96.928917), 'OR': (44.572021, -122.070938), 'PA': (40.590752, -77.209755),
            'RI': (41.680893, -71.51178), 'SC': (33.856892, -80.945007), 'SD': (44.299782, -99.438828),
            'TN': (35.747845, -86.692345), 'TX': (31.054487, -97.563461), 'UT': (40.150032, -111.862434),
            'VT': (44.045876, -72.710686), 'VA': (37.769337, -78.169968), 'WA': (47.400902, -121.490494),
            'WV': (38.491226, -80.954453), 'WI': (44.268543, -89.616508), 'WY': (42.755966, -107.30249),
            'PR': (18.220833, -66.590149)
        }

        # เตรียมข้อมูลป้ายชื่อ
        label_rows = []
        for _, r in ts_valid.iterrows():
            code = r['state_code']
            if code in STATE_CENTROIDS:
                lat, lon = STATE_CENTROIDS[code]
                if label_mode == "ตัวย่อรัฐ":
                    text = code
                elif label_mode == "ชื่อรัฐ":
                    text = r['customer_state']
                else:
                    text = f"{r['customer_state']} ({int(r['count']):,})"
                label_rows.append({"state_code": code, "lat": lat, "lon": lon, "text": text})

        if label_rows:
            labels_df = pd.DataFrame(label_rows)
            fig_map.add_trace(
                go.Scattergeo(
                    lon=labels_df['lon'],
                    lat=labels_df['lat'],
                    text=labels_df['text'],
                    mode="text",
                    textfont=dict(size=label_font_size, color="ORANGE"),
                    hoverinfo="skip",
                    showlegend=False
                )
            )

    # ปรับ layout
    fig_map.update_layout(
        geo=dict(scope='usa', projection_type='albers usa', showlakes=True, lakecolor="rgb(255,255,255)"),
        margin=dict(l=0, r=0, t=0, b=0)
    )
    st.plotly_chart(fig_map, use_container_width=True)


with st.expander("ตารางสรุปตามรัฐ"):
    st.dataframe(ts[['customer_state','count']].sort_values('count', ascending=False),
                 use_container_width=True)
    

# -----------------------------
st.markdown("### 8) ภูมิศาสตร์ลูกค้า & ลูกค้าซื้อซ้ำ")
colG1, colG2 = st.columns(2)
# สรุปจำนวนออเดอร์ต่อ customer_id
cust_orders = (
    f.groupby('customer_id', as_index=False)['order_id']
     .nunique()
     .rename(columns={'order_id':'order_count'})
).merge(
    customers[['customer_id','customer_city','customer_state']],
    on='customer_id', how='left'
)
cust_orders['is_repeat'] = cust_orders['order_count'] > 1

# สรุประดับเมือง/รัฐ
repeat_city = cust_orders.groupby('customer_city', as_index=False) \
                         .agg(repeat_rate=('is_repeat','mean'),
                              customers=('customer_id','nunique'))
repeat_state = cust_orders.groupby('customer_state', as_index=False) \
                          .agg(repeat_rate=('is_repeat','mean'),
                               customers=('customer_id','nunique'))

# ตั้งค่าควบคุมกรองขั้นต่ำลูกค้าและจำนวนอันดับที่จะแสดง
max_city = int(repeat_city['customers'].max()) if not repeat_city.empty else 1
max_state = int(repeat_state['customers'].max()) if not repeat_state.empty else 1
max_cust = max(1, max_city, max_state)

min_c = st.slider("ขั้นต่ำจำนวนลูกค้าต่อเมือง/รัฐ", min_value=1, max_value=max_cust, value=min(10, max_cust))
top_n = st.slider("จำนวนอันดับสูงสุดที่แสดง", min_value=5, max_value=50, value=15, step=5)

tabs_geo = st.tabs(["ตามเมือง (กราฟ)", "ตามรัฐ (กราฟ)", "ตาราง"])

# กราฟเมือง
with tabs_geo[0]:
    dfc = repeat_city[repeat_city['customers'] >= min_c] \
          .sort_values('repeat_rate', ascending=False) \
          .head(top_n)

    if dfc.empty:
        st.info("ไม่มีเมืองที่ผ่านเกณฑ์ขั้นต่ำจำนวนลูกค้า")
    else:
        fig_bar_city = px.bar(
            dfc, x='repeat_rate', y='customer_city',
            orientation='h',
            color='repeat_rate',
            color_continuous_scale='Tealrose',
            labels={'repeat_rate':'Repeat Rate', 'customer_city':'เมือง'},
            hover_data={'customers': True, 'repeat_rate': ':.2%'},
            text=dfc['repeat_rate'].map(lambda x: f"{x:.0%}")
        )
        fig_bar_city.update_layout(
            xaxis_tickformat=".0%",
            margin=dict(l=0, r=0, t=30, b=0)
        )
        fig_bar_city.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_bar_city, use_container_width=True)

        fig_sc_city = px.scatter(
            dfc, x='customers', y='repeat_rate', size='customers',
            color='repeat_rate', color_continuous_scale='Viridis',
            hover_name='customer_city',
            labels={'customers':'จำนวนลูกค้า', 'repeat_rate':'Repeat Rate'}
        )
        fig_sc_city.update_layout(yaxis_tickformat=".0%", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_sc_city, use_container_width=True)

# กราฟรัฐ
with tabs_geo[1]:
    dfs = repeat_state[repeat_state['customers'] >= min_c] \
          .sort_values('repeat_rate', ascending=False) \
          .head(top_n)

    if dfs.empty:
        st.info("ไม่มีรัฐที่ผ่านเกณฑ์ขั้นต่ำจำนวนลูกค้า")
    else:
        fig_bar_state = px.bar(
            dfs, x='repeat_rate', y='customer_state',
            orientation='h',
            color='repeat_rate',
            color_continuous_scale='Tealrose',
            labels={'repeat_rate':'Repeat Rate', 'customer_state':'รัฐ'},
            hover_data={'customers': True, 'repeat_rate': ':.2%'},
            text=dfs['repeat_rate'].map(lambda x: f"{x:.0%}")
        )
        fig_bar_state.update_layout(
            xaxis_tickformat=".0%",
            margin=dict(l=0, r=0, t=30, b=0)
        )
        fig_bar_state.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_bar_state, use_container_width=True)

        fig_sc_state = px.scatter(
            dfs, x='customers', y='repeat_rate', size='customers',
            color='repeat_rate', color_continuous_scale='Viridis',
            hover_name='customer_state',
            labels={'customers':'จำนวนลูกค้า', 'repeat_rate':'Repeat Rate'}
        )
        fig_sc_state.update_layout(yaxis_tickformat=".0%", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_sc_state, use_container_width=True)

# ตาราง
with tabs_geo[2]:
    st.write("ตามเมือง")
    st.dataframe(repeat_city.sort_values('repeat_rate', ascending=False), use_container_width=True)
    st.write("ตามรัฐ")
    st.dataframe(repeat_state.sort_values('repeat_rate', ascending=False), use_container_width=True)