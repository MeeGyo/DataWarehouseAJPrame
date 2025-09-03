import streamlit as st
import duckdb as dd
import polars as pl

conn = dd.connect(r'C:\Users\Lenovo\Desktop\bikestore\data_warehouse\bikestore.duckdb')

def execute_query(conn, query):
        result = conn.execute(query).fetchdf()
        return pl.from_pandas(result)

st.set_page_config(
    page_title="Dashboard Overview",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Dashboard Overview")
st.write("เลือกดูรายละเอียดของ Dashboard แต่ละหน้าได้จากเมนูด้านซ้าย หรือกดลิงก์ด้านล่าง")
    
# ...existing code...
dim_customers = execute_query(conn, "SELECT * FROM dim_customers")
dim_date = execute_query(conn, "SELECT * FROM dim_date")
dim_staffs = execute_query(conn, "SELECT * FROM dim_staffs")
dim_products = execute_query(conn, "SELECT * FROM dim_products")
dim_brands = execute_query(conn, "SELECT * FROM dim_brands")
dim_categories = execute_query(conn, "SELECT * FROM dim_categories")
dim_stores = execute_query(conn, "SELECT * FROM dim_stores")
fact_sales = execute_query(conn, "SELECT * FROM fact_sales")
conn.close()

st.write("### dim_customers")
st.write(dim_customers.head(5))
st.write("### dim_date")
st.write(dim_date.head(5))
st.write("### dim_staffs")
st.write(dim_staffs.head(5))
st.write("### dim_products")
st.write(dim_products.head(5))
st.write("### dim_brands")
st.write(dim_brands.head(5))
st.write("### dim_categories")
st.write(dim_categories.head(5))
st.write("### dim_stores")
st.write(dim_stores.head(5))
st.write("### fact_sales")
st.write(fact_sales.head(5))
# ...existing code...