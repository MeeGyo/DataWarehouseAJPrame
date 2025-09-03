"""
Data transformation module for creating dimensional model
"""


import polars as pl
from typing import Dict, List, Optional
import logging
from datetime import datetime
from src.config import Config




# Setup logging
logging.basicConfig(level=getattr(logging, Config.LOG_LEVEL),
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self):
        self.config = Config()


    def standardize_column_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Standardize column names by converting to lowercase and replacing spaces and hyphens with underscores.
       
        Args:
            df: Input DataFrame
        Returns:
            DataFrame with standardized column names    
        """
        new_columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        return df.rename(dict(zip(df.columns, new_columns)))
   
    def transform_brands(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform brands data into dimension table"""
        logger.info("=== Transforming brands dimension ===")
        df_clean = self.standardize_column_names(df)
        dim_brands = df_clean.select(
            pl.col("brand_id"),
            pl.col("brand_name"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        ).sort("brand_id").filter(pl.col("brand_id").is_not_null())
        return dim_brands


    def transform_categories(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform categories data into dimension table"""
        logger.info("=== Transforming categories dimension ===")
        df_clean = self.standardize_column_names(df)
        dim_categories = df_clean.select(
            pl.col("category_id"),
            pl.col("category_name"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        ).sort("category_id").filter(pl.col("category_id").is_not_null())
        return dim_categories


    def transform_stores(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform stores data into dimension table"""
        logger.info("=== Transforming stores dimension ===")
        df_clean = self.standardize_column_names(df)
        dim_stores = df_clean.select(
            pl.col("store_id"),
            pl.col("store_name"),
            pl.col("phone").alias("store_phone"),
            pl.col("email").alias("store_email"),
            pl.col("street").alias("store_street"),
            pl.col("city").alias("store_city"),
            pl.col("state").alias("store_state"),
            pl.col("zip_code").alias("store_zip_code"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        ).sort("store_id").filter(pl.col("store_id").is_not_null())
        return dim_stores


    def transform_staffs(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform staffs data into dimension table"""
        logger.info("=== Transforming staffs dimension ===")
        df_clean = self.standardize_column_names(df)
        dim_staffs = df_clean.select(
            pl.col("staff_id"),
            pl.col("first_name").alias("staff_firstname"),
            pl.col("last_name").alias("staff_lastname"),
            pl.col("email").alias("staff_email"),
            pl.col("phone").alias("staff_phone"),
            pl.col("active").alias("staff_active"),
            pl.col("store_id"),
            pl.col("manager_id"),
            pl.concat_str(
                pl.col("first_name"),
                pl.col("last_name"),
                separator=" "
            ).alias("staff_fullname"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        ).sort("staff_id").filter(pl.col("staff_id").is_not_null())
        return dim_staffs


    def transform_customers(self,df: pl.DataFrame) -> pl.DataFrame:
        """Transform customers data into dimension table"""
        logger.info("=== Transforming customers dimension ===")
        df_clean = self.standardize_column_names(df)
        dim_customers = df_clean.select(
            pl.col("customer_id"),
            pl.col("first_name").alias("customer_firstname"),
            pl.col("last_name").alias("customer_lastname"),
            pl.col("phone").alias("customer_phone"),
            pl.col("email").alias("customer_email"),
            pl.col("street").alias("customer_street"),
            pl.col("city").alias("customer_city"),
            pl.col("state").alias("customer_state"),
            pl.col("zip_code").alias("customer_zipcode"),
            pl.concat_str(
                pl.col("first_name"),
                pl.col("last_name"),
                separator=" "
            ).alias("customer_fullname"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        ).sort("customer_id").filter(pl.col("customer_id").is_not_null())
        return dim_customers


    def transform_products(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform products data into dimension table"""
        logger.info("Transforming products dimension")
        df_clean = self.standardize_column_names(df)
        dim_product = (df_clean.select(
            pl.col("product_id"),
            pl.col("product_name"),
            pl.col("brand_id"),
            pl.col("category_id"),
            pl.col("model_year"),
            pl.col("list_price"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        )
        .sort(pl.col("product_id"))
        .filter(pl.col("product_id").is_not_null()))
        return dim_product
   
    def get_fiscal_quarter(self, start_month: int) -> pl.Expr:
        """
        Returns a Polars expression to calculate the fiscal quarter.
        """
        return (
            (pl.col("date").dt.month() - start_month + 12) % 12 // 3
        ) + 1


    def create_date_dimension(self) -> pl.DataFrame:
        """
        Create a date dimension table
        """
        date_range = pl.date_range(
            start=pl.datetime(2015, 1, 1),
            end=pl.datetime(2025, 12, 31),
            interval="1d",
            eager=True
        )


        dim_date = pl.DataFrame({
            "date_key": date_range,
            "date": date_range,
            "year": date_range.dt.year(),
            "quarter": date_range.dt.quarter(),
            "month": date_range.dt.month(),
            "month_name": date_range.dt.strftime("%B"),
            "day": date_range.dt.day(),
            "day_of_week": date_range.dt.weekday(),
            "day_name": date_range.dt.strftime("%A"),
            "week_of_year": date_range.dt.week(),
            "is_weekend": date_range.dt.weekday().is_in([6, 7])
        })


        dim_date = dim_date.with_columns(
            self.get_fiscal_quarter(10).alias("fiscal_quarter")
        )
        return dim_date


    def transform_sales_fact(self, orders_df: pl.DataFrame, order_items_df: pl.DataFrame) -> pl.DataFrame:
        """Transform orders and order items into sales fact table"""
        logger.info("===Transforming sales fact table===")


        # Clean the data
        df_orders = self.standardize_column_names(orders_df)
        df_order_items = self.standardize_column_names(order_items_df)


        # Join orders with order items
        df_order_join = df_orders.join(
            df_order_items,
            left_on="order_id",
            right_on="order_id",
            how="inner"
        )
       
        # Select columns and calculate metrics
        sales_fact = df_order_join.select([
            pl.col("order_id"),
            pl.col("customer_id"),
            pl.col("store_id"),
            pl.col("staff_id"),
            pl.col("product_id"),
            pl.col("order_date"),
            pl.col("shipped_date"),
            pl.col("quantity"),
            pl.col("list_price"),
            pl.col("discount"),
            (pl.col("quantity") * pl.col("list_price")).alias("gross_amount"),
            (pl.col("quantity") * pl.col("list_price") * (1 - pl.col("discount"))).alias("net_amount"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(datetime.now()).alias("updated_at")
        ])
       
        return sales_fact
   
    def transform_all_data(self, raw_data: Dict[str, pl.DataFrame]) -> Dict[str, pl.DataFrame]:
        """
        Transform all raw data into dimensional model
        """
        logger.info("Starting data transformation process")
        transformed = {}
       
        # Create dimensions
        if "customers" in raw_data:
            transformed["dim_customers"] = self.transform_customers(raw_data["customers"])

        if "products" in raw_data:
            transformed["dim_products"] = self.transform_products(raw_data["products"])
       
        # เพิ่มการเรียกใช้เมธอดสำหรับ brands, categories, stores, และ staffs
        if "brands" in raw_data:
            transformed["dim_brands"] = self.transform_brands(raw_data["brands"])

        if "categories" in raw_data:
            transformed["dim_categories"] = self.transform_categories(raw_data["categories"])

        if "stores" in raw_data:
            transformed["dim_stores"] = self.transform_stores(raw_data["stores"])
       
        if "staffs" in raw_data:
            transformed["dim_staffs"] = self.transform_staffs(raw_data["staffs"])


        # Create date dimension
        transformed["dim_date"] = self.create_date_dimension()


        # Create fact tables
        if "orders" in raw_data and "order_items" in raw_data:
            transformed["fact_sales"] = self.transform_sales_fact(
                raw_data["orders"],
                raw_data["order_items"]
            )


        logger.info(f"Transformation complete. Created {len(transformed)} tables")
        return transformed
