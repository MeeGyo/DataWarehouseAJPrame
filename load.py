import duckdb as dd
import polars as pl
from typing import Dict, List, Optional
import logging
from pathlib import Path
from src.config import Config

# Setup logging
logging.basicConfig(level=getattr(logging, Config.LOG_LEVEL))
logger = logging.getLogger(__name__)

class DataLoader:
    """Class for loading data into DuckDB data warehouse"""
    
    def __init__(self):
        self.config = Config()
        self.db_path = self.config.DATABASE_PATH
        self.connection = None

    def connect(self) -> dd.DuckDBPyConnection:
        """
        Create connection to DuckDB database
        
        Returns:
            DuckDB connection object
        """
        try:
            # Ensure database directory exists
            df_path = Path(self.db_path)
            if not df_path.parent.exists():
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            
            # Create connection
            self.connection =dd.connect(self.db_path)
            logger.info(f"Connected to DuckDB at {self.db_path}")
            return self.connection
            
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def create_schema(self):
        """Create database schema for data warehouse"""
        logger.info("Creating database schema")
        
        if not self.connection:
            self.connect()
        try:
            # Create schemas (like create the folders for the database)
            # self.connection.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
            # self.connection.execute("CREATE SCHEMA IF NOT EXISTS fact")
            
            # Create dimension tables
            self.create_dimension_tables()
            
            # Create fact tables
            self.create_fact_tables()
            
            logger.info("Database schema created successfully")
            
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise

    def create_dimension_tables(self):
        """Create dimension tables (BikeStores)"""

        # 1) Date dimension (ใช้ DATE เป็น key ให้ join กับ fact ได้ตรงๆ)
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_date (
                date_key DATE PRIMARY KEY,
                date DATE,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                month_name VARCHAR,
                day INTEGER,
                day_of_week INTEGER,      -- Monday=0 ... Sunday=6
                day_name VARCHAR,
                week_of_year INTEGER,
                is_weekend BOOLEAN,
                fiscal_quarter INTEGER
            )
        """)

        # 2) Customers
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_customers (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                full_name VARCHAR,
                email VARCHAR,
                customer_phone VARCHAR,
                street VARCHAR,
                city VARCHAR,
                state VARCHAR,
                postal_code VARCHAR
            )
        """)

        # 3) Brands
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_brands (
                brand_id INTEGER PRIMARY KEY,
                brand_name VARCHAR
            )
        """)

        # 4) Categories
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_categories (
                category_id INTEGER PRIMARY KEY,
                category_name VARCHAR
            )
        """)

        # 5) Products
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_products (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR,
                brand_id INTEGER,
                category_id INTEGER,
                model_year INTEGER,
                list_price DECIMAL(10,2)
            )
        """)

        # 6) Stores
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_stores (
                store_id INTEGER PRIMARY KEY,
                store_name VARCHAR,
                phone VARCHAR,
                email VARCHAR,
                street VARCHAR,
                city VARCHAR,
                state VARCHAR,
                postal_code VARCHAR
            )
        """)

        # 7) Staffs แก้ใน transform
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_staffs (
                staff_id INTEGER PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                full_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                active BOOLEAN,
                store_id INTEGER,
                manager_id INTEGER
            )
        """)

        # 8) Order Status (static mapping)
        self.connection.execute("""
            CREATE OR REPLACE TABLE dim_order_status (
                order_status_id INTEGER PRIMARY KEY,
                order_status_name VARCHAR
            )
        """)

    def create_fact_tables(self):
        """Create fact tables (BikeStores)"""

        # Fact Sales (grain = order line)
        self.connection.execute("""
            CREATE OR REPLACE TABLE fact_sales (
                order_id INTEGER,
                item_id INTEGER,
                customer_id INTEGER,
                store_id INTEGER,
                staff_id INTEGER,
                product_id INTEGER,
                order_status_id INTEGER,

                order_date_key DATE,
                required_date_key DATE,
                shipped_date_key DATE,

                quantity INTEGER,
                list_price DECIMAL(10,2),
                discount DECIMAL(5,4),          -- fraction 0..1
                gross_amount DECIMAL(18,2),
                discount_amount DECIMAL(18,2),
                net_amount DECIMAL(18,2),

                order_to_ship_days INTEGER,
                shipped_on_time BOOLEAN,

                discount_pct DECIMAL(5,2),      -- 0..100
                discount_bucket VARCHAR,

                created_at TIMESTAMP,

                PRIMARY KEY (order_id, item_id)
            )
        """)

        # Fact Inventory (current stock per store-product)
        self.connection.execute("""
            CREATE OR REPLACE TABLE fact_inventory (
                store_id INTEGER,
                product_id INTEGER,
                quantity_on_hand INTEGER,
                PRIMARY KEY (store_id, product_id)
            )
        """)

    def load_dataframe(self, df: pl.DataFrame, table_name: str) -> bool:
        """
        Load Polars DataFrame into DuckDB table (replace mode)
        """
        try:
            if not self.connection:
                self.connect()

            arrow_table = df.to_arrow()
            self.connection.register("temp_table", arrow_table)

            # Insert data into target table
            full_table_name = f"{table_name}"
            # หมายเหตุ: คำสั่งนี้จะ "แทนที่" ตารางเดิมด้วย schema ของ df
            # หากต้องการบังคับ schema ให้ตรงตาม DDL ให้ใช้ INSERT INTO ... SELECT ... และคอลัมน์ให้ครบถ้วน
            self.connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_table")

            self.connection.unregister("temp_table")
            logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {str(e)}")
            return False

    def load_all_data(self, transformed_data: Dict[str, pl.DataFrame]) -> bool:
        """
        Load all transformed data into the data warehouse
        Expected keys:
        - dim_date, dim_customers, dim_brands, dim_categories, dim_products,
            dim_stores, dim_staffs, dim_order_status,
            fact_sales, fact_inventory
        """
        logger.info("Starting data loading process")
        if not self.connection:
            self.connect()

        # Create schema first
        self.create_schema()

        # Optional: inspect existing tables
        tables_in_schema = self.connection.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
        if tables_in_schema is not None:
            tables_in_schema.show()
        else:
            logger.warning("tables_in_schema is None")
        
        success_count = 0
        total_tables = len(transformed_data)

        # Load dimensions first (กำหนดลำดับเพื่อความชัดเจน)
        dim_order = [
            "dim_date",
            "dim_customers",
            "dim_brands",
            "dim_categories",
            "dim_products",
            "dim_stores",
            "dim_staffs",
            "dim_order_status",
        ]
        for name in dim_order:
            if name in transformed_data:
                if self.load_dataframe(transformed_data[name], name):
                    success_count += 1

        # Load facts
        fact_order = ["fact_sales", "fact_inventory"]
        for name in fact_order:
            if name in transformed_data:
                if self.load_dataframe(transformed_data[name], name):
                    success_count += 1

        # Load any remaining tables (ถ้ามี key อื่นๆ)
        for name, df in transformed_data.items():
            if name.startswith(("dim_", "fact_")) and name not in dim_order + fact_order:
                if self.load_dataframe(df, name):
                    success_count += 1

        logger.info(f"Data loading complete: {success_count}/{total_tables} tables loaded successfully")
        return success_count == total_tables