"""
Configuration management for the Data Warehouse project
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the application"""

    # Base paths
    DATA_DIR = os.getenv("RAW_DATA_DIR", "data")
    PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR", "processed")
    DATABASE_DIR = os.getenv("DATABASE_DIR", "data_warehouse")

    # Database configuration
    DATABASE_PATH = os.getenv("DATABASE_PATH", "data_warehouse/bikestore.duckdb")
    DATABASE_NAME = os.getenv("DATABASE_NAME", "bikestore.duckdb")

    # ETL configuration
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Date formats
    DATE_FORMAT = os.getenv("DATE_FORMAT", "%Y-%m-%d")
    DATETIME_FORMAT = os.getenv("DATETIME_FORMAT", "%Y-%m-%d %H:%M:%S")

    # CSV files mapping
    CSV_FILES = {
        "brands": "brands.csv",
        "categories": "categories.csv",
        "customers": "customers.csv",
        "order_items": "order_items.csv",
        "orders": "orders.csv",
        "products": "products.csv",
        "staffs": "staffs.csv",
        "stocks": "stocks.csv",
        "stores": "stores.csv"
    }

    @classmethod
    def get_csv_path(cls, table_name: str) -> str:
        """Get the full path to a CSV file"""
        if table_name not in cls.CSV_FILES:
            raise ValueError(f"Unknown table: {table_name}")
        return os.path.join(cls.DATA_DIR, cls.CSV_FILES[table_name])

    @classmethod
    def get_database_path(cls) -> str:
        """Get the full path to the database file"""