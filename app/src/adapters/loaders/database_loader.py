"""Database loader adapter using SQLite."""

import sqlite3
import os
import polars as pl
from typing import List, Dict, Any
# from ...domain.entities import Record
from ...ports.loader import Loader
from ...utils.logger import get_logger

class DatabaseLoader(Loader):
    """Loader for SQLite database storage."""
    def __init__(self):
        self.user = os.getenv("DB_USER", "your_username")
        self.password = os.getenv("DB_PASSWORD", "your_password")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "3306")
        self.database = os.getenv("DB_NAME", "your_database_name")
        self.database_uri = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.logger = get_logger("Adapters.Loader.DatabaseLoader")
    def load(self, records: pl.DataFrame, config: Dict[str, Any]) -> bool:
        """
        Load records into a mysql database.
        
        Config options:
        - table_name: Name of the table to insert into
        - create_table: If True, create table if it doesn't exist (default: True)
        - mode: 'insert' or 'replace' (default: 'insert')
        """
        if records.is_empty():
            return True
        
        table_name = config.get('table_name', 'etl_data')
        create_table = config.get('create_table', True)
        mode = config.get('mode', 'insert')
        
        
        try:
            records.write_database(
                table_name=table_name,
                connection=self.database_uri,
                if_table_exists='append',
            )
            self.logger.info(f"Successfully wrote data to table '{table_name}'")
            return True
            
        except Exception as e:
            self.logger.exception(f"Error loading to database: {e}")
            return False
    
    def validate_connection(self, config: Dict[str, Any]) -> bool:
        try:
            return True
        except Exception:
            return False

