"""CSV file extractor adapter."""

import csv
import os
import polars as pl
from ...ports.extractor import Extractor


class CsvExtractor(Extractor):
    """Extractor for CSV files."""
    
    def extract(self, source_path: str) -> pl.LazyFrame:
        """
        Extract data from a CSV file.
        
        First row is treated as headers.
        """
        if not self.validate_source(source_path):
            raise ValueError(f"Invalid CSV source: {source_path}")
        
        lazy_df = pl.scan_csv(source_path)
        return lazy_df
    
    def validate_source(self, source_path: str) -> bool:
        """Validate CSV file exists and is readable."""
        print(os.getcwd())
        if not os.path.exists(source_path):
            return False
        
        if not source_path.lower().endswith('.csv'):
            return False
        return True
        # try:
        #     with open(source_path, 'r', encoding='utf-8') as file:
        #         # Try to read first line to validate CSV format
        #         csv.Sniffer().sniff(file.read(1024))
        #         file.seek(0)
        #     return True
        # except (csv.Error, IOError):
        #     return False

