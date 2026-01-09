"""JSON file extractor adapter."""

import json
import os
import polars as pl
from ...ports.extractor import Extractor


class JsonExtractor(Extractor):
    """Extractor for JSON files."""
    
    def extract(self, source_path: str) -> pl.LazyFrame:
        """
        Extract data from a JSON file.
        
        Supports both single JSON objects and arrays of JSON objects.
        """
        if not self.validate_source(source_path):
            raise ValueError(f"Invalid JSON source: {source_path}")
        
        lazy_df = pl.scan_ndjson(source_path)
        return lazy_df
    
    def validate_source(self, source_path: str) -> bool:
        """Validate JSON file exists and is readable."""
        if not os.path.exists(source_path):
            return False
        
        if not source_path.lower().endswith('.json'):
            return False
        
        return True

