"""Port for data extraction."""

import polars as pl
from abc import ABC, abstractmethod
from typing import List



class Extractor(ABC):
    """Abstract interface for data extractors."""
    
    @abstractmethod
    def extract(self, source_path: str) -> pl.LazyFrame:
        """
        Extract data from a source.
        
        Args:
            source_path: Path to the data source
            
        Returns:
            List of Record objects containing extracted data
        """
        pass
    
    @abstractmethod
    def validate_source(self, source_path: str) -> bool:
        """
        Validate if the source is accessible and valid.
        
        Args:
            source_path: Path to the data source
            
        Returns:
            True if source is valid, False otherwise
        """
        pass

