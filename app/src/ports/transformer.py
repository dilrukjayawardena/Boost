"""Port for data transformation."""

import polars as pl
from abc import ABC, abstractmethod
from typing import List, Dict, Any
# from ..domain.entities import Record


class Transformer(ABC):
    """Abstract interface for data transformers."""
    
    @abstractmethod
    def transform(self, records: pl.DataFrame, config: Dict[str, Any] = None) -> pl.DataFrame :
        """
        Transform a dataframe of records.
        
        Args:
            records: dataframe
            config: Optional configuration for transformation rules
            
        Returns:
            transformed dataframe
        """
        pass

