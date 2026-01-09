"""Port for data loading."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
# from ..domain.entities import Record


class Loader(ABC):
    """Abstract interface for data loaders."""
    
    @abstractmethod
    def load(self, records: List[str], config: Dict[str, Any]) -> bool:
        """
        Load records into storage.
        
        Args:
            records: List of records to load
            config: Configuration for the storage backend
            
        Returns:
            True if loading was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_connection(self, config: Dict[str, Any]) -> bool:
        """
        Validate connection to storage backend.
        
        Args:
            config: Configuration for the storage backend
            
        Returns:
            True if connection is valid, False otherwise
        """
        pass

