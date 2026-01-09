"""Domain entities for ETL operations."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from enum import Enum


class DataSourceType(Enum):
    """Enumeration of supported data source types."""
    JSON = "json"
    CSV = "csv"


class StorageType(Enum):
    """Enumeration of supported storage types."""
    DATABASE = "database"
    FILE = "file"


@dataclass
class ETLConfig:
    """Configuration for ETL operations."""
    source_type: DataSourceType
    source_path: str
    storage_type: StorageType
    storage_config: Dict[str, Any]
    unnest_config: Optional[Dict[str, Any]] = None
    transformer_config: Optional[Dict[str, Any]] = None





