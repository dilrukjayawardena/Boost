
from typing import Dict, Type
from ...domain.entities import DataSourceType
from ...ports.extractor import Extractor
class SourceExtractorFactory:
    """Factory to return extractor instances based on extractor."""

    _registry: Dict[DataSourceType, Type[Extractor]] = {}

    @classmethod
    def register(cls, source_type: DataSourceType, extractor_cls: Type[Extractor]):
        """Register an extractor class for a given source type."""
        cls._registry[source_type] = extractor_cls

    @classmethod
    def get_extractor(cls, source_type: DataSourceType) -> Extractor:
        """Get an extractor instance based on source type."""
        extractor_cls = cls._registry.get(source_type)
        if not extractor_cls:
            raise ValueError(f"Unsupported source type: {source_type}")
        return extractor_cls()