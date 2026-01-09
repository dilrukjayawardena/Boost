
from typing import Dict, Type
from ...domain.entities import StorageType
from ...ports.loader import Loader
class StorageLoaderFactory:
    """Factory to return loader instances based on StorageType."""

    _registry: Dict[StorageType, Type[Loader]] = {}

    @classmethod
    def register(cls, storage_type: StorageType, loader_cls: Type[Loader]):
        cls._registry[storage_type] = loader_cls

    @classmethod
    def get_loader(cls, storage_type: StorageType) -> Loader:
        loader_cls = cls._registry.get(storage_type)
        if not loader_cls:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        return loader_cls()