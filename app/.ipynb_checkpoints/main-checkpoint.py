import polars as pl
from app.src.etl.orchestrator import ETLOrchestrator
from app.src.adapters.loaders.storage_loader_factory import StorageLoaderFactory
from app.src.adapters.extractors.source_extractor_factory import SourceExtractorFactory
from app.src.domain.entities import ETLConfig, DataSourceType, StorageType
from app.src.adapters.loaders.database_loader import DatabaseLoader
from app.src.adapters.loaders.file_loader import FileLoader
from app.src.adapters.extractors.csv_extractor import CsvExtractor
from app.src.adapters.extractors.json_extractor import JsonExtractor
from app.src.utils.transform_helpers import handle_paid_amount,str_to_bool,convert_to_timestamp


def example_json_to_database():
    """Example: Extract JSON, transform, and load to database."""
    config = ETLConfig(
        source_type=DataSourceType.JSON,
        source_path="./data/input.json",
        storage_type=StorageType.DATABASE,
        storage_config={
            "db_path": "output.db",
            "table_name": "users",
            "create_table": True,
            "mode": "insert"
        },
        transformer_config={
            "field_mappings": {
                "user_id": "id",
                "email_address": "email"
            }
        }
    )
    
    orchestrator = ETLOrchestrator()
    orchestrator.run(config)


def example_csv_to_file():
    """Example: Extract CSV, transform, and load to JSON file."""
    config = ETLConfig(
        source_type=DataSourceType.CSV,
        source_path="app/src/data/test.csv",
        storage_type=StorageType.DATABASE,
        storage_config={
            "db_path": "output.db",
            "table_name": "tests",
            "create_table": True,
            "mode": "insert"
        },
        transformer_config={
            "value_transforms":{"paid_amount":handle_paid_amount,
                                "is_claimed":str_to_bool,
                                "created_at":convert_to_timestamp},
        }
    )
    
    orchestrator = ETLOrchestrator()
    orchestrator.run(config)
    
def run_example():
    StorageLoaderFactory.register(StorageType.DATABASE, DatabaseLoader)
    SourceExtractorFactory.register(DataSourceType.CSV, CsvExtractor)
    SourceExtractorFactory.register(DataSourceType.JSON, JsonExtractor)
    print("ETL System Examples")
    print("=" * 50)
    
    # Uncomment to run examples:
    # example_json_to_database()
    example_csv_to_file()
    
if __name__ == "__main__":
    StorageLoaderFactory.register(StorageType.DATABASE, DatabaseLoader)
    SourceExtractorFactory.register(DataSourceType.CSV, CsvExtractor)
    SourceExtractorFactory.register(DataSourceType.JSON, JsonExtractor)
    print("ETL System Examples")
    print("=" * 50)
    
    # Uncomment to run examples:
    # example_json_to_database()
    example_csv_to_file()
   



