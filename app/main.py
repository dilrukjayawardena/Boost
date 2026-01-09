import polars as pl
import multiprocessing as mp
from app.src.etl.orchestrator import ETLOrchestrator
from app.src.adapters.loaders.storage_loader_factory import StorageLoaderFactory
from app.src.adapters.extractors.source_extractor_factory import SourceExtractorFactory
from app.src.domain.entities import ETLConfig, DataSourceType, StorageType
from app.src.adapters.loaders.database_loader import DatabaseLoader
from app.src.adapters.extractors.csv_extractor import CsvExtractor
from app.src.adapters.extractors.json_extractor import JsonExtractor
from app.src.utils.transform_helpers import handle_paid_amount,str_to_bool,convert_to_timestamp,convert_date
from app.src.utils.crypto import encrypt_value
from app.src.utils.logger import ETLLogger
def example_json_to_database():
    """Example: Extract JSON, transform, and load to database."""
    config_user = ETLConfig(
        source_type=DataSourceType.JSON,
        source_path="app/src/data/test.json",
        storage_type=StorageType.DATABASE,
        storage_config={
            "table_name": "users",
            "create_table": True,
            "mode": "insert"
        },
        unnest_config = {
            "column_to_extract":"user_details",
            "operations":{"0":"unnest"}
        },
        transformer_config={
            "columns_to_select": ["user_id","name","dob","address","username","password","national_id","created_at","updated_at","logged_at"],
            "exp_value_transforms": {"logged_at":pl.from_epoch(pl.col("logged_at"), time_unit="s"),
                                     "dob":pl.col("dob").str.to_date("%Y-%m-%d")},
            "value_transforms":{"updated_at":convert_to_timestamp,
                                "created_at":convert_to_timestamp,
                                "national_id":encrypt_value,
                                "password":encrypt_value},

        }
    )
    config_telephone = ETLConfig(
        source_type=DataSourceType.JSON,
        source_path="app/src/data/test.json",
        storage_type=StorageType.DATABASE,
        storage_config={
            "table_name": "telephone_numbers",
            "create_table": True,
            "mode": "insert"
        },
        unnest_config = {
            "preop":{
                "column_to_extract":"user_details",
                "operations":{"0":"unnest"}
            },
            "column_to_extract": "telephone_numbers",
            "operations":{"0":"explod"}
        },
        transformer_config={
            "columns_to_select": ["user_id","telephone_numbers"],
            "field_mappings": {
                "telephone_numbers":"telephone_number"
            }
        }
    )
    config_job_history = ETLConfig(
        source_type=DataSourceType.JSON,
        source_path="app/src/data/test.json",
        storage_type=StorageType.DATABASE,
        storage_config={
            "table_name": "jobs_history",
            "create_table": True,
            "mode": "insert"
        },
        unnest_config = {
            "column_to_extract":"jobs_history",
            "operations":{
                            "0":"explode",
                            "1":"unnest"
                          }
        },
        transformer_config={
            "columns_to_select": ["user_id","occupation","is_fulltime","start","end"],
            "exp_value_transforms":{"start":pl.col("start").str.to_date("%Y-%m-%d"),
                                "end":pl.col("end").str.to_date("%Y-%m-%d")
                                }, 
        }
    )
    configlist = [config_user,config_telephone,config_job_history]
    for config in configlist:
        run_etl_task(config)


def example_csv_to_database():
    """Example: Extract CSV, transform, and load to JSON file."""
    config = ETLConfig(
        source_type=DataSourceType.CSV,
        source_path="app/src/data/test.csv",
        storage_type=StorageType.DATABASE,
        storage_config={
            "table_name": "tests",
            "create_table": True,
            "mode": "insert"
        },
        unnest_config={},
        transformer_config={
            "exp_value_transforms": {"last_login": pl.from_epoch(pl.col("last_login"), time_unit="s")},
            "value_transforms":{"paid_amount":handle_paid_amount,
                                "is_claimed":str_to_bool,
                                "created_at":convert_to_timestamp},
        }
    )
    
    run_etl_task(config)

def run_etl_task(config: ETLConfig):
    """Worker function to execute ETL for a single configuration."""
    try:
        orchestrator = ETLOrchestrator()
        orchestrator.run(config)
    except Exception as e:
        print(f"Error running ETL for {config.storage_config['table_name']}: {e}")
        
    
def run_example():
    StorageLoaderFactory.register(StorageType.DATABASE, DatabaseLoader)
    SourceExtractorFactory.register(DataSourceType.CSV, CsvExtractor)
    SourceExtractorFactory.register(DataSourceType.JSON, JsonExtractor)
    ETLLogger.configure(
        log_level="DEBUG"
    )
    example_json_to_database()
    example_csv_to_database()
    
if __name__ == "__main__":

    run_example()