"""ETL orchestrator coordinating extract, transform, and load operations."""
import traceback
import polars as pl
from typing import Optional
from ..domain.entities import ETLConfig, DataSourceType, StorageType
from ..ports.extractor import Extractor
from ..ports.transformer import Transformer
from ..ports.loader import Loader
from ..adapters.transformers.default_transformer import DefaultTransformer
from ..adapters.loaders.storage_loader_factory import StorageLoaderFactory
from ..adapters.extractors.source_extractor_factory import SourceExtractorFactory
from ..utils.logger import get_logger
from ..utils.decorators import time_execution



class ETLOrchestrator:
    """Orchestrates ETL operations using hexagonal architecture."""
    
    def __init__(
        self,
        extractor: Optional[Extractor] = None,
        transformer: Optional[Transformer] = None,
        loader: Optional[Loader] = None
    ):
        """
        Initialize ETL orchestrator.
        
        Args:
            extractor: Optional custom extractor (defaults to auto-selection)
            transformer: Optional custom transformer (defaults to DefaultTransformer)
            loader: Optional custom loader (defaults to auto-selection)
        """
        self._extractor = extractor
        self._transformer = transformer or DefaultTransformer()
        self._loader = loader
        self.logger = get_logger("ETL.Orchestrator")
    
    def _get_extractor(self, source_type: DataSourceType) -> Extractor:
        """Get appropriate extractor based on source type."""
        if self._extractor:
            return self._extractor
        
        return SourceExtractorFactory.get_extractor(source_type)

    
    def _get_loader(self, storage_type: StorageType) -> Loader:
        """Get appropriate loader based on storage type."""
        if self._loader:
            return self._loader
        
        return StorageLoaderFactory.get_loader(storage_type)
    
    @time_execution(func_path="ETL.Orchestrator.run",logger_name="ETL.Orchestrator")
    def run(self, config: ETLConfig) -> bool:
        """
        Execute the ETL process.
        
        Args:
            config: ETL configuration
            
        Returns:
            True if ETL completed successfully, False otherwise
        """
        try:
            # Extract
            extractor = self._get_extractor(config.source_type)
            if not extractor.validate_source(config.source_path):
                raise ValueError(f"Invalid source: {config.source_path}")
            
            self.logger.info(f"Extracting data from {config.source_path}...")
            records = extractor.extract(config.source_path)
            self.logger.info(f"Extracted {records.count()} records")
            
            # Transform
            self.logger.info("Transforming data in bathces...")
            # unique_districts = records.select(pl.col("is_claimed").unique()).collect()
            # print(unique_districts)
            loader = self._get_loader(config.storage_type)
            if not loader.validate_connection(config):
                raise ValueError(f"Invalid storage connection: {config.storage_config}")
            for i, batch_df in enumerate(records.collect_batches(chunk_size=1000000)):
                success=self.handle_table(batch_df,config,loader)
                if success:
                    self.logger.info("ETL process completed successfully!")
                else:
                    self.logger.info("ETL process failed during loading")
            return True
            
        except Exception as e:
            self.logger.exception(f"ETL process failed: {e}")
            traceback.print_exc()
            return False

    def handle_table(self,batch_df: pl.DataFrame,config: ETLConfig,loader: Loader)-> bool:
        """Handle ETL depending on table complexity."""
        success =True
        if "column_to_extract" in config.unnest_config:
            table_data = self.handle_complex_data(batch_df, config.unnest_config)
        else:
            table_data=batch_df
        
        transformed_records = self._transformer.transform(
            table_data,
            config.transformer_config
        )

        self.logger.info(f"Loading data to {config.storage_type.value}...")
        success = loader.load(transformed_records, config.storage_config)
        return success


    def handle_complex_data(self,df,config):
        """Handle complex data extraction and transformation."""
        pre_ops =  config.get('preop',None)
        column_name = config.get('column_to_extract')
        op_order = config.get('operations')
        process_df = df
        if pre_ops:
            pre_column_name = pre_ops.get('column_to_extract')
            pre_op_order = pre_ops.get('operations')
            process_df = self.perform_op(process_df,pre_op_order,pre_column_name)
        process_df = self.perform_op(process_df,op_order,column_name)
        return process_df
    
    def perform_op(self,df,op_order,column_name):
        """Perform operations like unnest and explode in order."""
        process_df = df
        for i in range(0,len(op_order.items())):
            print(i)
            if op_order[str(i)] == "unnest":
                process_df = self.unnest_table(process_df,column_name)
            else:
                process_df = self.explod_table(process_df,column_name)
        return process_df

    def unnest_table(self, batch_df: pl.DataFrame,column_name: str) -> pl.DataFrame:
        """Unnest a nested column."""
        df_unnested_details = batch_df.unnest(column_name)
        return df_unnested_details

    def explod_table(self, batch_df: pl.DataFrame, column_name: str) -> pl.DataFrame:
        """Explode an array column."""
        exploded_df = batch_df.explode(column_name)
        return exploded_df
    

