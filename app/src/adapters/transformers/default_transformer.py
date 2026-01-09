"""Default transformer adapter with configurable transformation rules."""

import polars as pl
from typing import List, Dict, Any
# from ...domain.entities import Record
from ...ports.transformer import Transformer


class DefaultTransformer(Transformer):
    """Default transformer with configurable rules."""
    
    def transform(self, records: pl.DataFrame, config: Dict[str, Any] = None) -> pl.DataFrame:
        """
        Transform records based on configuration.
        
        Supported config options:
        - field_mappings: Dict mapping old field names to new field names
        - columns_to_select: List of fields to keep (if specified, only these fields are kept)
        - value_transforms: Dict of field names to transformation functions
        - exp_value_transforms: Dict of new fields to add with computed values
        """
        if config is None:
            config = {}
        
        # select columns
        if 'columns_to_select' in config:
            records=records.select(config["columns_to_select"])

        # Apply field mappings (rename fields)
        if 'field_mappings' in config:
            records = records.rename(config['field_mappings'])

        if 'exp_value_transforms' in config:
            records = records.with_columns(
                expr.alias(field)
                for field, expr in config["exp_value_transforms"].items())
            
        # Apply value transformations
        if 'value_transforms' in config:
            exprs = []

            for field, transform_func in config["value_transforms"].items():
                exprs.append(
                    pl.col(field).map_elements(transform_func).alias(field) 
                )

            records = records.with_columns(exprs)
        return records

