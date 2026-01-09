import time
import functools
from typing import Callable, Any
from .logger import get_logger


def time_execution(func_path: str, logger_name: str = "ETL.Timing"):
    """
    Decorator to measure and log execution time of a function or method.
    
    Args:
        logger_name: Name of the logger to use for timing logs
        
    Returns:
        Decorated function that logs execution time
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = get_logger(logger_name)
            
            # Get function name for logging
            display_name = func_path

            # Start timing
            start_time = time.perf_counter()
            logger.info(f"Starting {display_name}...")
            
            try:
                # Execute the function
                result = func(*args, **kwargs)
                
                # Calculate elapsed time
                elapsed_time = time.perf_counter() - start_time
                
                # Format time for display
                if elapsed_time < 1:
                    time_str = f"{elapsed_time * 1000:.2f} ms"
                elif elapsed_time < 60:
                    time_str = f"{elapsed_time:.2f} seconds"
                else:
                    minutes = int(elapsed_time // 60)
                    seconds = elapsed_time % 60
                    time_str = f"{minutes} minutes {seconds:.2f} seconds"
                
                logger.info(f"Completed {display_name} in {time_str}")
                
                return result
                
            except Exception as e:
                # Calculate elapsed time even on error
                elapsed_time = time.perf_counter() - start_time
                time_str = f"{elapsed_time:.2f} seconds"
                logger.error(f"{display_name} failed after {time_str}: {e}")
                raise
        
        return wrapper
    return decorator