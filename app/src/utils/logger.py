import logging
import sys


class ETLLogger:

    
    _configured: bool = False
    
    @classmethod
    def get_logger(cls, name: str = "ETL") -> logging.Logger:
        """
        Get or create a logger instance.
        
        Args:
            name: Logger name (default: "ETL")
            
        Returns:
            Configured logger instance
        """
        if not cls._configured:
            cls._configure()
        
        return logging.getLogger(name)
    
    @classmethod
    def _configure(
        cls,
        log_level: int = logging.INFO
    ):
        """
        Configure logging for the ETL system (console only).
        
        Args:
            log_level: Logging level (default: INFO)
        """
        if cls._configured:
            return
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Get root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Remove existing handlers to avoid duplicates
        root_logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        cls._configured = True
    
    @classmethod
    def configure(
        cls,
        log_level: str = "INFO"
    ):
        """
        Public method to configure logging with string log level (console only).
        
        Args:
            log_level: Logging level as string ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        """
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        
        numeric_level = level_map.get(log_level.upper(), logging.INFO)
        cls._configure(log_level=numeric_level)


def get_logger(name: str = "ETL") -> logging.Logger:
    return ETLLogger.get_logger(name)

