from configs.dev import DB_CONFIG, TABLES_CONFIG
from configs.logger import configure_logger

configure_logger()

__all__ = ['TABLES_CONFIG', 'DB_CONFIG']