import logging

from configs import TABLES_CONFIG
from src.utils import db_exec, load_function

logger = logging.getLogger(__name__)

class ExecutionException(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class PostProcessingException(Exception):
    def __init__(self, msg):
        super().__init__(msg)
            
def process(report, conn, config):
    table_name = config['name']
    try:
        # Execute the SQL for the table
        db_exec(conn, f'DROP TABLE {config["name"]} IF EXISTS')
        db_exec(conn, config['sql'])
        logger.success(f'Successfully processed table: {table_name}')
        report['executed']['success'].append(table_name)

    except Exception as e:
        # Log failure
        logger.error(f'Error processing table "{table_name}": {e}')
        report['executed']['failure'][table_name] = str(e)
        raise ExecutionException(str(e))

def post_process(report, conn, config):
    table_name = config['name']

    if 'post_process' not in config:
        return
        
    try:
        # Load the function based on the new config structure
        if isinstance(config['post_process'], dict):
            proc = load_function(config['post_process']['function'])
            # Get parameters from config
            params = config['post_process'].get('params', {})
            # Call function with table_name, connection and unpacked parameters
            proc(table_name, conn, **params)
        else:
            # Maintain backward compatibility for simple function references
            proc = load_function(config['post_process'])
            proc(table_name, conn)

        logger.success(f'Successfully post processed table: {table_name}')
        report['post_processing']['success'].append(table_name)
    
    except Exception as e:
        logger.error(f'Error post processing table "{table_name}": {e}')
        report['post_processing']['failure'][table_name] = str(e)
        raise PostProcessingException(str(e))