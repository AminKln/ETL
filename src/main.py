import logging

from configs import TABLES_CONFIG
from src.etl import ExecutionException, PostProcessingException, post_process, process
from src.utils import db_conn
from src.validation.report_generator import generate_report
from src.validation.validation import custom_validator, generic_validator


def main():
    """
    LOOP (Begin)
        1. Process each table
        2. Post process if applicable
        3. Run general tests
        4. Run custom tests if applicable
    LOOP (End)
    5. Generate report
    """
    # Initialize the report dictionary
    report = {
        'executed': {'total': len(TABLES_CONFIG), 'success': [], 'failure': {}},
        'table_creation': {'total': len(TABLES_CONFIG), 'success': [], 'failure': {}},
        'primary_key_validation': {'total': len(TABLES_CONFIG), 'success': [], 'skipped': [], 'failure': {}},
        'post_processing': {'total': len({k: v for k, v in TABLES_CONFIG.items() if 'post_process' in v}), 'success': [], 'skipped': [], 'failure': {}},
        'custom_tests': {'total': sum([len(v['custom_tests']) for v in TABLES_CONFIG.values() if 'custom_tests' in v]), 'success': {}, 'skipped': {}, 'failure': {}}
    }

    
    logger = logging.getLogger(__name__)

    try:
        conn = db_conn()
        for table_name, config in TABLES_CONFIG.items():
            logger.info(f'Running ETL Pipeline for: {table_name}')
            process(report, conn, config)
            post_process(report, conn, config)
            generic_validator(report, conn, config)
            custom_validator(report, conn, config)

    except Exception as e:
        logger.error(f'Error running ETL pipeline: {e}')
    finally:
        generate_report(report)
        conn.close()

if __name__ == '__main__':
    main()