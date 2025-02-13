import logging

from src.utils import db_get, load_function

from configs import TABLES_CONFIG

logger = logging.getLogger(__name__)

class TestFailedException(Exception):
    def __init__(self, msg):
        super().__init__(msg)

def validate_table_exists(report, conn, table_name):
    """
    Checks if a table exists in the database.
    Updates the report with the result.
    """
    query = f"""
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = '{table_name}'
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        exists = cursor.fetchone()[0] > 0

    if exists:
        logger.success(f"Table '{table_name}' exists.")
        report["table_creation"]["success"].append(table_name)
    else:
        logger.error(f"Table '{table_name}' does not exist.")
        report["table_creation"]["failure"][table_name] = "Table does not exist."

    return exists


def validate_primary_key_uniqueness(report, conn, table_name, pks):
    """
    Validates that the primary key column is unique.
    Updates the report with the result.
    """
    pks_str = ', '.join(pks)
    query = f"""
    SELECT {pks_str}, COUNT(*)
    FROM {table_name}
    GROUP BY {pks_str}
    HAVING COUNT(*) > 1
    """
    
    duplicates = db_get(conn, query)

    if len(duplicates) == 0:
        logger.success(f"Primary key validation passed for '{table_name}'.")
        report["primary_key_validation"]["success"].append(table_name)
        return True
    else:
        logger.error(f"Primary key validation failed for '{table_name}'. {len(duplicates)} Duplicate values found.")
        report["primary_key_validation"]["failure"][table_name] = f"{len(duplicates)} Duplicate primary key values found."
        return False


def generic_validator(report, conn, config):
    table_name = config['name']

    table_exists = validate_table_exists(report, conn, table_name)

    # Skip primary key validation if the table does not exist
    if not table_exists:
        logger.warning(f"Skipping primary key validation for '{table_name}' (table does not exist).")
        report["primary_key_validation"]["skipped"].append(table_name)
        return

    # Validate primary key uniqueness
    pks = config.get("exp_pks")
    if pks:
        validate_primary_key_uniqueness(report, conn, table_name, pks)

def custom_validator(report, conn, config):
    table_name = config['name']

    if 'custom_tests' not in config:
        return

    report['custom_tests']['success'][table_name] = []
    
    for func_path in config['custom_tests']:
        try:
            test = load_function(func_path)
            test(report, conn, table_name)
            logger.success(f'Test "{func_path}" PASSED for table {table_name}!')
            report['custom_tests']['success'][table_name].append(func_path)
        except TestFailedException as e:
            logger.error(f'Test "{func_path}" FAILED for table {table_name}!')
            report['custom_tests']['failure'][table_name].append(func_path)