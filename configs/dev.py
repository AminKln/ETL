TABLES_CONFIG = {
    'Example Table': {
        'name': 'EXAMPLE_TABLE',
        'exp_pks': ['pk1', 'pk2'],
        'sql_path': 'sql/example_table.sql',
        'post_process': {
            'function': 'src.post_processing.standardize_date_intervals.standardize_date_intervals',
            'params': {
                'sdt_col': 'START_DATE',
                'ndt_col': 'END_DATE',
                'id_col': 'ENTITY_ID'
            }
        },
        'custom_tests': ['src.validation.tests.test_example'],
    }
}

TABLES_CONFIG = {
    table: {**config, 'sql': open(config['sql_path'], 'r').read()}
    for table, config in TABLES_CONFIG.items()
}

DB_CONFIG = {
    'dsn': 'DSN_EXAMPLE'
}