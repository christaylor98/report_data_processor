"""
Test configuration settings.
"""

import os
from pathlib import Path

# Test identifiers
TEST_PREFIX = "TEST_"
TEST_MODE_PREFIX = "TEST_MODE_"
TEST_STRAND_PREFIX = "TEST_STRAND_"

# Test database configuration - using existing database
TEST_DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '3306')),
    'user': os.getenv('DB_USER', 'finance'),  # Match the default user from db/__init__.py
    'password': os.getenv('DB_PASSWORD', ''),  # Empty password as default
    'database': os.getenv('DB_NAME', 'trading'),
    'pool_size': 2,  # Smaller pool size for tests
    'connect_timeout': 5,
    'read_timeout': 10,
    'write_timeout': 10
}

# Test directory configuration
TEST_DIRS = {
    'input_dir': str(Path(__file__).parent / 'test_data' / 'input'),
    'processed_dir': str(Path(__file__).parent / 'test_data' / 'processed'),
    'output_dir': str(Path(__file__).parent / 'test_data' / 'output'),
    'temp_dir': str(Path(__file__).parent / 'test_data' / 'temp')
}

def get_test_config():
    """Get complete test configuration."""
    return {
        **TEST_DIRS,
        'db_config': TEST_DB_CONFIG,
        'test_prefix': TEST_PREFIX,
        'test_mode_prefix': TEST_MODE_PREFIX,
        'test_strand_prefix': TEST_STRAND_PREFIX
    } 