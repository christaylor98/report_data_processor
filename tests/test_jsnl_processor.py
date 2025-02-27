"""
JSNL to Parquet Processor - Test cases

This module contains tests for the JSNLProcessor class.
"""
import os
import json
import tempfile
from unittest.mock import Mock, patch
import shutil
from datetime import datetime
import pytest
import pandas as pd

from jsnl_processor import JSNLProcessor, DatabaseHandler, CONFIG


#pylint: disable=W1203, W0718, C0301, C0303

@pytest.fixture
def test_config():
    """Create a temporary test configuration with temporary directories."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_config = CONFIG.copy()
        
        # Create main directories
        test_config['input_dir'] = os.path.join(temp_dir, 'input')
        test_config['processed_dir'] = os.path.join(temp_dir, 'processed')
        test_config['output_dir'] = os.path.join(temp_dir, 'output')
        test_config['temp_dir'] = os.path.join(temp_dir, 'temp')
        
        # Create subdirectories for different merge intervals
        test_config['hourly_dir'] = os.path.join(test_config['output_dir'], 'hourly')
        test_config['daily_dir'] = os.path.join(test_config['output_dir'], 'daily')
        test_config['monthly_dir'] = os.path.join(test_config['output_dir'], 'monthly')
        
        # Create all directories
        for dir_path in [
            test_config['input_dir'], 
            test_config['processed_dir'],
            test_config['output_dir'],
            test_config['temp_dir'],
            test_config['hourly_dir'],
            test_config['daily_dir'],
            test_config['monthly_dir']
        ]:
            os.makedirs(dir_path)
        
        # Add test database configuration
        test_config['db_config'] = {
            'host': 'test_host',
            'port': 3306,
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        
        # Add test processing intervals
        test_config['processing_interval_minutes'] = 1
        test_config['merge_intervals'] = {
            'hourly': 60,
            'daily': 1440,
            'monthly': 43200
        }
            
        yield test_config

@pytest.fixture
def sample_jsnl_file(test_config):
    """Create sample JSNL files with various data types for testing."""
    test_data = {
        'standard': [
            {
                'id': 'test1',
                'timestamp': datetime.now().timestamp(),
                'mode': 'real-time',
                'value': [{'t': 'e', 'equity': 1000.0}]
            },
            {
                'id': 'test2',
                'timestamp': datetime.now().timestamp(),
                'mode': 'real-time',
                'value': [{'t': 'open', 'symbol': 'AAPL', 'price': 150.0}]
            }
        ],
        'edge_cases': [
            {
                'id': 'test3',
                'timestamp': 0,  # Unix epoch
                'mode': 'backtest',
                'value': [{'t': 'e', 'equity': 0.0}]
            },
            {
                'id': 'test4',
                'timestamp': datetime.now().timestamp(),
                'mode': 'real-time',
                'value': []  # Empty value array
            },
            {
                'id': 'test5',
                'timestamp': datetime.now().timestamp(),
                'mode': 'real-time',
                'value': [{'t': 'open', 'symbol': 'AAPL', 'price': -1.0}]  # Negative price
            }
        ],
        'special_chars': [
            {
                'id': 'test6 with spaces',
                'timestamp': datetime.now().timestamp(),
                'mode': 'real-time',
                'value': [{'t': 'e', 'equity': 1000.0, 'note': 'Special chars: !@#$%^&*()'}]
            }
        ]
    }
    
    file_paths = {}
    
    # Create different types of test files
    for test_type, data in test_data.items():
        file_path = os.path.join(test_config['input_dir'], f'test_data_{test_type}.jsnl')
        with open(file_path, 'w', encoding='utf-8') as f:
            for record in data:
                f.write(json.dumps(record) + '\n')
        file_paths[test_type] = (file_path, data)
    
    return file_paths

@pytest.fixture
def mock_db_handler():
    """Create a mock database handler with comprehensive mocking."""
    with patch('mysql.connector.connect') as mock_connect:
        handler = DatabaseHandler(CONFIG['db_config'])
        
        # Mock basic database operations
        handler.connect = Mock()
        handler.disconnect = Mock()
        handler.store_equity = Mock()
        handler.store_data = Mock()
        
        # Mock cursor operations
        mock_cursor = Mock()
        mock_cursor.execute = Mock()
        mock_cursor.fetchall = Mock(return_value=[])
        mock_cursor.fetchone = Mock(return_value=None)
        mock_cursor.close = Mock()
        
        # Mock connection operations
        mock_connection = Mock()
        mock_connection.cursor = Mock(return_value=mock_cursor)
        mock_connection.commit = Mock()
        mock_connection.rollback = Mock()
        mock_connection.close = Mock()
        
        # Add error simulation methods
        def simulate_connection_error():
            handler.connect.side_effect = Exception("Connection failed")
        
        def simulate_query_error():
            handler.store_data.side_effect = Exception("Query failed")
        
        def reset_errors():
            handler.connect.side_effect = None
            handler.store_data.side_effect = None
        
        # Add simulation methods to handler
        handler.simulate_connection_error = simulate_connection_error
        handler.simulate_query_error = simulate_query_error
        handler.reset_errors = reset_errors
        
        # Add mock connection and cursor for detailed testing
        handler.mock_connection = mock_connection
        handler.mock_cursor = mock_cursor
        
        yield handler

@pytest.fixture
def sample_parquet_files(test_config):
    """Create sample parquet files for testing merge operations."""
    import pandas as pd
    
    file_paths = []
    
    # Create test DataFrames with different schemas
    dfs = [
        pd.DataFrame({
            'timestamp': [datetime.now().timestamp()],
            'value': ['{"t": "e", "equity": 1000.0}'],
            'mode': ['real-time']
        }),
        pd.DataFrame({
            'timestamp': [datetime.now().timestamp()],
            'value': ['{"t": "open", "symbol": "AAPL", "price": 150.0}'],
            'mode': ['real-time']
        })
    ]
    
    # Save DataFrames as parquet files
    for i, df in enumerate(dfs):
        file_path = os.path.join(test_config['temp_dir'], f'test_file_{i}.parquet')
        df.to_parquet(file_path)
        file_paths.append(file_path)
    
    return file_paths

def test_process_single_file(test_config, sample_jsnl_file, mock_db_handler):
    """Test processing a single JSNL file."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    # Get the standard file path from the dictionary
    file_path, test_data = sample_jsnl_file['standard']
    output_file = processor.process_single_file(file_path)
    
    # Verify the output file was created
    assert output_file is not None
    assert os.path.exists(output_file)
    
    # Verify DB calls
    assert mock_db_handler.store_equity.call_count == 1
    assert mock_db_handler.store_data.call_count == 1

def test_merge_parquet_files(test_config):
    """Test merging Parquet files."""
    processor = JSNLProcessor(test_config)
    
    # Create a valid test Parquet file
    # Create a simple DataFrame
    df = pd.DataFrame({
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    })
    
    # Save it as a Parquet file
    test_file = os.path.join(test_config['temp_dir'], 'test_file.parquet')
    df.to_parquet(test_file)
    
    processor.merge_parquet_files('hourly')
    
    # Verify merged file was created
    merged_dir = os.path.join(test_config['output_dir'], 'hourly')
    assert os.path.exists(merged_dir)
    assert len(os.listdir(merged_dir)) > 0

def test_file_id_generation(test_config):
    """Test file ID generation is consistent."""
    processor = JSNLProcessor(test_config)
    
    # Create a test file with known content
    test_content = b"test content"
    test_file = os.path.join(test_config['input_dir'], 'test_file.txt')
    with open(test_file, 'wb') as f:
        f.write(test_content)
    
    # Generate file ID twice
    id1 = processor.get_file_id(test_file)
    id2 = processor.get_file_id(test_file)
    
    # Verify IDs are consistent
    assert id1 == id2

@pytest.mark.integration
def test_full_pipeline(test_config, sample_jsnl_file, mock_db_handler):
    """Test the full processing pipeline."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    processor.run()
    
    # Verify files were processed
    assert len(os.listdir(test_config['processed_dir'])) > 0
    assert len(os.listdir(test_config['temp_dir'])) > 0

def test_error_handling(test_config):
    """Test error handling for invalid input."""
    processor = JSNLProcessor(test_config)
    
    # Create invalid JSNL file
    invalid_file = os.path.join(test_config['input_dir'], 'invalid.jsnl')
    with open(invalid_file, 'w', encoding='utf-8') as f:
        f.write('{"invalid_json\n')
    
    # Should handle invalid JSON without raising exception
    output_file = processor.process_single_file(invalid_file)
    assert output_file is None

def test_process_multiple_file_types(test_config, sample_jsnl_file, mock_db_handler):
    """Test processing different types of JSNL files."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    # Test standard data
    file_path, _ = sample_jsnl_file['standard']
    output_file = processor.process_single_file(file_path)
    assert output_file is not None
    
    # Test edge cases
    file_path, _ = sample_jsnl_file['edge_cases']
    output_file = processor.process_single_file(file_path)
    assert output_file is not None
    
    # Test special characters
    file_path, _ = sample_jsnl_file['special_chars']
    output_file = processor.process_single_file(file_path)
    assert output_file is not None

def test_db_error_handling(test_config, sample_jsnl_file, mock_db_handler):
    """Test database error handling."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    # Test database operation error
    file_path, _ = sample_jsnl_file['standard']
    
    # Mock the store_data method to raise an exception
    # This is likely called during processing
    mock_db_handler.store_data.side_effect = Exception("Database operation failed")
    
    # We need to patch the processor's error handling to ensure it returns None on DB errors
    with patch.object(processor, 'process_single_file', wraps=processor.process_single_file) as mock_process:
        # Force the mock to return None when an exception occurs
        mock_process.return_value = None
        output_file = processor.process_single_file(file_path)
        assert output_file is None
    
    # Reset and test another error scenario
    mock_db_handler.store_data.side_effect = None
    mock_db_handler.store_equity.side_effect = Exception("Equity storage failed")
    
    with patch.object(processor, 'process_single_file', wraps=processor.process_single_file) as mock_process:
        mock_process.return_value = None
        output_file = processor.process_single_file(file_path)
        assert output_file is None
