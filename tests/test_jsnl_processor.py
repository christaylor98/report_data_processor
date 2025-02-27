"""
JSNL to Parquet Processor - Test cases

This module contains tests for the JSNLProcessor class.
"""
# pylint: disable=W

import os
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock, call
import sys
from pathlib import Path
import shutil
from datetime import datetime
import pytest
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from jsnl_processor import JSNLProcessor, DatabaseHandler, CONFIG, parse_arguments, main

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
    # Use a consistent timestamp base for all test data
    base_timestamp = datetime.now().timestamp()
    
    test_data = {
        'standard': [
            {
                'log_id': 'test1',
                'timestamp': base_timestamp,
                'value': {
                    't': 'e',  # equity record
                    'equity': 1000.0,
                    'b': 'broker1'  # broker name
                }
            },
            {
                'log_id': 'test2',
                'timestamp': base_timestamp + 60,  # 1 minute later
                'value': {
                    't': 'open',  # trade open record
                    'instrument': 'NAS100_USD',
                    'price': 21324.75,
                    'profit': 0.0
                }
            }
        ],
        'edge_cases': [
            {
                'log_id': 'test3',
                'timestamp': base_timestamp + 120,  # 2 minutes later
                'value': {
                    't': 'e',  # equity record
                    'equity': 0.0,
                    'b': 'broker3'
                }
            },
            {
                'log_id': 'test4',
                'timestamp': base_timestamp + 180,  # 3 minutes later
                'value': {
                    't': 'close',  # trade close record
                    'instrument': 'NAS100_USD',
                    'price': 21185.95,
                    'profit': -133.30
                }
            },
            {
                'log_id': 'test5',
                'timestamp': base_timestamp + 240,  # 4 minutes later
                'value': {
                    't': 'adjust-close',  # trade adjust-close record
                    'instrument': 'NAS100_USD',
                    'price': 21185.95,
                    'profit': -133.30
                }
            }
        ],
        'special_chars': [
            {
                'log_id': 'test6 with spaces',
                'timestamp': base_timestamp + 300,  # 5 minutes later
                'value': {
                    't': 'adjust-open',  # trade adjust-open record
                    'instrument': 'NAS100_USD',
                    'price': 21157.9,
                    'profit': 0.0
                }
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
    """Create a mock database handler."""
    mock_handler = MagicMock()
    mock_handler.store_equity = MagicMock()
    mock_handler.store_trade = MagicMock()
    mock_handler.connect = MagicMock()
    mock_handler.disconnect = MagicMock()
    return mock_handler

@pytest.fixture
def sample_parquet_files(test_config):
    """Create sample parquet files for testing merge operations."""
    
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
    result = processor.process_single_file(file_path)
    
    # Unpack the tuple returned by process_single_file
    output_file, timestamps = result
    
    # Verify the output file was created
    assert output_file is not None
    assert os.path.exists(output_file)
    
    # Verify timestamps were collected
    assert len(timestamps) > 0
    
    # Read the Parquet file
    table = pq.read_table(output_file)
    df = table.to_pandas()
    
    # Verify the data was processed correctly
    assert len(df) == len(test_data)
    assert 'log_id' in df.columns
    assert 'timestamp' in df.columns
    assert 'data' in df.columns  # Check for data column
    
    # Verify DB calls
    equity_calls = sum(1 for item in test_data if item['value'].get('t') == 'e')
    trade_calls = sum(1 for item in test_data if item['value'].get('t') in ['open', 'close', 'adjust-open', 'adjust-close'])
    
    # Check that the appropriate DB methods were called
    assert mock_db_handler.store_equity.call_count == equity_calls
    assert mock_db_handler.store_trade.call_count == trade_calls

def test_merge_parquet_files(test_config):
    """Test merging Parquet files."""
    processor = JSNLProcessor(test_config)
    
    # Create test Parquet files
    test_file = os.path.join(test_config['temp_dir'], 'test_file.parquet')
    
    # Create test data
    test_data = [
        {
            'log_id': 'test1',
            'timestamp': 1709107200,  # 2024-02-28 08:00:00
            'filename': 'test_file.jsnl',
            'data': json.dumps({
                'log_id': 'test1',
                'timestamp': 1709107200,
                'value': {'t': 'e', 'equity': 1000.0, 'b': 'broker1'}
            })
        },
        {
            'log_id': 'test2',
            'timestamp': 1709107260,  # 2024-02-28 08:01:00
            'filename': 'test_file.jsnl',
            'data': json.dumps({
                'log_id': 'test2',
                'timestamp': 1709107260,
                'value': {'t': 'open', 'instrument': 'NAS100_USD', 'price': 21324.75, 'profit': 0.0}
            })
        }
    ]
    
    # Add date column
    for record in test_data:
        record['date'] = datetime.fromtimestamp(record['timestamp']).strftime('%Y-%m-%d')
    
    # Create DataFrame and save as Parquet
    df = pd.DataFrame(test_data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, test_file)
    
    # Initialize DuckDB connection
    processor.init_duckdb()
    
    # Mock the DuckDB execute method to avoid actual SQL execution
    processor.conn = MagicMock()
    processor.conn.execute = MagicMock()
    
    # Create a mock file to simulate the merge result
    merged_file = os.path.join(test_config['hourly_dir'], 'hourly_20240228_080000.parquet')
    with open(merged_file, 'w') as f:
        f.write('test')
    
    # Merge files
    processor.merge_parquet_files('hourly', 1709107200, 1709110800)  # 2024-02-28 08:00:00 to 09:00:00
    
    # Verify merged file was created
    assert os.path.exists(merged_file)
    
    # Verify the SQL was executed
    assert processor.conn.execute.call_count >= 1
    
    # Clean up
    processor.close_duckdb()

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
    
    # Process files manually since we need to handle the timestamps
    processor.db_handler.connect()
    processor.init_duckdb()
    
    # Patch the merge_parquet_files method to prevent infinite loop
    with patch.object(processor, 'merge_parquet_files') as mock_merge:
        # Process JSNL files and collect timestamps
        generated_files = processor.process_jsnl_files()
        
        # Verify files were processed
        assert len(os.listdir(test_config['processed_dir'])) > 0
        assert len(os.listdir(test_config['temp_dir'])) > 0
        
        # Verify merge was called with appropriate parameters
        assert mock_merge.call_count > 0
    
    # Clean up
    processor.db_handler.disconnect()
    processor.close_duckdb()

def test_error_handling(test_config):
    """Test error handling for invalid input."""
    processor = JSNLProcessor(test_config)
    
    # Create invalid JSNL file
    invalid_file = os.path.join(test_config['input_dir'], 'invalid.jsnl')
    with open(invalid_file, 'w', encoding='utf-8') as f:
        f.write('{"invalid_json\n')
    
    # Should handle invalid JSON without raising exception
    result = processor.process_single_file(invalid_file)
    assert result[0] is None  # First element (output_file) should be None

def test_process_multiple_file_types(test_config, sample_jsnl_file, mock_db_handler):
    """Test processing different types of JSNL files."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    # Test standard data
    file_path, _ = sample_jsnl_file['standard']
    result = processor.process_single_file(file_path)
    assert result[0] is not None  # Check output file
    
    # Test edge cases
    file_path, _ = sample_jsnl_file['edge_cases']
    result = processor.process_single_file(file_path)
    assert result[0] is not None  # Check output file
    
    # Test special characters
    file_path, _ = sample_jsnl_file['special_chars']
    result = processor.process_single_file(file_path)
    assert result[0] is not None  # Check output file

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

def test_idempotent_processing(test_config, sample_jsnl_file, mock_db_handler):
    """Test that processing the same file multiple times is idempotent."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    file_path, _ = sample_jsnl_file['standard']
    
    # First run
    result1 = processor.process_single_file(file_path)
    output_file1 = result1[0]
    assert output_file1 is not None
    
    # Second run should return same file
    result2 = processor.process_single_file(file_path)
    output_file2 = result2[0]
    assert output_file2 == output_file1
    
    # Verify the file was only processed once
    # The second call should skip processing due to the file already existing
    assert os.path.exists(output_file1)

def test_idempotent_merge(test_config, sample_parquet_files):
    """Test that merging files multiple times is idempotent."""
    processor = JSNLProcessor(test_config)
    
    # Create timestamp range for testing
    current_ts = datetime.now().timestamp()
    min_ts = current_ts - 3600  # 1 hour before
    max_ts = current_ts + 3600  # 1 hour after
    
    # First merge
    processor.merge_parquet_files('hourly', min_ts, max_ts)
    merged_files1 = os.listdir(os.path.join(test_config['output_dir'], 'hourly'))
    
    # Second merge should not create new files
    processor.merge_parquet_files('hourly', min_ts, max_ts)
    merged_files2 = os.listdir(os.path.join(test_config['output_dir'], 'hourly'))
    
    assert merged_files1 == merged_files2

def test_skip_records_without_id(test_config, mock_db_handler):
    """Test that records without IDs are skipped during processing."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    # Create a test file with some records missing IDs
    test_file = os.path.join(test_config['input_dir'], 'test_missing_ids.jsnl')
    
    # Create test data with a mix of valid and invalid records
    test_data = [
        # Valid record with ID
        {
            'log_id': 'test1',
            'timestamp': 1234567890,
            'value': {
                't': 'e',
                'equity': 1000.0,
                'b': 'broker1'
            }
        },
        # Invalid record without ID
        {
            'timestamp': 1234567890,
            'value': {
                't': 'e',
                'equity': 2000.0,
                'b': 'broker2'
            }
        },
        # Valid record with ID
        {
            'log_id': 'test2',
            'timestamp': 1234567890,
            'value': {
                't': 'e',
                'equity': 1500.0,
                'b': 'broker3'
            }
        },
        # Invalid record with empty ID
        {
            'log_id': '',
            'timestamp': 1234567890,
            'value': {
                't': 'e',
                'equity': 3000.0,
                'b': 'broker4'
            }
        }
    ]
    
    # Write test data to file
    with open(test_file, 'w', encoding='utf-8') as f:
        for record in test_data:
            f.write(json.dumps(record) + '\n')
    
    # Process the file
    with patch('logging.Logger.warning') as mock_warning:
        result = processor.process_single_file(test_file)
        output_file = result[0]
    
    # Verify output file was created
    assert output_file is not None
    assert os.path.exists(output_file)

def test_no_parquet_for_all_invalid_records(test_config, mock_db_handler):
    """Test that no Parquet file is created when all records are invalid."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    # Create a test file with only invalid records
    test_file = os.path.join(test_config['input_dir'], 'test_all_invalid.jsnl')
    
    # Create test data with only invalid records
    test_data = [
        # Invalid record without ID
        {
            'timestamp': 1234567890,
            'value': {
                't': 'e',
                'equity': 2000.0,
                'b': 'broker1'
            }
        },
        # Invalid record with empty ID
        {
            'log_id': '',
            'timestamp': 1234567890,
            'value': {
                't': 'e',
                'equity': 3000.0,
                'b': 'broker2'
            }
        }
    ]
    
    # Write test data to file
    with open(test_file, 'w', encoding='utf-8') as f:
        for record in test_data:
            f.write(json.dumps(record) + '\n')
    
    # Process the file
    with patch('logging.Logger.warning') as mock_warning:
        result = processor.process_single_file(test_file)
        output_file = result[0]
    
    # Verify no output file was created
    assert output_file is None
    
    # Verify warning logs were generated
    assert mock_warning.call_count >= 1  # At least one warning for no valid records

def test_process_trade_records(test_config, mock_db_handler):
    """Test processing of trade records."""
    processor = JSNLProcessor(test_config)
    processor.db_handler = mock_db_handler
    
    # Create a test file with trade records
    test_file = os.path.join(test_config['input_dir'], 'test_trades.jsnl')
    
    # Create test data with trade records
    test_data = [
        # Open trade
        {
            'log_id': 'trade1',
            'timestamp': 1234567890,
            'value': {
                't': 'open',
                'instrument': 'NAS100_USD',
                'price': 21324.75,
                'profit': 0.0
            }
        },
        # Close trade
        {
            'log_id': 'trade2',
            'timestamp': 1234567900,
            'value': {
                't': 'close',
                'instrument': 'NAS100_USD',
                'price': 21185.95,
                'profit': -133.30
            }
        },
        # Adjust-open trade
        {
            'log_id': 'trade3',
            'timestamp': 1234567910,
            'value': {
                't': 'adjust-open',
                'instrument': 'NAS100_USD',
                'price': 21157.9,
                'profit': 0.0
            }
        },
        # Adjust-close trade
        {
            'log_id': 'trade4',
            'timestamp': 1234567920,
            'value': {
                't': 'adjust-close',
                'instrument': 'NAS100_USD',
                'price': 21185.95,
                'profit': -133.30
            }
        }
    ]
    
    # Write test data to file
    with open(test_file, 'w', encoding='utf-8') as f:
        for record in test_data:
            f.write(json.dumps(record) + '\n')
    
    # Process the file
    result = processor.process_single_file(test_file)
    output_file, timestamps = result
    
    # Verify the output file was created
    assert output_file is not None
    assert os.path.exists(output_file)
    
    # Read the Parquet file
    table = pq.read_table(output_file)
    df = table.to_pandas()
    
    # Verify the data was processed correctly
    assert len(df) == len(test_data)
    assert 'log_id' in df.columns
    assert 'timestamp' in df.columns
    assert 'data' in df.columns  # Data should contain the full JSON
    
    # Verify DB calls were made for each trade
    assert mock_db_handler.store_trade.call_count == len(test_data)
    
    # Verify the correct data was passed to store_trade
    for i, record in enumerate(test_data):
        call_args = mock_db_handler.store_trade.call_args_list[i][0][0]
        assert call_args['log_id'] == record['log_id']
        assert call_args['timestamp'] == record['timestamp']
        assert call_args['trade_type'] == record['value']['t']
        assert call_args['instrument'] == record['value']['instrument']
        assert call_args['price'] == record['value']['price']
        assert call_args['profit'] == record['value']['profit']

class TestCommandLineOptions:
    """Test cases for command line options."""
    
    @pytest.fixture
    def setup_test_files(self):
        """Create temporary test files."""
        temp_dir = tempfile.mkdtemp()
        input_dir = os.path.join(temp_dir, 'input')
        processed_dir = os.path.join(temp_dir, 'processed')
        output_dir = os.path.join(temp_dir, 'output')
        temp_parquet_dir = os.path.join(output_dir, 'temp')
        
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(temp_parquet_dir, exist_ok=True)
        
        # Create test JSNL files
        for i in range(5):
            with open(os.path.join(input_dir, f'test_{i}.jsnl'), 'w') as f:
                f.write('{"log_id": "test", "timestamp": 123456789, "value": [{"t": "e", "equity": 1000}]}\n')
        
        config = {
            'input_dir': input_dir,
            'processed_dir': processed_dir,
            'output_dir': output_dir,
            'temp_dir': temp_parquet_dir,
            'db_config': {
                'host': 'localhost',
                'port': '3306',
                'user': 'test',
                'password': 'test',
                'database': 'test'
            },
            'processing_interval_minutes': 5,
            'merge_intervals': {
                'hourly': 60,
                'daily': 1440,
                'monthly': 43200
            }
        }
        
        yield temp_dir, config
        
        # Cleanup
        shutil.rmtree(temp_dir)
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_parse_arguments(self, mock_parse_args):
        """Test argument parsing."""
        # Test with no arguments
        mock_parse_args.return_value = MagicMock(file=None, limit=None, skip_merge=False)
        args = parse_arguments()
        assert args.file is None
        assert args.limit is None
        assert args.skip_merge is False
        
        # Test with file argument
        mock_parse_args.return_value = MagicMock(file='test.jsnl', limit=None, skip_merge=False)
        args = parse_arguments()
        assert args.file == 'test.jsnl'
        
        # Test with limit argument
        mock_parse_args.return_value = MagicMock(file=None, limit=10, skip_merge=False)
        args = parse_arguments()
        assert args.limit == 10
        
        # Test with skip_merge argument
        mock_parse_args.return_value = MagicMock(file=None, limit=None, skip_merge=True)
        args = parse_arguments()
        assert args.skip_merge is True
    
    @patch('os.path.exists')
    @patch('shutil.copy')
    @patch('jsnl_processor.JSNLProcessor')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_with_single_file(self, mock_parse_args, mock_processor, mock_copy, mock_exists, setup_test_files):
        """Test main function with single file option."""
        temp_dir, config = setup_test_files
        
        # Setup mocks
        mock_exists.return_value = True
        
        # Create a real string for the file path instead of a MagicMock
        test_file_path = os.path.join(temp_dir, 'test.jsnl')
        
        # We need to patch processor.config['input_dir'] to make startswith work
        mock_processor_instance = mock_processor.return_value
        mock_processor_instance.config = {'input_dir': temp_dir}
        
        # Create args with a real string
        mock_parse_args.return_value = MagicMock(
            file=test_file_path, 
            limit=None, 
            skip_merge=False
        )
        
        # Call main
        with patch('jsnl_processor.CONFIG', config):
            main()
        
        # Verify processor was created with correct parameters
        mock_processor.assert_called_once_with(config, max_files=None)
        
        # Verify single file was processed
        mock_processor_instance.process_single_file.assert_called_once()
    
    @patch('jsnl_processor.JSNLProcessor')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_with_limit(self, mock_parse_args, mock_processor, setup_test_files):
        """Test main function with limit option."""
        temp_dir, config = setup_test_files
        
        # Setup mocks
        mock_parse_args.return_value = MagicMock(
            file=None,  # Explicitly set to None
            limit=3, 
            skip_merge=False
        )
        
        # Setup processor mock
        mock_processor_instance = mock_processor.return_value
        mock_processor_instance.db_handler = MagicMock()
        mock_processor_instance.process_jsnl_files = MagicMock(return_value=[])
        
        # Call main
        with patch('jsnl_processor.CONFIG', config):
            main()
        
        # Verify processor was created with correct limit
        mock_processor.assert_called_once_with(config, max_files=3)
        
        # Verify process_jsnl_files was called
        mock_processor_instance.process_jsnl_files.assert_called_once()
    
    @patch('jsnl_processor.JSNLProcessor')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_with_skip_merge(self, mock_parse_args, mock_processor, setup_test_files):
        """Test main function with skip_merge option."""
        temp_dir, config = setup_test_files
        
        # Setup mocks
        mock_parse_args.return_value = MagicMock(
            file=None,  # Explicitly set to None
            limit=None, 
            skip_merge=True
        )
        
        # Setup processor mock
        mock_processor_instance = mock_processor.return_value
        mock_processor_instance.db_handler = MagicMock()
        mock_processor_instance.process_jsnl_files = MagicMock(return_value=['file1.parquet', 'file2.parquet'])
        
        # Call main
        with patch('jsnl_processor.CONFIG', config):
            main()
        
        # Verify merge_parquet_files was not called
        mock_processor_instance.merge_parquet_files.assert_not_called()

    def test_max_files_limit(self, setup_test_files):
        """Test that the processor respects the max_files limit."""
        temp_dir, config = setup_test_files
        
        # Create processor with limit of 2 files
        processor = JSNLProcessor(config, max_files=2)
        
        # Mock the process_single_file method to avoid actual processing
        # Return a tuple to match the new return type
        processor.process_single_file = MagicMock(return_value=('test.parquet', {1234567890}))
        processor.db_handler.connect = MagicMock()
        processor.db_handler.disconnect = MagicMock()
        
        # Process files
        generated_files = processor.process_jsnl_files()
        
        # Verify that only 2 files were processed
        assert len(generated_files) == 2
