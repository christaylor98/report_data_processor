"""
Tests for live data processing functionality.
"""

# pylint: disable=W1203,C0303,C0301,C0304,W0212

import os
import json
# import tempfile
import unittest
from unittest.mock import Mock, patch
from datetime import datetime

import logging

from processor import JSNLProcessor
from processor.config_cache import ModeConfigCache
from db import DatabaseHandler
from tests.test_config import get_test_config

# Configure logging at the module level
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

class TestLiveDataProcessing(unittest.TestCase):
    """Test cases for live data processing functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        logger.info("Setting up test class")
        # Create test directories
        config = get_test_config()
        for dir_path in [config['input_dir'], config['processed_dir'],
                        config['output_dir'], config['temp_dir']]:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Created directory: {dir_path}")
    
    def setUp(self):
        """Set up test environment."""
        logger.info(f"\n{'='*80}\nSetting up test: {self._testMethodName}\n{'='*80}")
        
        # Get test configuration
        self.config = get_test_config()
        logger.info("Loaded test configuration")
        
        # Create mock database handler with all necessary methods
        self.mock_db = Mock(spec=DatabaseHandler)
        self.mock_db.execute_read_fetchone_with_retries.return_value = None
        self.mock_db.execute_write_query_with_retries.return_value = True
        
        # Create side effect for store_live_equity_batch that includes logging
        def store_live_equity_batch_side_effect(records):
            logger.debug(f"Processing {len(records)} records for live equity batch")
            for record in records:
                mode = record['mode']
                equity_value = record['value'].get('equity', 0.0)
                logger.debug(f"Processing record - Mode: {mode}, Equity: {equity_value}")
            logger.debug("Successfully stored live equity records")
        
        self.mock_db.store_live_equity_batch = Mock(side_effect=store_live_equity_batch_side_effect)
        self.mock_db.store_equity_batch = Mock()
        self.mock_db.init_pool = Mock()  # Mock init_pool to prevent real connection
        self.mock_db.check_pool = Mock(return_value=True)  # Mock check_pool to always return True
        self.mock_db.store_trading_instance = Mock(return_value=True)  # Mock store_trading_instance
        self.mock_db.ensure_partition_exists = Mock(return_value=True)  # Mock ensure_partition_exists
        logger.info("Created mock database handler")
        
        # Patch the DatabaseHandler class to return our mock
        self.db_patcher = patch('processor.DatabaseHandler', return_value=self.mock_db)
        self.db_patcher.start()
        logger.info("Patched DatabaseHandler class")
        
        # Create processor with mock database
        self.processor = JSNLProcessor(self.config)
        logger.info("Created JSNLProcessor with mock database")
        
        # Verify the processor is using our mock
        logger.info(f"Processor db_handler is mock: {isinstance(self.processor.db_handler, Mock)}")
        logger.info(f"Processor db_handler: {self.processor.db_handler}")
        
        # Create test data with test prefixes
        self.test_mode = f"{self.config['test_mode_prefix']}123"
        self.test_strand = f"{self.config['test_strand_prefix']}123"
        self.test_config = {
            'strand_id': self.test_strand,
            'name': f"{self.config['test_prefix']}Test Strategy",
            'live': True,
            'designator': 'strand_live',
            'config_file_path': f"{self.config['test_prefix']}/path/to/config.json",
            'tuning_date_range': '2024-01-01 to 2024-12-31'
        }
        logger.info(f"Created test data - Mode: {self.test_mode}, Strand: {self.test_strand}")
        
        # Reset mock before each test
        self.mock_db.reset_mock()
    
    def tearDown(self):
        """Clean up test environment."""
        logger.info(f"Cleaning up test: {self._testMethodName}")
        # Stop the patcher
        self.db_patcher.stop()
        logger.info("Stopped DatabaseHandler patch")
        
        # Clean up temporary directories
        for dir_path in [self.config['input_dir'], self.config['processed_dir'],
                        self.config['output_dir'], self.config['temp_dir']]:
            if os.path.exists(dir_path):
                for file in os.listdir(dir_path):
                    os.remove(os.path.join(dir_path, file))
                    logger.debug(f"Removed file: {file}")
                os.rmdir(dir_path)
                logger.debug(f"Removed directory: {dir_path}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test class."""
        logger.info("Cleaning up test class")
        # Clean up test directories
        config = get_test_config()
        for dir_path in [config['input_dir'], config['processed_dir'],
                        config['output_dir'], config['temp_dir']]:
            if os.path.exists(dir_path):
                for file in os.listdir(dir_path):
                    os.remove(os.path.join(dir_path, file))
                    logger.debug(f"Removed file: {file}")
                os.rmdir(dir_path)
                logger.debug(f"Removed directory: {dir_path}")
    
    def test_mode_config_cache(self):
        """Test ModeConfigCache functionality."""
        logger.info("Testing ModeConfigCache functionality")
        cache = ModeConfigCache(self.mock_db)
        
        # Test adding config
        logger.info("Testing config addition")
        cache.add_config(self.test_mode, self.test_config)
        self.assertEqual(cache.get_config(self.test_mode), self.test_config)
        logger.info("Successfully added and retrieved config")
        
        # Test updating config
        logger.info("Testing config update")
        updated_config = self.test_config.copy()
        updated_config['name'] = f"{self.config['test_prefix']}Updated Strategy"
        cache.add_config(self.test_mode, updated_config)
        self.assertEqual(cache.get_config(self.test_mode), updated_config)
        logger.info("Successfully updated config")
        
        # Test loading from database
        logger.info("Testing config loading from database")
        self.mock_db.execute_read_fetchone_with_retries.return_value = (json.dumps(self.test_config),)
        cache.clear()
        loaded_config = cache.get_config(self.test_mode)
        self.assertEqual(json.loads(loaded_config), self.test_config)
        logger.info("Successfully loaded config from database")
        
        # Test cache miss
        logger.info("Testing cache miss scenario")
        self.mock_db.execute_read_fetchone_with_retries.return_value = None
        cache.clear()
        result = cache.get_config(f"{self.config['test_mode_prefix']}nonexistent")
        self.assertIsNone(result)
        logger.info("Successfully handled cache miss")
    
    def test_process_strand_started(self):
        """Test processing of strand_started messages."""
        logger.info("Testing strand_started message processing")
        
        # Test valid strand_started message
        message = {
            'type': 'strand_started',
            'strand_id': self.test_strand,
            'mode': self.test_mode,
            'config': self.test_config
        }
        logger.info(f"Processing valid message: {message}")
        
        result = self.processor.process_component_started(message)
        self.assertTrue(result)
        logger.info("Successfully processed valid message")
        
        # Verify config was added to cache
        cached_config = self.processor.mode_cache.get_config(self.test_mode)
        self.assertEqual(cached_config['live'], True)
        self.assertEqual(cached_config['designator'], 'strand_live')
        logger.info("Verified config in cache")
        
        # Test invalid message
        invalid_message = {
            'type': 'strand_started',
            'mode': self.test_mode,
            'config': self.test_config
        }
        logger.info(f"Processing invalid message: {invalid_message}")
        
        result = self.processor.process_component_started(invalid_message)
        self.assertFalse(result)
        logger.info("Successfully handled invalid message")
    
    def test_live_equity_processing(self):
        """Test processing of live equity records."""
        logger.info("Testing live equity record processing")
        
        # Add test config to cache
        self.processor.mode_cache.add_config(self.test_mode, self.test_config)
        logger.info("Added test config to cache")
        
        # Create test equity record
        equity_record = {
            'timestamp': datetime.now().timestamp(),
            'mode': self.test_mode,
            'component': f"{self.config['test_prefix']}component",
            'value': {
                't': 'e',
                'equity': 1000.0,
                'b': f"{self.config['test_prefix']}broker"
            }
        }
        logger.info(f"Created test equity record: {equity_record}")
        
        # Test processing single record
        logger.info("Processing live equity record")
        self.processor._process_equity_records([equity_record])
        
        # Verify store_live_equity_batch was called
        self.mock_db.store_live_equity_batch.assert_called_once()
        logger.info("Verified live equity batch storage")
        
        # Test non-live record
        logger.info("Testing non-live equity record")
        non_live_config = self.test_config.copy()
        non_live_config['live'] = False
        non_live_mode = f"{self.config['test_mode_prefix']}non_live"
        self.processor.mode_cache.add_config(non_live_mode, non_live_config)
        
        non_live_record = equity_record.copy()
        non_live_record['mode'] = non_live_mode
        logger.info(f"Created non-live record: {non_live_record}")
        
        self.processor._process_equity_records([non_live_record])
        
        # Verify store_equity_batch was called
        self.mock_db.store_equity_batch.assert_called_once()
        logger.info("Verified non-live equity batch storage")
    
    def test_partition_management(self):
        """Test partition management functionality."""
        logger.info("Testing partition management")
        
        # Create a test equity record
        equity_record = {
            'timestamp': datetime.now().timestamp(),
            'mode': self.test_mode,
            'component': f"{self.config['test_prefix']}component",
            'value': {
                't': 'e',
                'equity': 1000.0,
                'b': f"{self.config['test_prefix']}broker"
            }
        }
        
        # Add test config to cache
        self.processor.mode_cache.add_config(self.test_mode, self.test_config)
        
        # Test processing live equity record
        logger.info("Testing live equity record processing")
        self.processor._process_equity_records([equity_record])
        
        # Verify the record was processed as live
        self.mock_db.store_live_equity_batch.assert_called_once()
        logger.info("Verified live equity record was processed")
        
        # Reset mock for next test
        self.mock_db.reset_mock()
        
        # Test non-live record
        logger.info("Testing non-live equity record")
        non_live_config = self.test_config.copy()
        non_live_config['live'] = False
        non_live_mode = f"{self.config['test_mode_prefix']}non_live"
        self.processor.mode_cache.add_config(non_live_mode, non_live_config)
        
        non_live_record = equity_record.copy()
        non_live_record['mode'] = non_live_mode
        
        # Process non-live record
        self.processor._process_equity_records([non_live_record])
        
        # Verify the record was processed as non-live
        self.mock_db.store_equity_batch.assert_called_once()
        logger.info("Verified non-live equity record was processed")
    
    def test_live_data_file_processing(self):
        """Test processing of a live data file."""
        logger.info("Testing live data file processing")
        
        # Ensure input directory exists
        os.makedirs(self.config['input_dir'], exist_ok=True)
        logger.info(f"Ensured input directory exists: {self.config['input_dir']}")
        
        # Create test file with strand_started and equity records
        test_file = os.path.join(self.config['input_dir'], f"{self.config['test_prefix']}live.jsnl")
        logger.info(f"Creating test file: {test_file}")
        
        with open(test_file, 'w', encoding='utf-8') as f:
            # Write strand_started message
            strand_started = {
                'type': 'strand_started',
                'strand_id': self.test_strand,
                'mode': self.test_mode,
                'config': self.test_config
            }
            f.write(json.dumps(strand_started) + '\n')
            logger.info("Wrote strand_started message")
            
            # Write equity record
            equity_record = {
                'timestamp': datetime.now().timestamp(),
                'mode': self.test_mode,
                'component': f"{self.config['test_prefix']}component",
                'value': {
                    't': 'e',
                    'equity': 1000.0,
                    'b': f"{self.config['test_prefix']}broker"
                }
            }
            f.write(json.dumps(equity_record) + '\n')
            logger.info("Wrote equity record")
        
        # Reset mock before processing
        self.mock_db.reset_mock()
        
        # Set up mock for process_single_file
        self.mock_db.store_live_equity_batch = Mock()
        self.mock_db.store_equity_batch = Mock()
        
        # Process the file
        logger.info("Processing test file")
        self.processor.process_single_file(test_file)
        
        # Verify config was cached
        cached_config = self.processor.mode_cache.get_config(self.test_mode)
        self.assertEqual(cached_config['live'], True)
        logger.info("Verified config in cache")
        
        # Verify equity record was processed
        self.mock_db.store_live_equity_batch.assert_called_once()
        logger.info("Verified equity record processing")
    
    def test_error_handling(self):
        """Test error handling in live data processing."""
        logger.info("Testing error handling")
        
        # Reset the mode cache to ensure clean state
        self.processor.mode_cache.clear()
        
        # Test database error during config storage
        logger.info("Testing database error during config storage")
        self.mock_db.execute_write_query_with_retries.side_effect = Exception("Database error")
        self.mock_db.store_trading_instance = Mock(side_effect=Exception("Database error"))
        
        message = {
            'type': 'strand_started',
            'strand_id': self.test_strand,
            'mode': self.test_mode,
            'config': self.test_config
        }
        logger.info(f"Processing message with database error: {message}")
        
        # Process the message
        result = self.processor.process_component_started(message)
        logger.info(f"Result: {result}")
        
        # Verify the result is False due to the error
        self.assertFalse(result)
        logger.info("Successfully handled database error")
        
        # Reset mock for next test
        self.mock_db.reset_mock()
        self.mock_db.execute_write_query_with_retries.side_effect = None
        self.mock_db.store_trading_instance = Mock(return_value=True)
        
        # Test invalid equity record
        logger.info("Testing invalid equity record handling")
        invalid_record = {
            'timestamp': datetime.now().timestamp(),
            'mode': self.test_mode,
            'component': f"{self.config['test_prefix']}component",
            'value': {
                't': 'e',
                'equity': 'invalid',  # Invalid equity value
                'b': f"{self.config['test_prefix']}broker"
            }
        }
        logger.info(f"Processing invalid record: {invalid_record}")
        
        # Process the invalid record
        self.processor._process_equity_records([invalid_record])
        
        # Verify no database calls were made for invalid record
        self.mock_db.store_live_equity_batch.assert_not_called()
        logger.info("Successfully handled invalid equity record")

if __name__ == '__main__':
    unittest.main() 