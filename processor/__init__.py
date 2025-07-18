'''
This is the main processor for the JSNL files.
'''

#pylint: disable=W1203, W0718, C0301, C0303
import json
import os
import sys
import logging
import time
import glob
import shutil
import hashlib
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import traceback
from typing import List, Dict, Any, Optional, Tuple, Set
import mmap

from dotenv import load_dotenv

import mariadb

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from db import DatabaseHandler
from .config_cache import ModeConfigCache
from .file_lock import FileLock

logger = logging.getLogger(__name__)

class JSNLProcessor:
    """Process JSNL files into Parquet format."""
    
    def __init__(self, config: Dict[str, Any], max_files: Optional[int] = None):
        """
        Initialize the processor with configuration.
        
        Args:
            config: Configuration dictionary
            max_files: Maximum number of files to process (optional)
        """
        self.config = config
        self.max_files = max_files
        self.current_file = None  # Track current file being processed
        
        # Initialize database handler
        self.db_handler = DatabaseHandler(config)
        
        # Initialize DuckDB connection
        self.conn = None
        
        # Initialize mode config cache
        self.mode_cache = ModeConfigCache(self.db_handler)
        
        # Track last equity value for each mode
        self.last_equity_values = {}  # Format: {mode: last_equity_value}
        
        # Track all equity records for final merge
        self.all_equity_records = []
        
    def init_duckdb(self) -> None:
        """Initialize DuckDB connection."""
        self.conn = duckdb.connect(':memory:')
        # Load Parquet extension
        self.conn.execute("INSTALL parquet")
        self.conn.execute("LOAD parquet")
        
    def close_duckdb(self) -> None:
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            
    def get_file_id(self, filepath: str) -> str:
        """Generate a unique ID for a file based on its content."""
        return hashlib.md5(Path(filepath).read_bytes()).hexdigest()
    
    def get_output_filename(self, jsnl_file: str, file_id: str) -> str:
        """Generate a deterministic output filename."""
        base_name = os.path.basename(jsnl_file).split('.')[0]
        return f"{base_name}_{file_id}.parquet"
    
    def process_jsnl_files(self) -> List[str]:
        """
        Process all JSNL files in the input directory.
        
        Returns:
            List of generated Parquet file paths
        """
        start_time = time.time()
        
        # Find all JSNL files in the input directory
        file_search_start = time.time()
        jsnl_files = glob.glob(os.path.join(self.config['input_dir'], '*.jsnl'))
        # Filter out files with 'current' in the filename
        jsnl_files = [f for f in jsnl_files if 'current' not in os.path.basename(f)]
        file_search_end = time.time()
        logger.info(f"File search took {file_search_end - file_search_start:.2f} seconds")
        
        if not jsnl_files:
            logger.info("No JSNL files found in input directory")
            return []
        
        # Sort files by timestamp in filename (if available)
        sort_start = time.time()
        sorted_files = self.sort_files_by_timestamp(jsnl_files)
        sort_end = time.time()
        logger.info(f"Sorting {len(jsnl_files)} files took {sort_end - sort_start:.2f} seconds")
        
        # Limit the number of files to process if specified
        if self.max_files and len(sorted_files) > self.max_files:
            logger.info(f"Sorted {len(sorted_files)} files by timestamp in filename")
            logger.info(f"Processing limited to {self.max_files} files")
            sorted_files = sorted_files[:self.max_files]
        
        logger.info(f"Found {len(sorted_files)} JSNL files to process")
        
        # Process each file
        generated_files = []
        timestamps = set()
        
        for jsnl_file in sorted_files:
            file_start_time = time.time()
            try:
                logger.info(f"Processing file in order: {os.path.basename(jsnl_file)}")
                
                # Process the file (locking is handled in process_single_file)
                process_start = time.time()
                result = self.process_single_file(jsnl_file)
                process_end = time.time()
                logger.info(f"File processing took {process_end - process_start:.2f} seconds")
                
                if result[0]:  # If a Parquet file was generated
                    generated_files.append(result[0])
                    timestamps.update(result[1])
                    
                    # Move the file to the processed directory
                    move_start = time.time()
                    processed_path = os.path.join(self.config['processed_dir'], os.path.basename(jsnl_file))
                    
                    if not self.config.get('leave_source_files', False):
                        try:
                            shutil.move(jsnl_file, processed_path)
                            logger.info(f"Moved processed file to {processed_path}")
                        except (shutil.Error, OSError) as e:
                            logger.warning(f"Could not move file {jsnl_file} to {processed_path}: {str(e)}")
                    move_end = time.time()
                    logger.info(f"File move took {move_end - move_start:.2f} seconds")
                
            except Exception as e:
                logger.error(f"Error processing file {jsnl_file}: {str(e)}")
                logger.error(traceback.format_exc())
                continue
            
            file_end_time = time.time()
            logger.info(f"Total file processing time: {file_end_time - file_start_time:.2f} seconds")
        
        end_time = time.time()
        logger.info(f"Total processing time: {end_time - start_time:.2f} seconds")
        logger.info(f"Generated {len(generated_files)} Parquet files")
        
        return generated_files
    
    def process_single_file(self, file_path: str) -> Tuple[Optional[str], Set[float]]:
        """
        Process a single JSNL file.
        
        Args:
            file_path: Path to the JSNL file
            
        Returns:
            Tuple of (output file path, set of timestamps)
        """
        # Initialize file lock
        file_lock = FileLock(os.path.join(self.config['temp_dir'], 'locks'))
        
        # Try to acquire lock
        with file_lock.acquire_lock(file_path) as lock_fd:
            if lock_fd is None:
                logger.warning(f"Could not acquire lock for {file_path}, skipping")
                return None, set()
            
            try:
                # Check if file still exists
                if not os.path.exists(file_path):
                    logger.warning(f"File no longer exists: {file_path}")
                    return None, set()
                
                # Generate file ID
                file_id = self.get_file_id(file_path)
                
                # Set up file processing
                output_file = self._setup_file_processing(file_path)
                if not output_file:
                    return None, set()
                
                # Process file contents
                records, timestamps, stats = self._process_file_contents(file_path)
                
                # Write records to Parquet file
                if records:
                    self._save_and_cleanup(file_path, output_file, records, timestamps)
                
                return output_file, timestamps
                
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                logger.error(traceback.format_exc())
                return None, set()
    
    def _setup_file_processing(self, file_path: str) -> Optional[str]:
        """
        Set up file processing by validating the file and preparing output path.
        
        Args:
            file_path: Path to the JSNL file
            
        Returns:
            Output Parquet file path if setup successful, None otherwise
        """
        # Check if file exists
        if not os.path.exists(file_path):
            logger.warning(f"File does not exist: {file_path}")
            return None
        
        # Generate a unique ID for this file based on content
        id_start = time.time()
        file_id = self.get_file_id(file_path)
        id_end = time.time()
        logger.info(f"File ID generation took {id_end - id_start:.2f} seconds")
        
        # Create output filename
        base_name = os.path.basename(file_path)
        output_file = os.path.join(self.config['temp_dir'], f"{os.path.splitext(base_name)[0]}_{file_id}.parquet")
        
        # Check if file already exists (idempotent)
        if os.path.exists(output_file):
            logger.info(f"File {output_file} already exists, skipping processing")
            self._move_to_processed(file_path)
            return output_file
        
        return output_file
    
    def _process_file_contents(self, file_path: str) -> Tuple[List[Dict], Set[float], Dict[str, int]]:
        """
        Process the contents of a JSNL file.
        
        Args:
            file_path: Path to the JSNL file
            
        Returns:
            Tuple of (records list, timestamps set, statistics dictionary)
        """
        records = []
        timestamps = set()
        
        # Statistics
        stats = {
            'total_records': 0,
            'invalid_records': 0,
            'missing_fields': 0,
            'equity_records': 0,
            'trade_records': 0,
            'skipped_db_only_records': 0,
            'skipped_equity_records': 0,
            'empty_lines': 0
        }
        
        # Temporary storage for candles and equity records
        # candles = {}  # Format: {(log_id, timestamp): candle_data}
        equity_records_data = []  # List of equity records
        last_mode = None
        try:
            read_start = time.time()
            with open(file_path, 'rb') as f:
                # Memory map the file for faster reading
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    logger.info(f"Processing file {file_path}, read into memory")
                    line_count = 0
                    line_process_time = 0
                    
                    for line in iter(mm.readline, b''):
                        line_start = time.time()
                        line_count += 1
                        
                        try:
                            # Decode and strip the line
                            line_str = line.decode('utf-8').strip()
                            
                            # Skip empty lines
                            if not line_str:
                                stats['empty_lines'] += 1
                                continue
                            
                            # Process the line - specifically the strand started message
                            if self.process_jsnl_line(line_str):
                                continue

                            # Parse and process the line
                            last_mode =self._process_single_line(line_str, records, timestamps, stats, 
                                                   equity_records_data)
                        
                        except json.JSONDecodeError:
                            stats['invalid_records'] += 1
                            if stats['invalid_records'] <= 5:
                                logger.warning(f"Invalid JSON at line {line_count}: {line[:100]}...")
                        except Exception as e:
                            stats['invalid_records'] += 1
                            if stats['invalid_records'] <= 5:
                                logger.warning(f"Error processing line {line_count}: {str(e)}")
                        
                        line_end = time.time()
                        line_process_time += (line_end - line_start)
                        
                        # Log progress for large files
                        if line_count % 10000 == 0:
                            logger.info(f"Processed {line_count} lines, avg time per line: {line_process_time/line_count:.6f} seconds")
                            logger.info(f"Statistics - Valid: {stats['total_records']}, Invalid: {stats['invalid_records']}, "
                                      f"Empty: {stats['empty_lines']}, Missing Fields: {stats['missing_fields']}")
            
            read_end = time.time()
            logger.info(f"File reading took {read_end - read_start:.2f} seconds for {stats['total_records']} records")
            
            # Process equity records with associated candles
            self._process_equity_records(equity_records_data)
            
            # Merge staging equity records into main table
            self.db_handler.merge_staging_equity(last_mode)
            
            # Log statistics
            self._log_processing_stats(file_path, stats)
            
            return records, timestamps, stats
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {str(e)}")
            logger.error(traceback.format_exc())
        
    
    def _process_single_line(self, line_str: str, records: List[Dict], timestamps: Set[float],
                           stats: Dict[str, int], equity_records_data: List[Dict]) -> Optional[str]:
        """
        Process a single line from the JSNL file.
        
        Args:
            line_str: The line to process
            records: List to store processed records
            timestamps: Set to store timestamps
            stats: Statistics dictionary
            equity_records_data: List to store equity records
            
        Returns:
            Optional[str]: The mode if found, None otherwise
        """
        # Parse JSON
        data = json.loads(line_str)
        stats['total_records'] += 1
    
        # Extract required fields
        timestamp = data.get('timestamp')
        mode = data.get('mode')
        component = data.get('component')
        value_obj = data.get('value', {})
        
        if not timestamp or not value_obj or not mode or not component:
            stats['missing_fields'] += 1
            return None
        
        # Check if we have config for this mode
        config = self.mode_cache.get_config(mode)
        if not config:
            logger.warning(f"No configuration found for mode {mode}, skipping record")
            stats['missing_fields'] += 1
            return None
        
        # Convert timestamp to float if it's not already
        if isinstance(timestamp, str):
            timestamp = float(timestamp)
        
        # Process the value object
        has_other_data = self._process_value_object(timestamp, mode, component, value_obj,
                                                  equity_records_data, records, stats)
        
        # Add timestamp to set for later use
        timestamps.add(timestamp)

        return mode
    
    def _process_value_object(self, timestamp: float, mode: str, component: str,
                            value_obj: Dict, equity_records_data: List[Dict], records: List[Dict],
                            stats: Dict[str, int]) -> bool:
        """
        Process a value object from a JSNL record.
        
        Args:
            timestamp: The timestamp
            mode: The mode
            component: The component
            value_obj: The value object to process
            equity_records_data: List to store equity records
            records: List to store processed records
            stats: Statistics dictionary
            
        Returns:
            bool: True if the object contains other data besides equity/trade
        """
        has_other_data = False
        
        # Handle single value object
        if isinstance(value_obj, dict):
            value_obj = [value_obj]
            
        equity = None
        trades = []
        others = []
        
        for item in value_obj:
            new_equity = self.process_value_item(item, trades, others)
            if new_equity:
                equity = new_equity

        # No equity records, no further processing
        if not equity:
            return False
        
        store_equity = False
        
        # Check if we should store equity based on configuration
        if self.config.get('equity_on_change', True):
            # Only store if value changed from previous
            previous_equity = self.last_equity_values.get(mode, None)
            if (previous_equity is None) or (previous_equity != equity['value']['equity']):
                store_equity = True
        else:
            # Store every equity record
            store_equity = True

        # Process trade records 
        if trades:
            for trade in trades:
                trade['timestamp'] = timestamp
                trade['mode'] = mode
                trade['component'] = component
                self.db_handler.store_trade(trade)
                stats['trade_records'] += 1
                
        # Process other records
        if others:
            record_data = {
                'timestamp': timestamp,
                'mode': mode,
                'component': component,
                'value': json.dumps(others)
            }
            records.append(record_data)
            has_other_data = True
        else:
            stats['skipped_db_only_records'] += 1

        # Process equity records
        if store_equity:
            equity['timestamp'] = timestamp
            equity['mode'] = mode
            equity['component'] = component
            equity_records_data.append(equity)
            stats['equity_records'] += 1
            self.last_equity_values[mode] = equity['value']['equity']
        else:
            stats['skipped_equity_records'] += 1
        
        return has_other_data
    
    def _process_equity_records(self, equity_records_data: List[Dict]) -> None:
        """
        Process equity records with associated candles.
        
        Args:
            equity_records_data: List of equity records
        """
        if not equity_records_data:
            logger.debug("No equity records to process")
            return
        
        logger.debug(f"Processing {len(equity_records_data)} equity records")
        db_start = time.time()
        
        # Group records by mode and check if they are live
        live_records = []
        non_live_records = []
        
        for record in equity_records_data:
            try:
                logger.debug(f"Processing record: {record}")
                # Validate equity value
                equity_value = record['value'].get('equity')
                logger.debug(f"Processing record for mode {record['mode']} with equity value: {equity_value}")
                
                if not isinstance(equity_value, (int, float)):
                    logger.warning(f"Invalid equity value type for mode {record['mode']}: {equity_value}")
                    continue
                
                mode = record['mode']
                config = self.mode_cache.get_config(mode)

                if config and config.get('live', False):
                    logger.debug(f"Mode {mode} is live, adding to live records")
                    live_records.append(record)
                else:
                    logger.debug(f"Mode {mode} is not live, adding to non-live records")
                    non_live_records.append(record)

            except Exception as e:
                logger.error(f"Error processing equity record: {str(e)}")
                logger.debug(f"Failed record: {record}")
                continue
        
        # Process live records
        if live_records:
            logger.debug(f"Processing {len(live_records)} live records")
            self.db_handler.store_live_equity_batch(live_records)
            logger.debug("Successfully stored live equity records")
            
        # Process non-live records
        if non_live_records:
            logger.debug(f"Processing {len(non_live_records)} non-live records")
            self.db_handler.store_equity_batch(non_live_records)
            logger.debug("Successfully stored non-live equity records")
            
        db_end = time.time()
        logger.info(f"Database operations took {db_end - db_start:.2f} seconds for {len(equity_records_data)} equity records")
        logger.debug(f"Processed {len(live_records)} live records and {len(non_live_records)} non-live records")
    
    def _save_and_cleanup(self, file_path: str, output_file: str, records: List[Dict],
                         timestamps: Set[float]) -> Tuple[str, Set[float]]:
        """
        Save records to Parquet and clean up processed files.
        
        Args:
            file_path: Original JSNL file path
            output_file: Output Parquet file path
            records: List of records to save
            timestamps: Set of timestamps
            
        Returns:
            Tuple of (output file path, timestamps set)
        """
        parquet_start = time.time()
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with optimizations
        pq.write_table(
            table, 
            output_file,
            compression='snappy',
            use_dictionary=True,
            write_statistics=True
        )
        parquet_end = time.time()
        logger.info(f"Parquet file creation took {parquet_end - parquet_start:.2f} seconds")
        
        logger.info(f"Saved Parquet file with optimizations for time range and ID queries: {output_file}")
        logger.info(f"Created Parquet file: {output_file} with {len(records)} records")
        
        # Move the original file to processed directory
        self._move_to_processed(file_path)
        
        return output_file, timestamps
    
    def _move_to_processed(self, file_path: str) -> None:
        """
        Move a processed file to the processed directory.
        
        Args:
            file_path: Path to the file to move
        """
        if self.config.get('leave_source_files', False): # If leave_source_files is True, don't move the file
            return

        if os.path.dirname(file_path) == self.config['input_dir'] and os.path.exists(file_path):
            processed_path = os.path.join(self.config['processed_dir'], os.path.basename(file_path))
            try:
                shutil.move(file_path, processed_path)
                logger.info(f"Moved processed file to {processed_path}")
            except (shutil.Error, OSError) as e:
                logger.warning(f"Could not move file {file_path} to {processed_path}: {str(e)}")
    
    def _log_processing_stats(self, file_path: str, stats: Dict[str, int]) -> None:
        """
        Log processing statistics for a file.
        
        Args:
            file_path: Path to the processed file
            stats: Statistics dictionary
        """
        logger.info(f"File processing statistics for {file_path}:")
        logger.info(f"  Total records: {stats['total_records']}")
        logger.info(f"  Invalid records: {stats['invalid_records']}")
        logger.info(f"  Missing required fields: {stats['missing_fields']}")
        logger.info(f"  Skipped (candle/equity/trade only): {stats['skipped_db_only_records']}")
        logger.info(f"  Skipped equity records: {stats['skipped_equity_records']}")
        logger.info(f"  Successfully processed: {stats['total_records'] - stats['invalid_records'] - stats['missing_fields'] - stats['skipped_db_only_records']}")
        logger.info(f"  Equity records: {stats['equity_records']}")
        logger.info(f"  Trade records: {stats['trade_records']}")
    
    def process_value_item(self, value_item, trades, others):
        """
        Process a single value item from a JSNL record.
        
        Args:
            value_item: The value item to process
            trades: List to store trade records
            others: List to store other records
        Returns:
            equity record or None
        """
        # Check if the item has a type field
        record_type = value_item.get('t')
        
        if not record_type:
            # Skip items without a type
            return None

        if record_type == 'e':  # Equity record
            try:
                # Get current equity value and ensure it's a float
                current_equity = float(value_item.get('equity', 0.0))
                broker = value_item.get('b', 'unknown')
                
                new_equity_record = {
                    'value': {
                        't': 'e',
                        'equity': current_equity,
                        'b': broker
                    }
                }

                return new_equity_record
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid equity value in record: {value_item.get('equity')} - {str(e)}")
                return None
            
        elif record_type == 'c':  # Candle record
            # Just add to others list
            others.append(value_item)
            return None
        
        elif record_type in ['open', 'close', 'adjust-open', 'adjust-close']:  # Trade record
            trade_data = {
                'type': record_type,
                'instrument': value_item.get('instrument', ''),
                'price': float(value_item.get('price', 0.0)),
                'units': float(value_item.get('units', 0.0)),
                'profit': float(value_item.get('profit', 0.0)),
                'id': value_item.get('id', '')
            }

            trades.append(trade_data)
            return None
        
        else:
            others.append(value_item)
            return None
    
    def process_component_started(self, message: dict) -> bool:
        """
        Process a component_started message.
        
        Args:
            message: The parsed component_started message
            
        Returns:
            bool: True if processing succeeded, False otherwise
        """
        try:
            # Extract required fields
            mode = message.get('mode', 'unknown')
            config = message.get('config', {})
            config_path = message.get('config_file_path', '')
            component = message.get('component', 'unknown')
            tuning_date_range = config.get('tuning_date_range', '')
            live = message.get('live', False) 
            is_live = live if isinstance(live, bool) else live.lower() == 'true'
            
            designator = f'{component}_simulation'

            if is_live:
                designator = f'{component}_live'
            
            # Get strategy name from config
            strategy_name = config.get('name', 'Unnamed Strategy')

            # Add file name and processing timestamp to config
            config['source_file'] = self.current_file
            config['config_path'] = config_path
            config['designator'] = designator
            config['tuning_date_range'] = tuning_date_range
            config['live'] = is_live
            
            logger.info(f"Processing component_started message: {mode}, {config}, {strategy_name}")
            
            # Store in cache and database
            try:
                self.mode_cache.add_config(mode, config)
            except Exception as e:
                logger.error(f"Failed to store config in cache/database: {str(e)}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error processing component_started message: {str(e)}")
            return False
    
    def process_jsnl_line(self, line: str) -> bool:
        """Process a single line from the JSNL file."""
        # logger.info(f"Processing line length: {len(line)}")
        # Skip empty lines
        if not line.strip():
            return False

        try:
            message = json.loads(line)
            message_type = message.get('type')
            
            if message_type in ['ensemble_started', 'strand_started']:
                return self.process_component_started(message)
            
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON line: {line}")
            return False
        except Exception as e:
            logger.error(f"Error processing line: {str(e)}")
            return False
    
    def create_weekly_files(self, min_timestamp: float, max_timestamp: float) -> None:
        """
        Create weekly Parquet files from temp files, organized by mode.
        
        Args:
            min_timestamp: Minimum timestamp in the data
            max_timestamp: Maximum timestamp in the data
        """
        # Convert timestamps to datetime
        min_dt = datetime.fromtimestamp(min_timestamp)
        max_dt = datetime.fromtimestamp(max_timestamp)
        
        logger.info(f"Creating weekly files for date range: {min_dt} to {max_dt}")
        
        # Get the start of the week for min_dt (Monday)
        start_of_week = min_dt - timedelta(days=min_dt.weekday())
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get the end of the week for max_dt
        end_of_week = max_dt + timedelta(days= 7 - max_dt.weekday())
        end_of_week = end_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"Processing weeks from {start_of_week} to {end_of_week}")
        
        # Find all temp files
        temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
        
        # First, group files by mode
        mode_groups = {}
        
        # Read each temp file to get unique modes
        for temp_file in temp_files:
            try:
                table = pq.read_table(temp_file, columns=['mode'])
                df = table.to_pandas()
                unique_modes = df['mode'].unique()
                
                # Add file to each mode's group
                for mode in unique_modes:
                    if mode not in mode_groups:
                        mode_groups[mode] = []
                    mode_groups[mode].append(temp_file)
                
            except Exception as e:
                logger.error(f"Error reading temp file {temp_file}: {str(e)}")
        
        logger.info(f"Found {len(mode_groups)} unique modes")
        
        # Process each mode separately
        for mode, mode_temp_files in mode_groups.items():
            logger.info(f"Processing files for mode: {mode}")
            
            # Create weekly files for this mode
            current_week = start_of_week
            while current_week < end_of_week:
                week_start_time = time.time()
                next_week = current_week + timedelta(days=7)
                
                logger.info(f"Processing week for {mode}: {current_week} to {next_week}")
                
                # Create mode-specific weekly file path
                weekly_file = os.path.join(
                    self.config['output_dir'],
                    str(mode),  # Create subdirectory for this mode
                    'weekly',
                    f"weekly_{current_week.strftime('%Y%m%d')}.parquet"
                )
                
                # Ensure the directory exists
                os.makedirs(os.path.dirname(weekly_file), exist_ok=True)
                
                # Filter files by timestamp range for this week
                week_start_ts = current_week.timestamp()
                week_end_ts = next_week.timestamp()
                
                filtered_temp_files = []
                has_new_data = False
                
                for temp_file in mode_temp_files:
                    try:
                        # Read the Parquet file
                        table = pq.read_table(temp_file, columns=['timestamp', 'mode'])
                        df = table.to_pandas()
                        
                        # Filter for this mode and timestamp range
                        df = df[
                            (df['mode'] == mode) & 
                            (df['timestamp'] >= week_start_ts) & 
                            (df['timestamp'] < week_end_ts)
                        ]
                        
                        if not df.empty:
                            filtered_temp_files.append(temp_file)
                            has_new_data = True
                            
                    except Exception as e:
                        logger.error(f"Error reading temp file {temp_file}: {str(e)}")
                
                # Only process if we have new data
                if has_new_data:
                    try:
                        merge_start = time.time()
                        # Create a DuckDB query to merge the files
                        files_array = "[" + ", ".join(f"'{file}'" for file in filtered_temp_files) + "]"
                        
                        # Create a temporary table for the merged data
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE merged AS
                            SELECT * FROM parquet_scan({files_array})
                            WHERE timestamp >= {week_start_ts} 
                            AND timestamp < {week_end_ts}
                            AND mode = '{mode}'
                            ORDER BY timestamp
                        """)
                        
                        # Check if there are any records in this period
                        result = self.conn.execute("SELECT COUNT(*) FROM merged").fetchone()
                        record_count = result[0] if result else 0
                        
                        if record_count > 0:
                            # Write the merged data to a Parquet file
                            self.conn.execute(f"""
                                COPY merged TO '{weekly_file}' (FORMAT 'parquet')
                            """)
                            merge_end = time.time()
                            logger.info(f"Merged {record_count} records into weekly file for {mode} in {merge_end - merge_start:.2f} seconds: {weekly_file}")
                        else:
                            logger.info(f"No records found for {mode} in period {current_week} to {next_week}, skipping")
                    
                    except Exception as e:
                        logger.error(f"Failed to create weekly file for {mode} period {current_week}: {str(e)}")
                        logger.error(traceback.format_exc())
                else:
                    logger.info(f"No new data for {mode} in period {current_week} to {next_week}, skipping")
                
                week_end_time = time.time()
                logger.info(f"Processing week {current_week} to {next_week} for {mode} took {week_end_time - week_start_time:.2f} seconds")
                
                # Move to next week
                current_week = next_week
    
    def sort_files_by_timestamp(self, jsnl_files: List[str]) -> List[str]:
        """
        Sort files by timestamp in filename.
        
        Args:
            jsnl_files: List of file paths to sort
            
        Returns:
            Sorted list of file paths
        """
        # Assuming filenames are in format: dashboard_data_YYYYMMDD_HHMMSS.jsnl
        def extract_timestamp(filename):
            try:
                # Extract date and time parts from filename
                base_name = os.path.basename(filename)
                parts = base_name.split('_')
                if len(parts) >= 4:
                    date_part = parts[2]  # YYYYMMDD
                    time_part = parts[3].split('.')[0]  # HHMMSS
                    
                    # Parse date and time
                    if len(date_part) == 8 and len(time_part) == 6:
                        dt_str = f"{date_part}_{time_part}"
                        dt = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
                        return dt.timestamp()
                
                # If we can't parse the timestamp, use file modification time as fallback
                return os.path.getmtime(filename)
            except Exception as e:
                logger.warning(f"Could not extract timestamp from filename {filename}: {str(e)}")
                return os.path.getmtime(filename)
        
        # Sort files by extracted timestamp
        sorted_files = sorted(jsnl_files, key=extract_timestamp)
        logger.info(f"Sorted {len(sorted_files)} files by timestamp in filename")
        
        return sorted_files
    
    def run(self) -> None:
        """Main entry point for the processor."""
        try:
            logger.info("Starting JSNL processor")
            self.db_handler.init_pool()
            self.init_duckdb()
            
            # Process JSNL files
            generated_files = self.process_jsnl_files()
            logger.info(f"Generated {len(generated_files)} Parquet files")
            
            # Get min and max timestamps from generated files
            min_timestamp = float('inf')
            max_timestamp = float('-inf')
            
            for file in generated_files:
                try:
                    table = pq.read_table(file, columns=['timestamp'])
                    df = table.to_pandas()
                    if not df.empty:
                        min_timestamp = min(min_timestamp, df['timestamp'].min())
                        max_timestamp = max(max_timestamp, df['timestamp'].max())
                except Exception as e:
                    logger.error(f"Error reading timestamps from {file}: {str(e)}")
            
            if min_timestamp != float('inf') and max_timestamp != float('-inf'):
                logger.info(f"Creating weekly files for timestamp range: {min_timestamp} to {max_timestamp}")
                self.create_weekly_files(min_timestamp, max_timestamp)
            else:
                logger.info("No timestamp range found in generated files, skipping weekly file creation")
            
            # Clean up old temp files
            self.cleanup_old_temp_files()
            
            logger.info("JSNL processor completed successfully")
            
        except Exception as e:
            logger.error(f"Error in JSNL processor: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # Clean up database connections
            if self.db_handler:
                self.db_handler.drop_pool()
            if self.conn:
                self.close_duckdb()
                
    def cleanup_old_temp_files(self) -> None:
        """
        Clean up all temporary Parquet files at the end of a run.

        This method removes all temporary files in the temp directory.
        """
        try:
            # Get all temp files
            temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
            
            if not temp_files:
                logger.info("No temporary files to clean up")
                return
            
            # Count of removed files
            removed_count = 0
            
            # Remove each file
            for temp_file in temp_files:
                try:
                    os.remove(temp_file)
                    logger.info(f"Removed temp file: {temp_file}")
                    removed_count += 1
                except OSError as e:
                    logger.error(f"Failed to remove temp file {temp_file}: {str(e)}")
            
            logger.info(f"Cleaned up {removed_count} temporary files")
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {str(e)}")
            logger.error(traceback.format_exc())
