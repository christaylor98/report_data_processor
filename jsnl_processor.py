#!/usr/bin/env python3
"""
JSNL to Parquet Processor

This script processes JSNL files into Parquet format using DuckDB.
It also extracts trade and equity data to MariaDB.
"""
#pylint: disable=W1203, W0718, C0301, C0303
import json
import os
import sys
import logging
# import time
import glob
import shutil
import hashlib
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import traceback
from typing import List, Dict, Any, Optional, Tuple, Set
# import time

from dotenv import load_dotenv
import mysql.connector
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

load_dotenv()

# Base directories
BASE_DATA_DIR = '/data'
PROCESS_DIR = f'{BASE_DATA_DIR}/to_process'
PROCESSED_DIR = f'{BASE_DATA_DIR}/processed'
PARQUET_DIR = f'{BASE_DATA_DIR}/parquet'
LOG_DIR = 'log'

# Subdirectories
DASHBOARD_ARCHIVE_DIR = 'dashboard_data_archive'
PARQUET_TEMP_DIR = 'temp'
PARQUET_HOURLY_DIR = 'hourly'
PARQUET_DAILY_DIR = 'daily'
PARQUET_MONTHLY_DIR = 'monthly'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'jsnl_processor.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('jsnl_processor')

# Configuration
CONFIG = {
    'input_dir': os.path.join(PROCESS_DIR, DASHBOARD_ARCHIVE_DIR),
    'processed_dir': os.path.join(PROCESSED_DIR, DASHBOARD_ARCHIVE_DIR),
    'output_dir': PARQUET_DIR,
    'temp_dir': os.path.join(PARQUET_DIR, PARQUET_TEMP_DIR),
    'processing_interval_minutes': 5,
    'merge_intervals': {
        'hourly': 60,  # minutes
        'daily': 1440,  # minutes (24 hours)
        'monthly': 43200  # minutes (30 days)
    }
}

# Ensure all directories exist
for dir_path in [CONFIG['input_dir'], CONFIG['processed_dir'], CONFIG['output_dir'], CONFIG['temp_dir']]:
    os.makedirs(dir_path, exist_ok=True)


class DatabaseHandler:
    """Handles database operations for JSNL data."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        self.cursor = None
        
    def connect(self) -> None:
        """Connect to the database."""
        try:
            # Check if db_config exists in the configuration
            if 'db_config' not in self.config:
                logger.warning("Database configuration missing. Using environment variables.")
                # Use environment variables as fallback
                db_config = {
                    'host': os.getenv('DB_HOST', 'localhost'),
                    'port': os.getenv('DB_PORT', '3306'),
                    'user': os.getenv('DB_USER', 'root'),
                    'password': os.getenv('DB_PASSWORD', ''),
                    'database': os.getenv('DB_NAME', 'trading')
                }
            else:
                db_config = self.config['db_config']
            
            self.conn = mysql.connector.connect(**db_config)
            self.cursor = self.conn.cursor()
            logger.info("Connected to database")
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
            
    def disconnect(self) -> None:
        """Disconnect from the database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
            
    def store_equity(self, record: Dict[str, Any]) -> None:
        """
        Store equity record in the database.
        
        Args:
            record: Dictionary containing equity record data
        """
        if not self.conn or not self.cursor:
            logger.error("Database connection not established")
            return
        
        try:
            # Extract candle data if present
            candle_data = None
            if 'value' in record and isinstance(record['value'], dict):
                if 'candle' in record['value']:
                    candle_data = json.dumps(record['value']['candle'])
            
            # Prepare SQL query
            query = """
                INSERT INTO dashboard_equity 
                (id, timestamp, mode, equity, candle) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                equity = VALUES(equity),
                candle = VALUES(candle)
            """
            
            # Extract broker name (mode)
            _broker = record.get('value', {}).get('b', 'unknown')
            
            # Execute query
            self.cursor.execute(
                query, 
                (
                    record['log_id'],
                    record['timestamp'],
                    record['mode'],
                    record['value'].get('equity', 0.0),
                    candle_data
                )
            )
            self.conn.commit()
            
        except mysql.connector.Error as e:
            logger.error(f"Error storing equity record: {str(e)}")
            logger.error(f"Record: {record}")
            self.conn.rollback()
        except Exception as e:
            logger.error(f"Unexpected error storing equity record: {str(e)}")
            logger.error(f"Record: {record}")
            logger.error(traceback.format_exc())
            self.conn.rollback()
            
    def store_trade(self, data: Dict[str, Any]) -> None:
        """Store trade data in the database."""
        if not self.conn or not self.cursor:
            logger.error("Database connection not established")
            return
        
        try:
            # Log the trade data being stored
            logger.debug(f"Storing trade data: id={data['log_id']}, timestamp={data['timestamp']}, type={data['type']}")
            
            # Create JSON data for the dashboard_data table
            json_data = json.dumps({
                "instrument": data['instrument'],
                "price": data['price'],
                "profit": data['profit'],
                "t": data['type']
            })
            
            # Check if a record with this timestamp, id, and type already exists
            check_query = """
            SELECT id FROM trading.dashboard_data 
            WHERE id = %s AND timestamp = %s AND mode = %s AND data_type = %s
            """
            
            self.cursor.execute(
                check_query, 
                (
                    data['log_id'],
                    data['timestamp'],
                    data['mode'],
                    data['type']
                )
            )
            
            existing = self.cursor.fetchone()
            
            if existing:
                # Update existing record
                update_query = """
                UPDATE trading.dashboard_data 
                SET json_data = %s
                WHERE id = %s AND timestamp = %s AND mode = %s AND data_type = %s
                """
                
                self.cursor.execute(
                    update_query, 
                    (
                        json_data,
                        data['log_id'],
                        data['timestamp'],
                        data['mode'],
                        data['type']
                    )
                )
            else:
                # Insert new record
                insert_query = """
                INSERT INTO trading.dashboard_data 
                (id, timestamp, mode, data_type, json_data) 
                VALUES (%s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(
                    insert_query, 
                    (
                        data['log_id'],
                        data['timestamp'],
                        data['mode'],
                        data['type'],
                        json_data
                    )
                )
            
            self.conn.commit()
            logger.debug(f"Stored trade data for {data['log_id']} at {data['timestamp']}")
        except Exception as e:
            logger.error(f"Failed to store trade data: {str(e)}")
            self.conn.rollback()


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
        
        # Initialize database handler
        self.db_handler = DatabaseHandler(config)
        
        # Initialize DuckDB connection
        self.conn = None
        
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
        # Find all JSNL files in the input directory
        jsnl_files = glob.glob(os.path.join(self.config['input_dir'], '*.jsnl'))
        
        if not jsnl_files:
            logger.info("No JSNL files found in input directory")
            return []
        
        # Sort files by timestamp in filename (if available)
        sorted_files = self.sort_files_by_timestamp(jsnl_files)
        
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
            try:
                logger.info(f"Processing file in order: {os.path.basename(jsnl_file)}")
                
                # Check if file still exists before processing
                if not os.path.exists(jsnl_file):
                    logger.warning(f"File no longer exists, skipping: {jsnl_file}")
                    continue
                    
                # Process the file
                result = self.process_single_file(jsnl_file)
                
                if result[0]:  # If a Parquet file was generated
                    generated_files.append(result[0])
                    timestamps.update(result[1])
                
                # Move the file to the processed directory
                processed_path = os.path.join(self.config['processed_dir'], os.path.basename(jsnl_file))
                
                # Check if file still exists before moving
                if os.path.exists(jsnl_file):
                    try:
                        shutil.move(jsnl_file, processed_path)
                        logger.info(f"Moved processed file to {processed_path}")
                    except (shutil.Error, OSError) as e:
                        logger.warning(f"Could not move file {jsnl_file} to {processed_path}: {str(e)}")
                else:
                    logger.warning(f"File no longer exists, cannot move: {jsnl_file}")
                    
            except Exception as e:
                logger.error(f"Error processing file {jsnl_file}: {str(e)}")
                logger.error(traceback.format_exc())
        
        # If we have timestamps, log the range
        if timestamps:
            min_ts = min(timestamps)
            max_ts = max(timestamps)
            min_dt = datetime.fromtimestamp(min_ts)
            max_dt = datetime.fromtimestamp(max_ts)
            logger.info(f"Data timestamps range from {min_dt} to {max_dt}")
        
        # Create weekly Parquet files from temp files
        logger.info("Creating weekly Parquet files")
        if timestamps:
            self.create_weekly_files(min_ts, max_ts)
        
        return generated_files
    
    def process_single_file(self, file_path: str) -> Tuple[Optional[str], Set[float]]:
        """
        Process a single JSNL file and convert it to Parquet format.
        
        Args:
            file_path: Path to the JSNL file
            
        Returns:
            Tuple of (generated Parquet file path, set of timestamps)
        """
        logger.info(f"Processing file {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.warning(f"File does not exist: {file_path}")
            return None, set()
        
        # Generate a unique ID for this file based on content
        file_id = self.get_file_id(file_path)
        
        # Create output filename
        base_name = os.path.basename(file_path)
        output_file = os.path.join(self.config['temp_dir'], f"{os.path.splitext(base_name)[0]}_{file_id}.parquet")
        
        # Check if file already exists (idempotent)
        if os.path.exists(output_file):
            logger.info(f"File {output_file} already exists, skipping processing")
            
            # Move the original file to processed directory if needed
            if os.path.dirname(file_path) == self.config['input_dir'] and os.path.exists(file_path):
                processed_path = os.path.join(self.config['processed_dir'], os.path.basename(file_path))
                try:
                    shutil.move(file_path, processed_path)
                    logger.info(f"Moved processed file to {processed_path}")
                except (shutil.Error, OSError) as e:
                    logger.warning(f"Could not move file {file_path} to {processed_path}: {str(e)}")
            
            # Return the existing file path and an empty set of timestamps
            return output_file, set()
        
        # Process the file
        records = []
        timestamps = set()
        
        # Statistics
        total_records = 0
        invalid_records = 0
        missing_fields = 0
        equity_records = 0
        trade_records = 0
        skipped_no_candle = 0
        
        # Temporary storage for candles by log_id and timestamp
        candles = {}  # Format: {(log_id, timestamp): candle_data}
        equity_records_data = {}  # Format: {(log_id, timestamp): equity_record}
        
        try:
            # Check again if file exists before opening
            if not os.path.exists(file_path):
                logger.warning(f"File disappeared during processing: {file_path}")
                return None, set()
            
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    total_records += 1
                    
                    try:
                        # Parse JSON
                        data = json.loads(line.strip())
                        
                        # Extract required fields
                        log_id = data.get('log_id')
                        timestamp = data.get('timestamp')
                        mode = data.get('type')
                        component = data.get('component')
                        value_obj = data.get('value', {})
                        
                        if not log_id or not timestamp or not value_obj or not mode or not component:
                            missing_fields += 1
                            continue
                        
                        # Convert timestamp to float if it's not already
                        if isinstance(timestamp, str):
                            timestamp = float(timestamp)
                        
                        # Check if this line contains a candle record
                        has_candle = False
                        
                        # Handle both single objects and arrays
                        if isinstance(value_obj, list):
                            for item in value_obj:
                                if item.get('t') == 'c':
                                    has_candle = True
                                    break
                        else:
                            has_candle = value_obj.get('t') == 'c'
                        
                        # Skip lines without candles
                        if not has_candle:
                            skipped_no_candle += 1
                            continue
                        
                        # Add timestamp to set for later use
                        timestamps.add(timestamp)
                        
                        # Create record with common fields
                        record = {
                            'log_id': log_id,
                            'timestamp': timestamp,
                            'mode': mode,
                            'component': component,
                            'filename': os.path.basename(file_path),
                            'data': line.strip(),
                            'date': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                        }
                        
                        # Add to records list
                        records.append(record)
                        
                        # Process by record type - handle both single objects and arrays
                        if isinstance(value_obj, list):
                            # Process array of records
                            for item in value_obj:
                                equity_processed, trade_processed = self._process_value_item(
                                    log_id, timestamp, mode, component, item, equity_records_data, candles
                                )
                                if equity_processed:
                                    equity_records += 1
                                if trade_processed:
                                    trade_records += 1
                        else:
                            # Process single record
                            equity_processed, trade_processed = self._process_value_item(
                                log_id, timestamp, mode, component, value_obj, equity_records_data, candles
                            )
                            if equity_processed:
                                equity_records += 1
                            if trade_processed:
                                trade_records += 1
                        
                    except json.JSONDecodeError as e:
                        invalid_records += 1
                        logger.error(f"Invalid JSON in file {file_path}: {str(e)}")
                    except Exception as e:
                        invalid_records += 1
                        logger.error(f"Error processing record in file {file_path}: {str(e)}")
                
                # Now process equity records with associated candles
                for key, equity_record in equity_records_data.items():
                    # Check if we have a candle for this equity record
                    if key in candles:
                        # Add candle data to the equity record
                        equity_record['value']['candle'] = candles[key]
                    
                    # Store in database
                    self.db_handler.store_equity(equity_record)
                
                # Log statistics
                logger.info(f"File processing statistics for {file_path}:")
                logger.info(f"  Total records: {total_records}")
                logger.info(f"  Invalid records: {invalid_records}")
                logger.info(f"  Missing required fields: {missing_fields}")
                logger.info(f"  Skipped (no candle): {skipped_no_candle}")
                logger.info(f"  Successfully processed: {total_records - invalid_records - missing_fields - skipped_no_candle}")
                logger.info(f"  Equity records: {equity_records}")
                logger.info(f"  Trade records: {trade_records}")
                
                # If we have valid records, save to Parquet
                if records:
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
                    
                    logger.info(f"Saved Parquet file with optimizations for time range and ID queries: {output_file}")
                    logger.info(f"Created Parquet file: {output_file} with {len(records)} records (Equity: {equity_records}, Trade: {trade_records})")
                    
                    # Move the original file to processed directory
                    if os.path.dirname(file_path) == self.config['input_dir']:
                        processed_path = os.path.join(self.config['processed_dir'], os.path.basename(file_path))
                        shutil.move(file_path, processed_path)
                        logger.info(f"Moved processed file to {processed_path}")
                    
                    return output_file, timestamps
                else:
                    logger.warning(f"No valid records found in {file_path}, no Parquet file created")
                    return None, set()
                
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {str(e)}")
            logger.error(traceback.format_exc())
            return None, set()
    
    def _process_value_item(self, log_id, timestamp, mode, component, value_item, equity_records_data, candles):
        """
        Process a single value item from a JSNL record.
        
        Args:
            log_id: The log ID of the record
            timestamp: The timestamp of the record
            mode: The mode of the record
            component: The component of the record
            value_item: The value item to process
            equity_records_data: Dictionary to store equity records
            candles: Dictionary to store candle data
            
        Returns:
            Tuple of (equity_record_processed, trade_record_processed) booleans
        """
        # Check if the item has a type field
        record_type = value_item.get('t')
        
        if not record_type:
            # Skip items without a type
            return False, False
        
        equity_processed = False
        trade_processed = False
        
        if record_type == 'e':  # Equity record
            equity_processed = True
            
            # Store equity record for later processing (after we find candles)
            # Use the broker field if available, otherwise use a default
            broker = value_item.get('b', 'unknown')
            
            equity_records_data[(log_id, timestamp)] = {
                'log_id': log_id,
                'timestamp': timestamp,
                'mode': mode,
                'component': component,
                'value': {
                    't': 'e',
                    'equity': value_item.get('equity', 0.0),
                    'b': broker
                }
            }
            
        elif record_type == 'c':  # Candle record
            # Store candle data for later association with equity records
            candles[(log_id, timestamp)] = value_item.get('candle', value_item)
            
        elif record_type in ['open', 'close', 'adjust-open', 'adjust-close']:  # Trade record
            trade_processed = True
            
            # Extract trade data - handle both formats
            trade_data = {
                'log_id': log_id,
                'timestamp': timestamp,
                'type': record_type,
                'mode': mode,
                'component': component,
                'instrument': value_item.get('instrument', ''),
                'price': value_item.get('price', 0.0),
                'profit': value_item.get('profit', 0.0)
            }
            
            # Store in database
            self.db_handler.store_trade(trade_data)
        
        return equity_processed, trade_processed
    
    def create_weekly_files(self, min_timestamp: float, max_timestamp: float) -> None:
        """
        Create weekly Parquet files from temp files.
        
        Args:
            min_timestamp: Minimum timestamp in the data
            max_timestamp: Maximum timestamp in the data
        """
        # Convert timestamps to datetime
        min_dt = datetime.fromtimestamp(min_timestamp)
        max_dt = datetime.fromtimestamp(max_timestamp)
        
        # Get the start of the week for min_dt (Monday)
        start_of_week = min_dt - timedelta(days=min_dt.weekday())
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get the start of the next week for max_dt
        end_of_week = max_dt + timedelta(days=(7 - max_dt.weekday()) % 7)
        end_of_week = end_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Create weekly files
        current_week = start_of_week
        while current_week < end_of_week:
            next_week = current_week + timedelta(days=7)
            
            # Create weekly file name
            weekly_file = os.path.join(
                self.config['output_dir'], 
                'weekly', 
                f"weekly_{current_week.strftime('%Y%m%d')}.parquet"
            )
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(weekly_file), exist_ok=True)
            
            # Find temp files in this date range
            temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
            
            # Filter files by timestamp range
            filtered_temp_files = []
            for temp_file in temp_files:
                try:
                    # Read the Parquet file to get timestamps
                    table = pq.read_table(temp_file)
                    df = table.to_pandas()
                    
                    # Check if any timestamps fall within this week
                    if 'timestamp' in df.columns:
                        timestamps = df['timestamp'].values
                        if any((ts >= current_week.timestamp() and ts < next_week.timestamp()) for ts in timestamps):
                            filtered_temp_files.append(temp_file)
                except Exception as e:
                    logger.error(f"Error reading temp file {temp_file}: {str(e)}")
            
            # If we found temp files, merge them
            if filtered_temp_files:
                try:
                    # Create a DuckDB query to merge the files
                    files_array = "[" + ", ".join(f"'{file}'" for file in filtered_temp_files) + "]"
                    
                    # Create a temporary table for the merged data
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE merged AS
                        SELECT * FROM parquet_scan({files_array})
                        WHERE timestamp >= {current_week.timestamp()} AND timestamp < {next_week.timestamp()}
                        ORDER BY timestamp, log_id
                    """)
                    
                    # Check if there are any records in this period
                    result = self.conn.execute("SELECT COUNT(*) FROM merged").fetchone()
                    if result[0] == 0:
                        logger.info(f"No records found for period {current_week} to {next_week}, skipping")
                        current_week = next_week
                        continue
                    
                    # Write the merged data to a Parquet file
                    self.conn.execute(f"""
                        COPY merged TO '{weekly_file}' (FORMAT 'parquet')
                    """)
                    
                    logger.info(f"Created weekly file for period {current_week} to {next_week}: {weekly_file}")
                    logger.info(f"Merged {len(filtered_temp_files)} temp files")
                    
                    # Remove the temp files that were merged
                    for temp_file in filtered_temp_files:
                        try:
                            os.remove(temp_file)
                            logger.info(f"Removed processed temp file: {temp_file}")
                        except OSError as e:
                            logger.error(f"Failed to remove temp file {temp_file}: {str(e)}")
                except Exception as e:
                    logger.error(f"Failed to create weekly file for period {current_week}: {str(e)}")
                    logger.error(traceback.format_exc())
            else:
                logger.info(f"No temp files found for period {current_week} to {next_week}")
            
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
        """Run the JSNL processing pipeline."""
        try:
            self.db_handler.connect()
            self.init_duckdb()
            
            # Process JSNL files
            generated_files = self.process_jsnl_files()
            logger.info(f"Generated {len(generated_files)} Parquet files")
            
            # Clean up old temp files
            self.cleanup_old_temp_files()
            
        except Exception as e:
            logger.error(f"Error running JSNL processor: {str(e)}")
            logger.error(traceback.format_exc())
            sys.exit(1)
        finally:
            self.db_handler.disconnect()
            self.close_duckdb()

    def cleanup_old_temp_files(self) -> None:
        """
        Clean up old temporary Parquet files that are no longer needed.
        
        This method removes temporary files that are older than a certain threshold
        and have already been merged into weekly files.
        """
        try:
            # Get all temp files
            temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
            
            if not temp_files:
                logger.info("No temporary files to clean up")
                return
            
            # Get current time
            now = datetime.now()
            
            # Define threshold for old files (default: 7 days)
            threshold_days = 7
            threshold = now - timedelta(days=threshold_days)
            
            # Count of removed files
            removed_count = 0
            
            # Check each file
            for temp_file in temp_files:
                try:
                    # Get file modification time
                    mod_time = datetime.fromtimestamp(os.path.getmtime(temp_file))
                    
                    # If file is older than threshold, remove it
                    if mod_time < threshold:
                        os.remove(temp_file)
                        logger.info(f"Removed old temp file: {temp_file}")
                        removed_count += 1
                except OSError as e:
                    logger.error(f"Failed to check or remove temp file {temp_file}: {str(e)}")
            
            logger.info(f"Cleaned up {removed_count} old temporary files")
        except Exception as e:
            logger.error(f"Error cleaning up old temp files: {str(e)}")
            logger.error(traceback.format_exc())


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process JSNL files into Parquet format.')
    parser.add_argument('--file', '-f', help='Process a single specific file')
    parser.add_argument('--limit', '-l', type=int, help='Limit processing to N files')
    return parser.parse_args()


def main():
    """Main entry point for the script."""
    try:
        args = parse_arguments()
        logger.info("Starting JSNL processor")
        
        # Ensure configuration has db_config
        if 'db_config' not in CONFIG:
            CONFIG['db_config'] = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '3306'),
                'user': os.getenv('DB_USER', 'root'),
                'password': os.getenv('DB_PASSWORD', ''),
                'database': os.getenv('DB_NAME', 'trading')
            }
        
        # Create processor with file limit if specified
        processor = JSNLProcessor(CONFIG, max_files=args.limit)
        
        # Process a single file if specified
        if args.file:
            if not os.path.exists(args.file):
                logger.error(f"File not found: {args.file}")
                return 1
                
            logger.info(f"Processing single file: {args.file}")
            # If the file is not in the input directory, copy it there
            if not args.file.startswith(processor.config['input_dir']):
                target_path = os.path.join(processor.config['input_dir'], os.path.basename(args.file))
                shutil.copy(args.file, target_path)
                logger.info(f"Copied file to input directory: {target_path}")
                processor.process_single_file(target_path)
            else:
                processor.process_single_file(args.file)
        else:
            # Normal processing
            processor.run()
            
        logger.info("JSNL processor completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Unhandled exception in JSNL processor: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
