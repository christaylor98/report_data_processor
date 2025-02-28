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
import time

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
    'db_config': {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    },
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
            self.conn = mysql.connector.connect(**self.config['db_config'])
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
            broker = record.get('value', {}).get('b', 'unknown')
            
            # Execute query
            self.cursor.execute(
                query, 
                (
                    record['log_id'],
                    record['timestamp'],
                    broker,
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
            
            # Check if a record with this timestamp, id, and type already exists
            check_query = """
            SELECT id FROM trading.dashboard_trades 
            WHERE id = %s AND timestamp = %s AND type = %s
            """
            
            self.cursor.execute(
                check_query, 
                (
                    data['log_id'],
                    data['timestamp'],
                    data['type']
                )
            )
            
            existing = self.cursor.fetchone()
            
            if existing:
                # Update existing record
                update_query = """
                UPDATE trading.dashboard_trades 
                SET instrument = %s, price = %s, profit = %s
                WHERE id = %s AND timestamp = %s AND type = %s
                """
                
                self.cursor.execute(
                    update_query, 
                    (
                        data['instrument'],
                        data['price'],
                        data['profit'],
                        data['log_id'],
                        data['timestamp'],
                        data['type']
                    )
                )
            else:
                # Insert new record
                insert_query = """
                INSERT INTO trading.dashboard_trades 
                (id, timestamp, type, instrument, price, profit) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(
                    insert_query, 
                    (
                        data['log_id'],
                        data['timestamp'],
                        data['type'],
                        data['instrument'],
                        data['price'],
                        data['profit']
                    )
                )
            
            self.conn.commit()
            logger.debug(f"Stored trade data for {data['log_id']} at {data['timestamp']}")
        except Exception as e:
            logger.error(f"Failed to store trade data: {str(e)}")
            self.conn.rollback()


class JSNLProcessor:
    """Processes JSNL files into Parquet format and extracts data to MariaDB."""
    
    def __init__(self, config: Dict[str, Any], max_files: Optional[int] = None):
        self.config = config
        self.db_handler = DatabaseHandler(config['db_config'])
        self.conn = None  # DuckDB connection
        self.max_files = max_files  # Maximum number of files to process
        
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
        Process all JSNL files in the input directory in chronological order.
        Returns a list of generated Parquet files.
        """
        jsnl_files = glob.glob(os.path.join(self.config['input_dir'], '*.jsnl'))
        if not jsnl_files:
            logger.info("No JSNL files found to process")
            return []
        
        # Sort files by timestamp in filename
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
        jsnl_files.sort(key=extract_timestamp)
        logger.info(f"Sorted {len(jsnl_files)} files by timestamp in filename")
        
        # Limit the number of files if max_files is specified
        if self.max_files is not None:
            jsnl_files = jsnl_files[:self.max_files]
            logger.info(f"Processing limited to {self.max_files} files")
        
        logger.info(f"Found {len(jsnl_files)} JSNL files to process")
        generated_files = []
        data_timestamps = set()  # Track timestamps from the data
        
        for jsnl_file in jsnl_files:
            try:
                logger.info(f"Processing file in order: {os.path.basename(jsnl_file)}")
                output_file, timestamps = self.process_single_file(jsnl_file)
                if output_file:
                    generated_files.append(output_file)
                    data_timestamps.update(timestamps)
                    
                # Move processed file
                processed_path = os.path.join(self.config['processed_dir'], os.path.basename(jsnl_file))
                shutil.move(jsnl_file, processed_path)
                logger.info(f"Moved processed file to {processed_path}")
                
            except Exception as e:
                logger.error(f"Error processing file {jsnl_file}: {str(e)}")
                logger.error(traceback.format_exc())
                
        # Perform weekly merge based on data timestamps
        if data_timestamps:
            min_ts = min(data_timestamps)
            max_ts = max(data_timestamps)
            logger.info(f"Data timestamps range from {datetime.fromtimestamp(min_ts)} to {datetime.fromtimestamp(max_ts)}")
            
            # Create weekly files
            logger.info("Creating weekly Parquet files")
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
        
        # Generate a unique ID for this file based on content
        file_id = self.get_file_id(file_path)
        
        # Create output filename
        base_name = os.path.basename(file_path)
        output_file = os.path.join(self.config['temp_dir'], f"{os.path.splitext(base_name)[0]}_{file_id}.parquet")
        
        # Check if file already exists (idempotent)
        if os.path.exists(output_file):
            logger.info(f"File {output_file} already exists, skipping processing")
            
            # Move the original file to processed directory if needed
            if os.path.dirname(file_path) == self.config['input_dir']:
                processed_path = os.path.join(self.config['processed_dir'], os.path.basename(file_path))
                shutil.move(file_path, processed_path)
                logger.info(f"Moved processed file to {processed_path}")
            
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
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    total_records += 1
                    
                    try:
                        # Parse JSON
                        data = json.loads(line.strip())
                        
                        # Extract required fields
                        log_id = data.get('log_id')
                        timestamp = data.get('timestamp')
                        value_obj = data.get('value', {})
                        
                        if not log_id or not timestamp or not value_obj:
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
                            has_candle = (value_obj.get('t') == 'c')
                        
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
                                self._process_value_item(log_id, timestamp, item, equity_records_data, candles, equity_records, trade_records)
                        else:
                            # Process single record
                            self._process_value_item(log_id, timestamp, value_obj, equity_records_data, candles, equity_records, trade_records)
                        
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
    
    def _process_value_item(self, log_id, timestamp, value_item, equity_records_data, candles, equity_records_count, trade_records_count):
        """
        Process a single value item from a JSNL record.
        
        Args:
            log_id: The log ID of the record
            timestamp: The timestamp of the record
            value_item: The value item to process
            equity_records_data: Dictionary to store equity records
            candles: Dictionary to store candle data
            equity_records_count: Counter for equity records
            trade_records_count: Counter for trade records
        """
        # Check if the item has a type field
        record_type = value_item.get('t')
        
        if not record_type:
            # Skip items without a type
            return
        
        if record_type == 'e':  # Equity record
            equity_records_count += 1
            
            # Store equity record for later processing (after we find candles)
            # Use the broker field if available, otherwise use a default
            broker = value_item.get('b', 'unknown')
            
            equity_records_data[(log_id, timestamp)] = {
                'log_id': log_id,
                'timestamp': timestamp,
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
            trade_records_count += 1
            
            # Extract trade data - handle both formats
            trade_data = {
                'log_id': log_id,
                'timestamp': timestamp,
                'type': record_type,  # Use 'type' instead of 'trade_type'
                'instrument': value_item.get('instrument', ''),
                'price': value_item.get('price', 0.0),
                'profit': value_item.get('profit', 0.0)
            }
            
            # Store in database
            self.db_handler.store_trade(trade_data)
    
    def create_equity_parquet(self, records: List[Dict[str, Any]], output_file: str) -> None:
        """Create a Parquet file from equity records."""
        # Add date column for partitioning
        for record in records:
            record['date'] = datetime.fromtimestamp(record['timestamp']).strftime('%Y-%m-%d')
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Create PyArrow table
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

    def create_trade_parquet(self, records: List[Dict[str, Any]], output_file: str) -> None:
        """Create a Parquet file from trade records."""
        # Add date column for partitioning
        for record in records:
            record['date'] = datetime.fromtimestamp(record['timestamp']).strftime('%Y-%m-%d')
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Create PyArrow table
        table = pa.Table.from_pandas(df)
        
        # Write to Parquet with optimizations
        pq.write_table(
            table,
            output_file,
            compression='snappy',
            use_dictionary=True,
            write_statistics=True
        )
        
        logger.info(f"Saved Parquet file with optimizations for trade data: {output_file}")
    
    def create_weekly_files(self, min_ts: float, max_ts: float) -> None:
        """
        Create weekly Parquet files from temp files.
        
        Args:
            min_ts: Minimum timestamp to include
            max_ts: Maximum timestamp to include
        """
        # Ensure output directory exists
        weekly_dir = os.path.join(self.config['output_dir'], 'weekly')
        os.makedirs(weekly_dir, exist_ok=True)
        
        # Convert timestamps to datetime for interval calculations
        start_dt = datetime.fromtimestamp(min_ts)
        end_dt = datetime.fromtimestamp(max_ts)
        
        # Round to the start of the week (Monday)
        start_dt = start_dt - timedelta(days=start_dt.weekday())
        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Find all temp files
        temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
        if not temp_files:
            logger.info("No temp files found to merge into weekly files")
            return
        
        # Group files by week
        weekly_files = {}
        
        # Read each temp file to get its timestamps
        for temp_file in temp_files:
            try:
                # Read the Parquet file to get timestamps
                table = pq.read_table(temp_file, columns=['timestamp'])
                df = table.to_pandas()
                
                if df.empty:
                    continue
                    
                # Group records by week
                for ts in df['timestamp']:
                    dt = datetime.fromtimestamp(ts)
                    # Get the start of the week (Monday)
                    week_start = dt - timedelta(days=dt.weekday())
                    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    # Skip if outside our range
                    if week_start < start_dt or week_start > end_dt:
                        continue
                        
                    week_key = week_start.strftime('%Y%m%d')
                    if week_key not in weekly_files:
                        weekly_files[week_key] = []
                    
                    if temp_file not in weekly_files[week_key]:
                        weekly_files[week_key].append(temp_file)
                    
            except Exception as e:
                logger.error(f"Error reading timestamps from {temp_file}: {str(e)}")
        
        # If no files were grouped, exit
        if not weekly_files:
            logger.info("No data found in the specified time range for weekly files")
            return
        
        # Process each week that has files
        processed_temp_files = set()
        
        for week_key, files in weekly_files.items():
            try:
                # Parse the week start time
                week_start = datetime.strptime(week_key, '%Y%m%d')
                week_end = week_start + timedelta(days=7)
                
                # Create the output filename
                output_file = os.path.join(weekly_dir, f"weekly_{week_key}.parquet")
                
                # Check if file already exists (idempotent)
                if os.path.exists(output_file):
                    logger.info(f"Weekly file already exists: {output_file}, skipping")
                    continue
                
                # Convert the list to a proper array for DuckDB
                files_array = "[" + ", ".join(f"'{file}'" for file in files) + "]"
                
                # Create a temporary table for the merged data
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE merged AS
                    SELECT * FROM parquet_scan({files_array})
                    WHERE timestamp >= {week_start.timestamp()} AND timestamp < {week_end.timestamp()}
                    ORDER BY timestamp, log_id
                """)
                
                # Check if there are any records in this period
                result = self.conn.execute("SELECT COUNT(*) FROM merged").fetchone()
                if result[0] == 0:
                    logger.info(f"No records found for week {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}, skipping")
                    continue
                
                # Write the merged data to a Parquet file
                self.conn.execute(f"""
                    COPY merged TO '{output_file}' (FORMAT 'parquet')
                """)
                
                logger.info(f"Created weekly file for period {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}: {output_file}")
                logger.info(f"Merged {len(files)} temp files")
                
                # Mark these files as processed
                for file in files:
                    processed_temp_files.add(file)
                
            except Exception as e:
                logger.error(f"Failed to create weekly file for week {week_key}: {str(e)}")
                logger.error(traceback.format_exc())
        
        # After all merges complete, clean up temp files that were successfully merged
        self.cleanup_temp_files(processed_temp_files)
    
    def cleanup_temp_files(self, processed_files: Set[str]) -> None:
        """
        Clean up temporary Parquet files that have been successfully merged.
        
        Args:
            processed_files: Set of file paths that were successfully merged
        """
        try:
            temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
            
            for temp_file in temp_files:
                if temp_file in processed_files:
                    try:
                        os.remove(temp_file)
                        logger.info(f"Removed processed temp file: {temp_file}")
                    except OSError as e:
                        logger.error(f"Failed to remove temp file {temp_file}: {str(e)}")
                else:
                    logger.debug(f"Keeping unprocessed temp file: {temp_file}")
                
        except Exception as e:
            logger.error(f"Error during temp file cleanup: {str(e)}")
            logger.error(traceback.format_exc())
    
    def cleanup_old_temp_files(self, max_age_days: int = 7) -> None:
        """
        Clean up any temporary files older than the specified number of days.
        
        Args:
            max_age_days: Maximum age of temp files in days
        """
        try:
            temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
            cutoff_time = time.time() - (max_age_days * 24 * 60 * 60)
            
            for temp_file in temp_files:
                try:
                    file_mtime = os.path.getmtime(temp_file)
                    if file_mtime < cutoff_time:
                        os.remove(temp_file)
                        logger.warning(f"Removed old temp file: {temp_file} (age: {(time.time() - file_mtime) / 86400:.1f} days)")
                except OSError as e:
                    logger.error(f"Failed to process old temp file {temp_file}: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error during old temp file cleanup: {str(e)}")
            logger.error(traceback.format_exc())
    
    def rollup_to_larger_interval(self, source_interval: str, target_interval: str, min_ts: float, max_ts: float) -> None:
        """
        Roll up files from a smaller interval to a larger interval.
        For example, hourly to daily or daily to monthly.
        """
        # Get source and target directories
        source_dir = os.path.join(self.config['output_dir'], source_interval)
        target_dir = os.path.join(self.config['output_dir'], target_interval)
        
        # Ensure target directory exists
        os.makedirs(target_dir, exist_ok=True)
        
        # Convert timestamps to datetime
        start_dt = datetime.fromtimestamp(min_ts)
        end_dt = datetime.fromtimestamp(max_ts)
        
        # Initialize delta for all cases
        delta = timedelta(days=1)  # Default value
        
        # Set up interval parameters
        if target_interval == 'daily':
            # For daily, round to the start of the day
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            delta = timedelta(days=1)
            format_str = '%Y%m%d_000000'
        elif target_interval == 'monthly':
            # For monthly, round to the start of the month
            start_dt = start_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            format_str = '%Y%m01_000000'
        else:
            logger.error(f"Invalid target interval: {target_interval}")
            return
        
        # Process each time period
        current_dt = start_dt
        processed_source_files = set()
        
        while current_dt <= end_dt:
            # Calculate the next period
            if target_interval == 'monthly':
                next_dt = (current_dt.replace(day=1) + timedelta(days=32)).replace(day=1)
            else:
                next_dt = current_dt + delta
            
            # Format the period for the output filename
            period_str = current_dt.strftime(format_str)
            
            try:
                # Create the output filename
                output_file = os.path.join(target_dir, f"{target_interval}_{period_str}.parquet")
                
                # Check if the file already exists (idempotent)
                if os.path.exists(output_file):
                    logger.info(f"Merged file already exists: {output_file}, skipping")
                else:
                    # Find source files that fall within this period
                    filtered_source_files = []  # Initialize here to avoid reference before assignment
                    
                    if source_interval == 'hourly':
                        source_pattern = f"{source_interval}_*.parquet"
                        source_files = glob.glob(os.path.join(source_dir, source_pattern))
                        
                        # Filter files by timestamp range
                        for file in source_files:
                            try:
                                # Extract date and time from filename
                                filename = os.path.basename(file)
                                parts = filename.split('_')
                                if len(parts) >= 3:
                                    dt_str = f"{parts[1]}_{parts[2].split('.')[0]}"
                                    dt = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
                                    if current_dt <= dt < next_dt:
                                        filtered_source_files.append(file)
                                        processed_source_files.add(file)
                            except Exception as e:
                                logger.warning(f"Could not extract timestamp from filename {file}: {str(e)}")
                    elif source_interval == 'daily':
                        source_pattern = f"{source_interval}_*.parquet"
                        source_files = glob.glob(os.path.join(source_dir, source_pattern))
                        
                        # Filter files by timestamp range
                        for file in source_files:
                            try:
                                # Extract date from filename
                                filename = os.path.basename(file)
                                parts = filename.split('_')
                                if len(parts) >= 2:
                                    dt_str = parts[1]
                                    dt = datetime.strptime(dt_str, "%Y%m%d")
                                    if current_dt.replace(day=1) <= dt < next_dt:
                                        filtered_source_files.append(file)
                                        processed_source_files.add(file)
                            except Exception as e:
                                logger.warning(f"Could not extract timestamp from filename {file}: {str(e)}")
                    
                    # If we found source files, merge them
                    if filtered_source_files:
                        # Convert the list to a proper array for DuckDB
                        files_array = "[" + ", ".join(f"'{file}'" for file in filtered_source_files) + "]"
                        
                        # Create a temporary table for the merged data
                        self.conn.execute(f"""
                            CREATE OR REPLACE TABLE merged AS
                            SELECT * FROM parquet_scan({files_array})
                            WHERE timestamp >= {current_dt.timestamp()} AND timestamp < {next_dt.timestamp()}
                            ORDER BY timestamp, log_id
                        """)
                        
                        # Check if there are any records in this period
                        result = self.conn.execute("SELECT COUNT(*) FROM merged").fetchone()
                        if result[0] == 0:
                            logger.info(f"No records found for period {current_dt} to {next_dt}, skipping")
                            continue
                        
                        # Write the merged data to a Parquet file
                        self.conn.execute(f"""
                            COPY merged TO '{output_file}' (FORMAT 'parquet')
                        """)
                        
                        logger.info(f"Created {target_interval} file for period {current_dt} to {next_dt}: {output_file}")
                        logger.info(f"Merged {len(filtered_source_files)} {source_interval} files")
                    else:
                        logger.info(f"No {source_interval} files found for period {current_dt} to {next_dt}")
                
            except Exception as e:
                logger.error(f"Failed to create {target_interval} file for period {current_dt}: {str(e)}")
                logger.error(traceback.format_exc())
            
            current_dt = next_dt
        
        # Remove source files that were successfully rolled up
        if processed_source_files:
            logger.info(f"Removing {len(processed_source_files)} {source_interval} files that were rolled up to {target_interval}")
            for file in processed_source_files:
                try:
                    os.remove(file)
                    logger.debug(f"Removed {source_interval} file after rollup: {file}")
                except OSError as e:
                    logger.error(f"Failed to remove {source_interval} file {file}: {str(e)}")
    
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
