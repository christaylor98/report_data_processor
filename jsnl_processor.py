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
            self.conn = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database {self.config['database']} on {self.config['host']}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
            
    def disconnect(self) -> None:
        """Disconnect from the database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
            
    def store_equity(self, data: Dict[str, Any]) -> None:
        """Store equity data in the database."""
        if not self.conn or not self.cursor:
            logger.error("Database connection not established")
            return
            
        try:
            # Insert or replace equity record
            query = """
            REPLACE INTO trading.dashboard_equity 
            (id, timestamp, mode, equity) 
            VALUES (%s, %s, %s, %s)
            """
            
            self.cursor.execute(
                query, 
                (
                    data['log_id'],  # Use log_id as id
                    data['timestamp'],
                    data.get('mode', 'live'),  # Default to 'live' if mode not provided
                    data['equity']
                )
            )
            self.conn.commit()
            logger.debug(f"Stored equity data for {data['log_id']} at {data['timestamp']}")
        except Exception as e:
            logger.error(f"Failed to store equity data: {str(e)}")
            self.conn.rollback()
            
    def store_trade(self, data: Dict[str, Any]) -> None:
        """Store trade data in the database."""
        if not self.conn or not self.cursor:
            logger.error("Database connection not established")
            return
            
        try:
            # Use log_id directly as the ID
            trade_id = data['log_id']
            
            # Create JSON data for the trade
            json_data = json.dumps({
                'instrument': data['instrument'],
                'price': data['price'],
                'profit': data['profit']
            })
            
            # Insert or replace trade record in trading.dashboard_data table
            query = """
            REPLACE INTO trading.dashboard_data 
            (timestamp, id, mode, data_type, json_data) 
            VALUES (%s, %s, %s, %s, %s)
            """
            
            self.cursor.execute(
                query, 
                (
                    data['timestamp'],
                    trade_id,
                    data.get('mode', 'live'),  # Default to 'live' if mode not provided
                    data['trade_type'],  # Use trade_type as data_type
                    json_data
                )
            )
            self.conn.commit()
            logger.debug(f"Stored trade data for {trade_id} at {data['timestamp']}")
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
        Process all JSNL files in the input directory.
        Returns a list of generated Parquet files.
        """
        jsnl_files = glob.glob(os.path.join(self.config['input_dir'], '*.jsnl'))
        if not jsnl_files:
            logger.info("No JSNL files found to process")
            return []
            
        # Limit the number of files if max_files is specified
        if self.max_files is not None:
            jsnl_files = jsnl_files[:self.max_files]
            logger.info(f"Processing limited to {self.max_files} files")
            
        logger.info(f"Found {len(jsnl_files)} JSNL files to process")
        generated_files = []
        data_timestamps = set()  # Track timestamps from the data
        
        for jsnl_file in jsnl_files:
            try:
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
                
        # Perform merges based on data timestamps
        if data_timestamps:
            min_ts = min(data_timestamps)
            max_ts = max(data_timestamps)
            logger.info(f"Data timestamps range from {datetime.fromtimestamp(min_ts)} to {datetime.fromtimestamp(max_ts)}")
            
            # Merge files for each hour in the data range
            self.merge_parquet_files('hourly', min_ts, max_ts)
            
            # Merge files for each day in the data range
            self.merge_parquet_files('daily', min_ts, max_ts)
            
            # Merge files for each month in the data range
            self.merge_parquet_files('monthly', min_ts, max_ts)
        
        return generated_files
    
    def process_single_file(self, jsnl_file: str) -> Tuple[Optional[str], Set[float]]:
        """
        Process a single JSNL file into Parquet format.
        Returns the path to the generated Parquet file and set of timestamps.
        """
        file_id = self.get_file_id(jsnl_file)
        output_filename = self.get_output_filename(jsnl_file, file_id)
        
        # Check if file already exists (idempotent processing)
        output_file = os.path.join(self.config['temp_dir'], output_filename)
        if os.path.exists(output_file):
            logger.info(f"File {output_file} already exists, skipping processing")
            return output_file, set()
        
        logger.info(f"Processing file {jsnl_file}")
        
        # Initialize counters for statistics
        total_records = 0
        invalid_records = 0
        missing_fields = 0
        processed_records = 0
        
        # Initialize data structures for records
        records = []
        timestamps = set()
        
        # Process each line in the file
        with open(jsnl_file, 'r', encoding='utf-8') as f:
            for _line_num, line in enumerate(f, 1):
                total_records += 1
                
                try:
                    # Parse JSON
                    record = json.loads(line)
                    
                    # Extract required fields
                    log_id = record.get('log_id')
                    timestamp = record.get('timestamp')
                    value = record.get('value')
                    
                    # Skip records without required fields
                    if not log_id or not timestamp or not value:
                        missing_fields += 1
                        continue
                    
                    # Skip records with empty log_id
                    if not log_id.strip():
                        missing_fields += 1
                        continue
                    
                    # Add timestamp to set
                    timestamps.add(timestamp)
                    
                    # Add filename to record for traceability
                    record_data = {
                        'log_id': log_id,
                        'timestamp': timestamp,
                        'filename': os.path.basename(jsnl_file),
                        'data': json.dumps(record)  # Store the entire record as JSON string
                    }
                    
                    # Add to records for Parquet
                    records.append(record_data)
                    processed_records += 1
                    
                    # Process for database storage based on value type
                    self.process_record_for_db(record)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in file {jsnl_file}: {str(e)}")
                    invalid_records += 1
                except Exception as e:
                    logger.error(f"Error processing line in {jsnl_file}: {str(e)}")
                    invalid_records += 1
        
        # Log processing statistics
        logger.info(f"File processing statistics for {jsnl_file}:")
        logger.info(f"  Total records: {total_records}")
        logger.info(f"  Invalid records: {invalid_records}")
        logger.info(f"  Missing required fields: {missing_fields}")
        logger.info(f"  Successfully processed: {processed_records}")
        
        # If no valid records were found, return None
        if not records:
            logger.warning(f"No valid records found in {jsnl_file}, no Parquet file created")
            return None, timestamps
        
        # Create Parquet file
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
        logger.info(f"Created Parquet file: {output_file} with {len(records)} records")
        
        return output_file, timestamps
    
    def process_record_for_db(self, record: Dict[str, Any]) -> None:
        """
        Process a record for database storage.
        Extracts equity and trade data and stores in the appropriate database tables.
        """
        log_id = record.get('log_id')
        timestamp = record.get('timestamp')
        value = record.get('value')
        
        if not log_id or not timestamp or not value:
            return
        
        # Process based on value type
        if isinstance(value, dict):
            # Single value object
            self.process_value_object(log_id, timestamp, value)
        elif isinstance(value, list):
            # Multiple value objects
            for value_obj in value:
                if isinstance(value_obj, dict):
                    self.process_value_object(log_id, timestamp, value_obj)

    def process_value_object(self, log_id: str, timestamp: float, value_obj: Dict[str, Any]) -> None:
        """Process a single value object for database storage."""
        record_type = value_obj.get('t')
        
        if record_type == 'e':  # Equity record
            # Extract equity data
            equity_data = {
                'log_id': log_id,
                'timestamp': timestamp,
                'broker': value_obj.get('b', ''),
                'equity': value_obj.get('equity', 0.0)
            }
            
            # Store in database
            self.db_handler.store_equity(equity_data)
            
        elif record_type in ['open', 'close', 'adjust-open', 'adjust-close']:  # Trade record
            # Extract trade data
            trade_data = {
                'log_id': log_id,
                'timestamp': timestamp,
                'trade_type': record_type,
                'instrument': value_obj.get('instrument', ''),
                'price': value_obj.get('price', 0.0),
                'profit': value_obj.get('profit', 0.0)
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
    
    def merge_parquet_files(self, time_interval: str, min_ts: float, max_ts: float) -> None:
        """
        Merge Parquet files based on the specified time interval and timestamp range.
        Intervals: 'hourly', 'daily', 'monthly'
        """
        # Get the appropriate output directory based on the interval
        if time_interval == 'hourly':
            output_dir = os.path.join(self.config['output_dir'], 'hourly')
        elif time_interval == 'daily':
            output_dir = os.path.join(self.config['output_dir'], 'daily')
        elif time_interval == 'monthly':
            output_dir = os.path.join(self.config['output_dir'], 'monthly')
        else:
            logger.error(f"Invalid time interval: {time_interval}")
            return
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Find all Parquet files in the temp directory
        temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
        
        if not temp_files:
            logger.info(f"No files found to merge for {time_interval} interval")
            return
        
        # Track which files were successfully processed
        processed_temp_files = set()
        
        # Convert timestamps to datetime for interval calculations
        start_dt = datetime.fromtimestamp(min_ts)
        end_dt = datetime.fromtimestamp(max_ts)
        
        # Initialize delta for all cases
        delta = timedelta(hours=1)  # Default value
        
        if time_interval == 'hourly':
            # For hourly, round to the start of the hour
            start_dt = start_dt.replace(minute=0, second=0, microsecond=0)
            delta = timedelta(hours=1)
        elif time_interval == 'daily':
            # For daily, round to the start of the day
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            delta = timedelta(days=1)
        elif time_interval == 'monthly':
            # For monthly, round to the start of the month
            start_dt = start_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # For monthly, we'll use the delta in the next_dt calculation
        
        current_dt = start_dt
        
        # Process each time period
        while current_dt <= end_dt:
            # Calculate the next period
            if time_interval == 'monthly':
                next_dt = (current_dt.replace(day=1) + timedelta(days=32)).replace(day=1)
            else:
                next_dt = current_dt + delta
            
            # Format the period for the output filename
            period_str = current_dt.strftime('%Y%m%d_%H%M%S')
            
            try:
                # Create the output filename
                output_file = os.path.join(output_dir, f"{time_interval}_{period_str}.parquet")
                
                # Check if the file already exists (idempotent)
                if os.path.exists(output_file):
                    logger.info(f"Merged file already exists: {output_file}, skipping")
                else:
                    # Create a temporary table for the merged data
                    self.conn.execute(f"""
                        CREATE OR REPLACE TABLE merged AS
                        SELECT * FROM parquet_scan('{os.path.join(self.config['temp_dir'], '*.parquet')}')
                        WHERE timestamp >= '{current_dt.timestamp()}'
                        AND timestamp < '{next_dt.timestamp()}'
                        ORDER BY timestamp, log_id
                    """)
                    
                    # Write the merged data to a Parquet file
                    self.conn.execute(f"""
                        COPY merged TO '{output_file}' (FORMAT 'parquet')
                    """)
                    
                    logger.info(f"Created merged file for period {current_dt} to {next_dt}: {output_file}")
                
                # Mark these files as processed
                for file in temp_files:
                    processed_temp_files.add(file)
                
            except Exception as e:
                logger.error(f"Failed to merge {time_interval} files for period {current_dt}: {str(e)}")
                logger.error(traceback.format_exc())
            
            current_dt = next_dt
        
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
    
    def run(self) -> None:
        """Run the JSNL processing pipeline."""
        try:
            self.db_handler.connect()
            self.init_duckdb()
            
            # Process JSNL files
            generated_files = self.process_jsnl_files()
            logger.info(f"Generated {len(generated_files)} Parquet files")
            
            # Determine which merges to perform
            current_time = datetime.now()
            
            # Hourly merge (every hour)
            if current_time.minute < self.config['processing_interval_minutes']:
                logger.info("Performing hourly merge")
                self.merge_parquet_files('hourly', current_time.timestamp(), current_time.timestamp())
                
            # Daily merge (at midnight)
            if current_time.hour == 0 and current_time.minute < self.config['processing_interval_minutes']:
                logger.info("Performing daily merge")
                self.merge_parquet_files('daily', current_time.timestamp(), current_time.timestamp())
                
            # Monthly merge (on the 1st of each month)
            if current_time.day == 1 and current_time.hour == 0 and current_time.minute < self.config['processing_interval_minutes']:
                logger.info("Performing monthly merge")
                self.merge_parquet_files('monthly', current_time.timestamp(), current_time.timestamp())
                
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
    parser.add_argument('--skip-merge', action='store_true', help='Skip the merge step')
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
            processor.db_handler.connect()
            processor.init_duckdb()
            
            # Process JSNL files
            generated_files = processor.process_jsnl_files()
            logger.info(f"Generated {len(generated_files)} Parquet files")
            
            # Skip merge if requested
            if not args.skip_merge:
                # Determine which merges to perform
                current_time = datetime.now()
                
                # Hourly merge (every hour)
                if current_time.minute < processor.config['processing_interval_minutes']:
                    logger.info("Performing hourly merge")
                    processor.merge_parquet_files('hourly', current_time.timestamp(), current_time.timestamp())
                    
                # Daily merge (at midnight)
                if current_time.hour == 0 and current_time.minute < processor.config['processing_interval_minutes']:
                    logger.info("Performing daily merge")
                    processor.merge_parquet_files('daily', current_time.timestamp(), current_time.timestamp())
                    
                # Monthly merge (on the 1st of each month)
                if current_time.day == 1 and current_time.hour == 0 and current_time.minute < processor.config['processing_interval_minutes']:
                    logger.info("Performing monthly merge")
                    processor.merge_parquet_files('monthly', current_time.timestamp(), current_time.timestamp())
            else:
                logger.info("Skipping merge step as requested")
                
            processor.db_handler.disconnect()
            processor.close_duckdb()
            
        logger.info("JSNL processor completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Unhandled exception in JSNL processor: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
