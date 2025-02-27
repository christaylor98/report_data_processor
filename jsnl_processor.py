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
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
import mysql.connector
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

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
    """Handles all database operations for the processor."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        
    def connect(self) -> None:
        """Connect to the MariaDB database."""
        try:
            self.conn = mysql.connector.connect(**self.config)
            logger.info("Connected to MariaDB database")
        except mysql.connector.Error as err:
            logger.error(f"Failed to connect to database: {err}")
            raise
            
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logger.info("Disconnected from MariaDB database")
            
    def store_equity(self, log_id: str, timestamp: float, mode: str, equity: float) -> None:
        """Store equity data in the database idempotently."""
        if not self.conn or not self.conn.is_connected():
            self.connect()
            
        try:
            cursor = self.conn.cursor()
            
            # First check if record exists and is identical
            check_query = """
                SELECT equity FROM dashboard_equity 
                WHERE id = %s AND timestamp = %s AND mode = %s
            """
            cursor.execute(check_query, (log_id, timestamp, mode))
            result = cursor.fetchone()
            
            if result and abs(result[0] - equity) < 1e-10:  # Compare floats with tolerance
                logger.debug(f"Identical equity record exists for {log_id}, skipping")
                cursor.close()
                return
                
            # Insert or update if different
            query = """
                INSERT INTO dashboard_equity (id, timestamp, mode, equity) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    equity = IF(ABS(equity - VALUES(equity)) >= 1e-10, VALUES(equity), equity)
            """
            cursor.execute(query, (log_id, timestamp, mode, equity))
            self.conn.commit()
            cursor.close()
            
        except mysql.connector.Error as err:
            logger.error(f"Failed to store equity data: {err}")
            self.conn.rollback()
            raise
            
    def store_data(self, log_id: str, timestamp: float, mode: str, data_json: str) -> None:
        """Store trade data in the database idempotently."""
        if not self.conn or not self.conn.is_connected():
            self.connect()
            
        try:
            cursor = self.conn.cursor()
            
            # First check if record exists and is identical
            check_query = """
                SELECT data_json FROM dashboard_data 
                WHERE id = %s AND timestamp = %s AND mode = %s
            """
            cursor.execute(check_query, (log_id, timestamp, mode))
            result = cursor.fetchone()
            
            if result and result[0] == data_json:
                logger.debug(f"Identical trade record exists for {log_id}, skipping")
                cursor.close()
                return
                
            # Insert or update if different
            query = """
                INSERT INTO dashboard_data (id, timestamp, mode, data_json) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    data_json = IF(data_json != VALUES(data_json), VALUES(data_json), data_json)
            """
            cursor.execute(query, (log_id, timestamp, mode, data_json))
            self.conn.commit()
            cursor.close()
            
        except mysql.connector.Error as err:
            logger.error(f"Failed to store trade data: {err}")
            self.conn.rollback()
            raise


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
        
        for jsnl_file in jsnl_files:
            try:
                output_file = self.process_single_file(jsnl_file)
                if output_file:
                    generated_files.append(output_file)
                    
                # Move processed file
                processed_path = os.path.join(self.config['processed_dir'], os.path.basename(jsnl_file))
                shutil.move(jsnl_file, processed_path)
                logger.info(f"Moved processed file to {processed_path}")
                
            except Exception as e:
                logger.error(f"Error processing file {jsnl_file}: {str(e)}")
                logger.error(traceback.format_exc())
                
        return generated_files
    
    def process_single_file(self, jsnl_file: str) -> Optional[str]:
        """
        Process a single JSNL file into Parquet format.
        Returns the path to the generated Parquet file.
        """
        file_id = self.get_file_id(jsnl_file)
        output_file = os.path.join(
            self.config['temp_dir'],
            self.get_output_filename(jsnl_file, file_id)
        )
        
        # Skip if file already exists
        if os.path.exists(output_file):
            logger.info(f"File {output_file} already exists, skipping processing")
            return output_file
            
        # Data containers
        records = []
        
        logger.info(f"Processing file {jsnl_file}")
        
        try:
            with open(jsnl_file, 'r', encoding='utf-8') as f:
                line_count = 0
                for line in f:
                    line_count += 1
                    try:
                        data = json.loads(line)
                        
                        # Extract required fields
                        log_id = data.get('id', '')
                        timestamp = data.get('timestamp', 0.0)
                        value = data.get('value', [])
                        mode = data.get('mode', 'real-time')
                        
                        # Process record for Parquet
                        records.append({
                            'log_id': log_id,
                            'timestamp': timestamp,
                            'data': json.dumps(data)  # Store full data as JSON string
                        })
                        
                        # Check for equity data
                        for item in value:
                            if isinstance(item, dict) and item.get('t') == 'e' and 'equity' in item:
                                equity = float(item.get('equity', 0.0))
                                self.db_handler.store_equity(log_id, timestamp, mode, equity)
                            
                            # Check for trade events
                            if isinstance(item, dict) and item.get('t') in ['open', 'close', 'adjust-open']:
                                self.db_handler.store_data(log_id, timestamp, mode, json.dumps(item))
                    
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON on line {line_count} in {jsnl_file}")
                    except Exception as e:
                        logger.error(f"Error processing line {line_count} in {jsnl_file}: {str(e)}")
            
            # Create Parquet file if we have records
            if records:
                self.save_to_parquet(records, output_file)
                logger.info(f"Created Parquet file: {output_file}")
                return output_file
            else:
                logger.warning(f"No valid records found in {jsnl_file}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to process file {jsnl_file}: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    def save_to_parquet(self, records: List[Dict[str, Any]], output_file: str) -> None:
        """Save records to a Parquet file using DuckDB."""
        if not self.conn:
            self.init_duckdb()
            
        try:
            # Convert records to Arrow table
            df = pa.Table.from_pylist(records)
            
            # Write to Parquet file
            pq.write_table(df, output_file, compression='snappy')
            
        except Exception as e:
            logger.error(f"Failed to save Parquet file: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def merge_parquet_files(self, time_interval: str) -> None:
        """
        Merge Parquet files based on the specified time interval.
        Intervals: 'hourly', 'daily', 'monthly'
        """
        if time_interval not in self.config['merge_intervals']:
            logger.error(f"Invalid merge interval: {time_interval}")
            return
            
        # Calculate the time threshold
        minutes = self.config['merge_intervals'][time_interval]
        threshold = datetime.now() - timedelta(minutes=minutes)
        
        # Directory for the merged files
        merged_dir = os.path.join(self.config['output_dir'], time_interval)
        os.makedirs(merged_dir, exist_ok=True)
        
        # Generate deterministic merged filename based on time period
        period_start = threshold.replace(minute=0, second=0, microsecond=0)
        merged_file = os.path.join(
            merged_dir, 
            f"{time_interval}_{period_start.strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        
        # Skip if merged file already exists
        if os.path.exists(merged_file):
            logger.info(f"Merged file {merged_file} already exists, skipping merge")
            return
        
        # Find files to merge
        temp_files = glob.glob(os.path.join(self.config['temp_dir'], '*.parquet'))
        
        # Filter files based on modification time
        files_to_merge = []
        for file_path in temp_files:
            mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if mod_time >= threshold:
                files_to_merge.append(file_path)
        
        if not files_to_merge:
            logger.info(f"No files found to merge for {time_interval} interval")
            return
            
        logger.info(f"Merging {len(files_to_merge)} files for {time_interval} interval")
        
        try:
            if not self.conn:
                self.init_duckdb()
                
            # Create a table with all the data
            self.conn.execute(f"CREATE TABLE merged AS SELECT * FROM parquet_scan({files_to_merge})")
            
            # Write to a new Parquet file
            self.conn.execute(f"COPY merged TO '{merged_file}' (FORMAT PARQUET)")
            
            # Clean up
            self.conn.execute("DROP TABLE merged")
            
            logger.info(f"Successfully created merged file: {merged_file}")
            
            # If it's a monthly merge, we can delete the original files
            if time_interval == 'monthly':
                for file_path in files_to_merge:
                    try:
                        os.remove(file_path)
                        logger.info(f"Deleted merged file: {file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete file {file_path}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Failed to merge {time_interval} files: {str(e)}")
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
                self.merge_parquet_files('hourly')
                
            # Daily merge (at midnight)
            if current_time.hour == 0 and current_time.minute < self.config['processing_interval_minutes']:
                logger.info("Performing daily merge")
                self.merge_parquet_files('daily')
                
            # Monthly merge (on the 1st of each month)
            if current_time.day == 1 and current_time.hour == 0 and current_time.minute < self.config['processing_interval_minutes']:
                logger.info("Performing monthly merge")
                self.merge_parquet_files('monthly')
                
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
                    processor.merge_parquet_files('hourly')
                    
                # Daily merge (at midnight)
                if current_time.hour == 0 and current_time.minute < processor.config['processing_interval_minutes']:
                    logger.info("Performing daily merge")
                    processor.merge_parquet_files('daily')
                    
                # Monthly merge (on the 1st of each month)
                if current_time.day == 1 and current_time.hour == 0 and current_time.minute < processor.config['processing_interval_minutes']:
                    logger.info("Performing monthly merge")
                    processor.merge_parquet_files('monthly')
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
