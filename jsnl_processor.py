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
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/jsnl_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('jsnl_processor')

# Configuration
CONFIG = {
    'input_dir': '/data/to_process/dashboard_data_archive',
    'processed_dir': '/data/processed/dashboard_data_archive',
    'output_dir': '/data/parquet',
    'temp_dir': '/data/parquet/temp',
    'db_config': {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME')
    },
    'processing_interval_minutes': 10,
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
        """Store equity data in the database."""
        if not self.conn or not self.conn.is_connected():
            self.connect()
            
        try:
            cursor = self.conn.cursor()
            query = """
                INSERT INTO dashboard_equity (id, timestamp, mode, equity) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE timestamp = %s, mode = %s, equity = %s
            """
            cursor.execute(query, (log_id, timestamp, mode, equity, timestamp, mode, equity))
            self.conn.commit()
            cursor.close()
        except mysql.connector.Error as err:
            logger.error(f"Failed to store equity data: {err}")
            self.conn.rollback()
            raise
            
    def store_data(self, log_id: str, timestamp: float, mode: str, data_json: str) -> None:
        """Store trade data in the database."""
        if not self.conn or not self.conn.is_connected():
            self.connect()
            
        try:
            cursor = self.conn.cursor()
            query = """
                INSERT INTO dashboard_data (id, timestamp, mode, data_json) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE timestamp = %s, mode = %s, data_json = %s
            """
            cursor.execute(query, (log_id, timestamp, mode, data_json, timestamp, mode, data_json))
            self.conn.commit()
            cursor.close()
        except mysql.connector.Error as err:
            logger.error(f"Failed to store trade data: {err}")
            self.conn.rollback()
            raise


class JSNLProcessor:
    """Processes JSNL files into Parquet format and extracts data to MariaDB."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.db_handler = DatabaseHandler(config['db_config'])
        self.conn = None  # DuckDB connection
        
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
    
    def process_jsnl_files(self) -> List[str]:
        """
        Process all JSNL files in the input directory.
        Returns a list of generated Parquet files.
        """
        jsnl_files = glob.glob(os.path.join(self.config['input_dir'], '*.jsnl'))
        if not jsnl_files:
            logger.info("No JSNL files found to process")
            return []
            
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
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(
            self.config['temp_dir'], 
            f"{os.path.basename(jsnl_file).split('.')[0]}_{timestamp}_{file_id}.parquet"
        )
        
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
        
        # Path for the merged file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        merged_file = os.path.join(merged_dir, f"{time_interval}_{timestamp}.parquet")
        
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


def main():
    """Main entry point for the script."""
    try:
        logger.info("Starting JSNL processor")
        processor = JSNLProcessor(CONFIG)
        processor.run()
        logger.info("JSNL processor completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Unhandled exception in JSNL processor: {str(e)}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
