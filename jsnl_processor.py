#!/usr/bin/env python3
"""
JSNL to Parquet Processor

This script processes JSNL files into Parquet format using DuckDB.
It also extracts trade and equity data to MariaDB.
"""
#pylint: disable=W1203, W0718, C0301, C0303
import os
import sys
import logging
import shutil
import argparse
import traceback

from dotenv import load_dotenv


from processor import JSNLProcessor

load_dotenv()

# Base directories
BASE_DATA_DIR = '/data'
PROCESS_DIR = f'{BASE_DATA_DIR}/to_process'
PROCESSED_DIR = '/data2/processed'
PARQUET_DIR = f'{BASE_DATA_DIR}/parquet'
LOG_DIR = 'log'

# Subdirectories
DASHBOARD_ARCHIVE_DIR = 'dashboard_data_archive'
PARQUET_TEMP_DIR = 'temp'
PARQUET_HOURLY_DIR = 'hourly'
PARQUET_DAILY_DIR = 'daily'
PARQUET_MONTHLY_DIR = 'monthly'

# Configure logging with more detailed information
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d:%(funcName)s - %(message)s',
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
    },
    'equity_on_change': True
}

# Ensure all directories exist
for dir_path in [CONFIG['input_dir'], CONFIG['processed_dir'], CONFIG['output_dir'], CONFIG['temp_dir']]:
    os.makedirs(dir_path, exist_ok=True)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Process JSNL files into Parquet format.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Process all files in default directories:
    python jsnl_processor.py

  Process a specific file:
    python jsnl_processor.py --file /path/to/file.jsnl

  Process files from custom directories:
    python jsnl_processor.py --source_path /custom/input --processed_path /custom/processed

  Limit processing to N files:
    python jsnl_processor.py --limit 10
        """
    )
    parser.add_argument('--file', '-f', help='Process a single specific file')
    parser.add_argument('--source_path', '-s', help='Source path to process')
    parser.add_argument('--processed_path', '-p', help='Processed path to process')
    parser.add_argument('--limit', '-l', type=int, help='Limit processing to N files')
    parser.add_argument('--leave-source-files', '-ls', action='store_true', help='Leave source files in input directory')
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

        CONFIG['input_dir'] = args.source_path if args.source_path else CONFIG['input_dir']
        CONFIG['processed_dir'] = args.processed_path if args.processed_path else CONFIG['processed_dir']
        CONFIG['leave_source_files'] = args.leave_source_files if args.leave_source_files else False
        
        # Create processor with file limit if specified
        processor = JSNLProcessor(CONFIG, max_files=args.limit)
        
        # Process a single file if specified
        if args.file:
            if not os.path.exists(args.file):
                logger.error(f"File not found: {args.file}")
                return 1
                
            logger.info(f"Processing single file: {args.file}")
            processor.db_handler.init_pool()
            processor.init_duckdb()
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
