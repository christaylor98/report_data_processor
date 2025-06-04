#!/usr/bin/env python3
#pylint: disable=W1203, W0718, C0301, C0303, C0304
"""
Utility to delete all data for a given trading mode from the trading system.
Supports --dry-run to preview actions without making changes.

Usage:
    python delete_mode.py <mode>
    python delete_mode.py <mode> --dry-run
"""
import os
import sys
import glob
import shutil
import argparse
import logging
from dotenv import load_dotenv

# Project imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db import partition_name, DatabaseHandler

# --- Configuration ---
BASE_DATA_DIR = '/data'
PARQUET_DIR = f'{BASE_DATA_DIR}/parquet'
PARQUET_TEMP_DIR = os.path.join(PARQUET_DIR, 'temp')
LOG_DIR = 'log'

# --- Logging setup ---
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'delete_mode.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('delete_mode')

# --- Load environment variables ---
load_dotenv()

def get_db_config():
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '3306'),
        'user': os.getenv('DB_USER', 'finance'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'trading')
    }

def delete_from_db(mode, db_handler, dry_run):
    part_name = partition_name(mode)
    queries = [
        (f"DELETE FROM dashboard_data WHERE mode = %s", (mode,)),
        (f"DELETE FROM trading_instances WHERE mode = %s", (mode,)),
        (f"ALTER TABLE dashboard_equity DROP PARTITION {part_name}", None),
    ]
    for sql, params in queries:
        if dry_run:
            if params:
                logger.info(f"[DRY-RUN] Would execute: {sql} with params {params}")
            else:
                logger.info(f"[DRY-RUN] Would execute: {sql}")
        else:
            try:
                if params:
                    db_handler.execute_write_query_with_retries(sql, params)
                else:
                    db_handler.execute_write_query_with_retries(sql)
                logger.info(f"Executed: {sql}")
            except Exception as e:
                logger.error(f"Failed to execute: {sql} | Error: {e}")

def find_mode_files(mode):
    # Parquet files are stored in PARQUET_DIR/<mode>/
    mode_dir = os.path.join(PARQUET_DIR, str(mode))
    # All temp files that mention the mode in their filename
    temp_files = glob.glob(os.path.join(PARQUET_TEMP_DIR, f'*{mode}*.parquet'))
    return mode_dir, [f for f in temp_files if os.path.isfile(f)]

def delete_files(mode_dir, temp_files, dry_run):
    # Delete the whole mode directory
    if os.path.isdir(mode_dir):
        if dry_run:
            logger.info(f"[DRY-RUN] Would delete directory: {mode_dir}")
        else:
            try:
                shutil.rmtree(mode_dir)
                logger.info(f"Deleted directory: {mode_dir}")
            except Exception as e:
                logger.error(f"Failed to delete directory: {mode_dir} | Error: {e}")
    else:
        logger.info(f"No Parquet directory found for mode: {mode_dir}")
    # Delete temp files as before
    for f in temp_files:
        if dry_run:
            logger.info(f"[DRY-RUN] Would delete file: {f}")
        else:
            try:
                os.remove(f)
                logger.info(f"Deleted file: {f}")
            except Exception as e:
                logger.error(f"Failed to delete file: {f} | Error: {e}")

def main():
    parser = argparse.ArgumentParser(description='Delete all data for a trading mode (with dry-run support)')
    parser.add_argument('mode', help='Trading mode to delete')
    parser.add_argument('--dry-run', action='store_true', help='Preview actions without making changes')
    args = parser.parse_args()

    mode = args.mode
    dry_run = args.dry_run

    logger.info(f"Starting deletion for mode: {mode} (dry-run={dry_run})")

    # --- Database ---
    try:
        db_config = get_db_config()
        db_handler = DatabaseHandler(db_config)
        db_handler.init_pool()
        delete_from_db(mode, db_handler, dry_run)
        db_handler.drop_pool()
    except Exception as e:
        logger.error(f"Failed to delete from database: {e}")
        sys.exit(1)

    # --- Parquet files ---
    mode_dir, temp_files = find_mode_files(mode)
    delete_files(mode_dir, temp_files, dry_run)

    logger.info(f"Completed deletion for mode: {mode} (dry-run={dry_run})")

if __name__ == '__main__':
    main() 