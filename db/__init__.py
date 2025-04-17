'''
This is a database handler for the JSNL processor.
'''

#pylint: disable=W1203, W0718, C0301, C0303
import os
import time
import logging
import json
import hashlib
from typing import Dict, Any, List
import traceback

import mariadb

logger = logging.getLogger(__name__)

def partition_name(mode: str) -> str:
    """Generate a partition name from a mode value."""
    h = hashlib.md5(mode.encode()).hexdigest()[:8]
    return f"p_{h}"

class DatabaseHandler:
    """Handles database operations for JSNL data."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the database handler.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.conn = None
        self.temp_conn = None  # Separate connection for temp table operations
        self.cursor = None
        self.known_instances = set()
        self.temp_equity_table = 'dashboard_equity_staging'  # Changed to match new staging table name
        self.pool_size = 5
        self.connect_timeout = 10
        self.read_timeout = 30
        self.write_timeout = 30
        
    def init_pool(self) -> None:
        """Initialize the database connection pool."""
        try:
            # Create main connection pool with timeouts
            self.conn = mariadb.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'trading'),
                pool_name='mypool',
                pool_size=self.pool_size,
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                write_timeout=self.write_timeout,
                autocommit=False
            )
            
            # Create separate connection for temp table operations
            self.temp_conn = mariadb.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'trading'),
                pool_name='temp_pool',
                pool_size=1,  # Single connection for temp operations
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                write_timeout=self.write_timeout,
                autocommit=False
            )
            
            # Create temporary equity table if it doesn't exist
            self._create_temp_equity_table()
            
        except mariadb.Error as e:
            logger.error(f"Error initializing database pools: {str(e)}")
            raise
            
    def _get_cursor(self, use_temp: bool = False):
        """Get a new cursor for database operations."""
        conn = self.temp_conn if use_temp else self.conn
        if not conn or not conn.open:
            if use_temp:
                self._init_temp_pool()
            else:
                self.init_pool()
        return conn.cursor()
            
    def _init_temp_pool(self) -> None:
        """Initialize the temporary table connection pool."""
        try:
            self.temp_conn = mariadb.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'trading'),
                pool_name='temp_pool',
                pool_size=1,
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
                write_timeout=self.write_timeout,
                autocommit=False
            )
        except mariadb.Error as e:
            logger.error(f"Error initializing temp pool: {str(e)}")
            raise
            
    def _create_temp_equity_table(self) -> None:
        """Create a staging table for equity records."""
        try:
            cursor = self._get_cursor(use_temp=True)
            
            # Drop the staging table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {self.temp_equity_table}")
            
            # Create staging table like the main table
            cursor.execute(f"CREATE TABLE {self.temp_equity_table} LIKE dashboard_equity")
            
            # Remove partitioning from the staging table
            cursor.execute(f"ALTER TABLE {self.temp_equity_table} REMOVE PARTITIONING")
            
            self.temp_conn.commit()
            cursor.close()
            logger.info(f"Created staging equity table: {self.temp_equity_table}")
            
        except Exception as e:
            logger.error(f"Error creating staging equity table: {str(e)}")
            raise
            
    def store_equity_batch(self, records: List[Dict[str, Any]]) -> None:
        """Store a batch of equity records in the staging table."""
        
        if not records:
            return
        
        try:
            # Process records in chunks of 1000 to avoid connection issues
            chunk_size = 1000
            total_records = len(records)
            processed_records = 0
            
            while processed_records < total_records:
                # Get chunk of records
                chunk = records[processed_records:processed_records + chunk_size]
                
                # Prepare SQL query for staging table
                query = f"""
                    INSERT INTO {self.temp_equity_table}
                    (time_ns, mode, equity) 
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    equity = VALUES(equity)
                """
                
                # Prepare batch data for this chunk
                batch_data = []
                for record in chunk:
                    # Convert timestamp to nanoseconds
                    time_ns = int(record['timestamp'] * 1e9)
                    batch_data.append((
                        time_ns,
                        record['mode'],
                        record['value'].get('equity', 0.0)
                    ))
                
                # Use retry mechanism for each chunk
                retries = 0
                max_retries = 3
                while retries < max_retries:
                    try:
                        cursor = self._get_cursor(use_temp=True)
                        try:
                            # Execute batch insert for this chunk
                            cursor.executemany(query, batch_data)
                            self.temp_conn.commit()
                            break  # Success, exit retry loop
                            
                        finally:
                            cursor.close()
                            
                    except mariadb.Error as e:
                        retries += 1
                        if retries == max_retries:
                            logger.error(f"Failed to store equity batch after {max_retries} retries: {str(e)}")
                            raise
                        logger.warning(f"Retry {retries}/{max_retries} for equity batch due to: {str(e)}")
                        time.sleep(2)  # Wait before retry
                        self._handle_connection_error(e, retries, use_temp=True)
                
                processed_records += len(chunk)
                logger.info(f"Processed {processed_records}/{total_records} equity records")
            
            logger.info(f"Successfully stored all {total_records} equity records in staging table")
            
        except mariadb.Error as e:
            logger.error(f"Error storing equity batch in staging table: {str(e)}")
            if self.temp_conn:
                self.temp_conn.rollback()
        except Exception as e:
            logger.error(f"Unexpected error storing equity batch in staging table: {str(e)}")
            logger.error(traceback.format_exc())
            if self.temp_conn:
                self.temp_conn.rollback()
            
    # def merge_temp_equity_records(self) -> None:
    #     """Legacy method that calls merge_staging_equity for backward compatibility."""
    #     logger.warning("merge_temp_equity_records is deprecated, use merge_staging_equity instead")
    #     self.merge_staging_equity(None)

    def _load_existing_instances(self) -> None:
        """Load existing trading instances from the database into memory."""
        try:
            logger.info("Loading existing trading instances into memory")
            results = self.execute_read_query_with_retries(
                "SELECT mode FROM trading_instances"
            )
            if results:
                self.known_instances = set(row[0] for row in results)
            logger.info(f"Loaded {len(self.known_instances)} existing trading instances")
        except Exception as e:
            logger.error(f"Error loading existing trading instances: {str(e)}")
            # Continue with empty set if loading fails
            self.known_instances = set()

    def check_pool(self) -> bool:
        """Check if the pool is connected."""
        if self.conn is None:
            self.init_pool()
            if self.conn is None:
                logger.error("Database pool not initialized")
                return False
        return True

    def _handle_connection_error(self, e: Exception, retries: int, use_temp: bool = False) -> None:
        """Handle connection errors and attempt recovery."""
        logger.error(f"Database error (attempt {retries + 1}): {str(e)}")
        
        # Close existing connection
        conn = self.temp_conn if use_temp else self.conn
        if conn:
            try:
                conn.close()
            except:
                pass
            if use_temp:
                self.temp_conn = None
            else:
                self.conn = None
            
        # Wait before retrying
        time.sleep(2)
        
        # Reinitialize appropriate pool
        if use_temp:
            self._init_temp_pool()
        else:
            self.init_pool()

    def execute_write_query_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return False
        
        retries = 0
        while retries < max_retries:
            try:
                cursor = self._get_cursor()
                
                # Set query timeout if specified
                if timeout:
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                
                # Execute query
                cursor.execute(query, params)
                self.conn.commit()
                cursor.close()
                return True
                
            except mariadb.OperationalError as e:
                self._handle_connection_error(e, retries)
                retries += 1
                
            except mariadb.Error as e:
                logger.error(f"Database error: {str(e)}")
                if self.conn:
                    self.conn.rollback()
                retries += 1
                self._handle_connection_error(e, retries)
                
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                if self.conn:
                    self.conn.rollback()
                retries += 1
                self._handle_connection_error(e, retries)
                
        raise RuntimeError(f"Max retries ({max_retries}) reached for query: {query}")

    def execute_read_query_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a read query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return None
        
        retries = 0
        while retries < max_retries:
            try:
                cursor = self._get_cursor()
                
                # Set query timeout if specified
                if timeout:
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                
                # Execute query
                cursor.execute(query, params)
                
                # Get results if any
                results = cursor.fetchall() if cursor.rowcount > 0 else None
                cursor.close()
                return results
                
            except mariadb.OperationalError as e:
                self._handle_connection_error(e, retries)
                retries += 1
                
            except mariadb.Error as e:
                logger.error(f"Database error: {str(e)}")
                retries += 1
                self._handle_connection_error(e, retries)
                
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                retries += 1
                self._handle_connection_error(e, retries)
                
        raise RuntimeError(f"Max retries ({max_retries}) reached for query: {query}")

    def execute_read_fetchone_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a read query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return None
        
        retries = 0
        while retries < max_retries:
            try:
                cursor = self._get_cursor()
                
                # Set query timeout if specified
                if timeout:
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                
                # Execute query
                cursor.execute(query, params)
                
                # Get result if any
                result = cursor.fetchone() if cursor.rowcount > 0 else None
                cursor.close()
                return result
                
            except mariadb.OperationalError as e:
                self._handle_connection_error(e, retries)
                retries += 1
                
            except mariadb.Error as e:
                logger.error(f"Database error: {str(e)}")
                retries += 1
                self._handle_connection_error(e, retries)
                
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                retries += 1
                self._handle_connection_error(e, retries)
                
        raise RuntimeError(f"Max retries ({max_retries}) reached for query: {query}")

    def execute_write_batch_query_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a batch write query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return False
        
        retries = 0
        while retries < max_retries:
            try:
                cursor = self._get_cursor()
                
                # Set query timeout if specified
                if timeout:
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                
                # Execute batch query
                cursor.executemany(query, params)
                self.conn.commit()
                cursor.close()
                return True
                
            except mariadb.OperationalError as e:
                self._handle_connection_error(e, retries)
                retries += 1
                
            except mariadb.Error as e:
                logger.error(f"Database error: {str(e)}")
                if self.conn:
                    self.conn.rollback()
                retries += 1
                self._handle_connection_error(e, retries)
                
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                if self.conn:
                    self.conn.rollback()
                retries += 1
                self._handle_connection_error(e, retries)
                
        raise RuntimeError(f"Max retries ({max_retries}) reached for batch query: {query}")

    def store_equity(self, record: Dict[str, Any]) -> None:
        """
        Store equity record in the database.
        
        Args:
            record: Dictionary containing equity record data
        """        
        try:
            # Convert timestamp to nanoseconds
            time_ns = int(record['timestamp'] * 1e9)
            
            # Prepare SQL query
            query = """
                INSERT INTO dashboard_equity 
                (time_ns, mode, equity) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                equity = VALUES(equity)
            """
            
            # Execute query
            self.execute_write_query_with_retries(
                query, 
                (
                    time_ns,
                    record['mode'],
                    record['value'].get('equity', 0.0)
                )
            )
        except mariadb.Error as e:
            logger.error(f"Error storing equity record: {str(e)}")
            logger.error(f"Record: {record}")
        except Exception as e:
            logger.error(f"Unexpected error storing equity record: {str(e)}")
            logger.error(f"Record: {record}")
            logger.error(traceback.format_exc())
            
    def store_trade(self, data: Dict[str, Any]) -> None:
        """Store trade data in the database."""
        try:
            # Convert timestamp to nanoseconds
            time_ns = int(data['timestamp'] * 1e9)
            
            # Create JSON data for the dashboard_data table
            json_data = json.dumps({
                "id": data.get('id', ''),
                "instrument": data['instrument'],
                "price": data['price'],
                "profit": data['profit'],
                "units": data['units'],
                "t": data['type']
            })
            
            # Check if a record with this timestamp and type already exists
            check_query = """
            SELECT time_ns FROM dashboard_data 
            WHERE time_ns = %s AND mode = %s AND data_type = %s
            """
            
            existing = self.execute_read_fetchone_with_retries(
                check_query, 
                (
                    time_ns,
                    data['mode'],
                    data['type']
                )
            )
            
            if existing:
                # Update existing record
                update_query = """
                UPDATE dashboard_data 
                SET data = %s
                WHERE time_ns = %s AND mode = %s AND data_type = %s
                """
                
                self.execute_write_query_with_retries(
                    update_query, 
                    (
                        json_data,
                        time_ns,
                        data['mode'],
                        data['type']
                    )
                )
            else:
                # Insert new record
                insert_query = """
                INSERT INTO dashboard_data 
                (time_ns, mode, data_type, data) 
                VALUES (%s, %s, %s, %s)
                """
                
                self.execute_write_query_with_retries(
                    insert_query, 
                    (
                        time_ns,
                        data['mode'],
                        data['type'],
                        json_data
                    )
                )
            
            logger.debug(f"Stored trade data at {time_ns}")
            
        except Exception as e:
            logger.error(f"Failed to store trade data: {str(e)}")
            logger.error(traceback.format_exc())


    def store_trading_instance(self, mode: str, component: str, designator: str, config: dict) -> bool:
        """
        Store a trading instance in the database if it doesn't exist.
        
        Args:
            mode: The trading mode (primary key)
            component: The component name
            designator: The designator value
            config: The configuration dictionary to store in the data field
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if we've seen this instance before
            if mode in self.known_instances:
                return True
            
            # Prepare data JSON if we have config
            data_json = None
            if config:
                data_json = json.dumps(config)
            
            # New instance found, add to database
            self.execute_write_query_with_retries(
                "INSERT IGNORE INTO trading_instances (mode, component, designator, data) VALUES (%s, %s, %s, %s)",
                (mode, component, designator, data_json)
            )
            
            # Add to known instances set
            self.known_instances.add(mode)
            return True
            
        except Exception as e:
            logger.error(f"Error storing trading instance: {str(e)}")
            return False

    def drop_pool(self) -> None:
        """Disconnect from the database."""
        if self.conn:
            self.conn.close()
            self.conn = None
        if self.temp_conn:
            self.temp_conn.close()
            self.temp_conn = None
        logger.info("Disconnected from database")

    def merge_staging_equity(self, last_mode: str) -> None:
        """Merge staging equity records into the main dashboard_equity table for the current run."""
        try:
            # Use temp connection to get unique modes from staging table
            mode = None
            if not last_mode:
                temp_cursor = self._get_cursor(use_temp=True)
                temp_cursor.execute(f"SELECT DISTINCT mode FROM {self.temp_equity_table}")
                mode = [row[0] for row in temp_cursor.fetchall()][0]
                temp_cursor.close()
            else:
                mode = last_mode
            
            if not mode:
                logger.info("No records to merge from staging table")
                return
                
            # Get partition name for this mode
            p_name = partition_name(mode)
            
            # Use main connection for partition operations
            cursor = self._get_cursor()
            try:
                # Add partition if it doesn't exist
                add_partition_query = f"""
                    ALTER TABLE dashboard_equity
                    ADD PARTITION (
                        PARTITION {p_name} VALUES IN ('{mode}')
                    )
                """
                try:
                    cursor.execute(add_partition_query)
                    self.conn.commit()
                    logger.info(f"Added partition {p_name} for mode {mode}")
                except mariadb.Error as e:
                    if "Duplicate partition name" in str(e):
                        logger.info(f"Partition {p_name} already exists for mode {mode}")
                    else:
                        raise
                
                # Exchange partition with staging table
                exchange_query = f"""
                    ALTER TABLE dashboard_equity
                    EXCHANGE PARTITION {p_name} WITH TABLE {self.temp_equity_table}
                    WITHOUT VALIDATION
                """
                cursor.execute(exchange_query)
                self.conn.commit()
                logger.info(f"Exchanged partition {p_name} with staging table for mode {mode}")
                
            finally:
                cursor.close()
            
            # Truncate staging table after successful merge
            temp_cursor = self._get_cursor(use_temp=True)
            try:
                temp_cursor.execute(f"TRUNCATE TABLE {self.temp_equity_table}")
                self.temp_conn.commit()
                logger.info("Successfully truncated staging equity table")
            finally:
                temp_cursor.close()
            
            logger.info("Successfully merged staging equity records into main table")
            
        except Exception as e:
            # Rollback on error
            if self.conn:
                self.conn.rollback()
            if self.temp_conn:
                self.temp_conn.rollback()
            logger.error(f"Error merging staging equity records: {str(e)}")
            logger.error(traceback.format_exc())
            raise
