'''
This is a database handler for the JSNL processor.
'''

#pylint: disable=W1203, W0718, C0301, C0303
import os
import time
import logging
import json
from typing import Dict, Any, List
import traceback

import mariadb

logger = logging.getLogger(__name__)

class DatabaseHandler:
    """Handles database operations for JSNL data."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = None
        self.known_instances = set()  # Set to track known (log_id, mode) pairs
        
    def init_pool(self) -> None:
        """Connect to the database and load existing instances."""
        try:
            logger.info("Initializing database pool")
            self.pool = mariadb.ConnectionPool(
                pool_name = 'trading_db_pool',
                pool_size = 5,
                pool_reset_connection = True,
                autocommit = True,
                host = os.getenv('DB_HOST', 'localhost'),
                port = int(os.getenv('DB_PORT', '3306')),
                user = os.getenv('DB_USER', 'root'),
                password = os.getenv('DB_PASSWORD', ''),
                database = os.getenv('DB_NAME', 'trading'),
            )

            logger.info("Connected to database")
            
            # Load existing instances into memory
            self._load_existing_instances()
            
        except mariadb.Error as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def _load_existing_instances(self) -> None:
        """Load existing trading instances from the database into memory."""
        try:
            logger.info("Loading existing trading instances into memory")
            results = self.execute_read_query_with_retries(
                "SELECT id, mode FROM trading_instances"
            )
            if results:
                self.known_instances = set((row[0], row[1]) for row in results)
            logger.info(f"Loaded {len(self.known_instances)} existing trading instances")
        except Exception as e:
            logger.error(f"Error loading existing trading instances: {str(e)}")
            # Continue with empty set if loading fails
            self.known_instances = set()

    def check_pool(self) -> bool:
        """Check if the pool is connected."""
        if self.pool is None:
            self.init_pool()
            if self.pool is None:
                logger.error("Database pool not initialized")
                return False
        return True

    def execute_write_batch_query_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return
        
        retries = 0
        while retries < max_retries:
            try:
                # logger.info("Getting connection from pool")
                conn = self.pool.get_connection()  # Get a connection from the pool
                cursor = conn.cursor()

                # Set query timeout (per query)
                if timeout:
                    # logger.info(f"Setting query timeout to {timeout * 1000} milliseconds")
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                
                # Execute batch insert
                # logger.info("Executing batch insert")
                cursor.executemany(query, params)
                conn.commit()

                # Get results if any
                # if cursor.rowcount > 0:
                #     # logger.info("Fetching results")
                #     results = cursor.fetchall()
                
                # logger.info("Closing cursor and connection")
                cursor.close()
                conn.close()  # Return connection to pool
                return True

            except mariadb.OperationalError as e:
                print(f"Database error: {e}")
                retries += 1
                time.sleep(2)  # Wait before retrying

                # close and reconnect
                self.drop_pool()
                self.init_pool()

            finally:
                try:
                    conn.close()  # Ensure connection is returned to the pool
                except mariadb.Error as e:
                    logger.error(f"Error closing connection: {str(e)}")

        raise RuntimeError("Max retries reached")

    def execute_read_query_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return
        
        results = None
        retries = 0
        while retries < max_retries:
            try:
                conn = self.pool.get_connection()  # Get a connection from the pool
                conn.autocommit = True
                cursor = conn.cursor()

                # Set query timeout (per query)
                if timeout:
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                

                # Execute batch insert
                cursor.execute(query, params)

                # Get results if any
                logger.info(f"Cursor rowcount: {cursor.rowcount}")
                if cursor.rowcount > 0:
                    results = cursor.fetchall()
                
                cursor.close()
                conn.close()  # Return connection to pool
                return results

            except mariadb.OperationalError as e:
                print(f"Database error: {e}")
                retries += 1
                time.sleep(2)  # Wait before retrying

                # close and reconnect
                self.drop_pool()
                self.init_pool()

            finally:
                try:
                    conn.close()  # Ensure connection is returned to the pool
                except mariadb.Error as e:
                    logger.error(f"Error closing connection: {str(e)}")

        raise RuntimeError("Max retries reached")

    def execute_write_query_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return
        
        retries = 0
        while retries < max_retries:
            try:
                conn = self.pool.get_connection()  # Get a connection from the pool
                conn.autocommit = True
                cursor = conn.cursor()

                # Set query timeout (per query)
                if timeout:
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                

                # Execute batch insert
                cursor.execute(query, params)

                # Get results if any
                # logger.info(f"Cursor rowcount: {cursor.rowcount}")
                # if cursor.rowcount > 0:
                #     results = cursor.fetchall()
                
                cursor.close()
                conn.close()  # Return connection to pool
                return True

            except mariadb.OperationalError as e:
                print(f"Database error: {e}")
                retries += 1
                time.sleep(2)  # Wait before retrying

                # close and reconnect
                self.drop_pool()
                self.init_pool()

            finally:
                try:
                    conn.close()  # Ensure connection is returned to the pool
                except mariadb.Error as e:
                    logger.error(f"Error closing connection: {str(e)}")

        raise RuntimeError("Max retries reached")


    def execute_read_fetchone_with_retries(self, query, params=(), max_retries=3, timeout=None):
        """Executes a query using connection pooling with retries and timeouts."""
        if not self.check_pool():
            return None
        
        result = None
        retries = 0
        while retries < max_retries:
            try:
                conn = self.pool.get_connection()  # Get a connection from the pool
                conn.autocommit = True
                cursor = conn.cursor()

                # Set query timeout (per query)
                if timeout:
                    cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
                

                # Execute batch insert
                cursor.execute(query, params)

                if cursor.rowcount > 0:
                    result = cursor.fetchone()
                
                cursor.close()
                conn.close()  # Return connection to pool
                return result

            except mariadb.OperationalError as e:
                print(f"Database error: {e}")
                retries += 1
                time.sleep(2)  # Wait before retrying

                # close and reconnect
                self.drop_pool()
                self.init_pool()

            finally:
                try:
                    conn.close()  # Ensure connection is returned to the pool
                except mariadb.Error as e:
                    logger.error(f"Error closing connection: {str(e)}")

        raise RuntimeError("Max retries reached")


# Example usage
# query = "SELECT * FROM your_table WHERE column = %s"
# results = execute_query_with_retries(query, ("value",))
# print(results)


            
    def drop_pool(self) -> None:
        """Disconnect from the database."""
        if self.pool:
            self.pool.close()
            logger.info("Disconnected from database")
            
    def store_equity(self, record: Dict[str, Any]) -> None:
        """
        Store equity record in the database.
        
        Args:
            record: Dictionary containing equity record data
        """        
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
            self.execute_write_query_with_retries(
                query, 
                (
                    record['log_id'],
                    record['timestamp'],
                    record['mode'],
                    record['value'].get('equity', 0.0),
                    candle_data
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
            # Log the trade data being stored
            # logger.info(f"Storing trade data: id={data['log_id']}, timestamp={data['timestamp']}, type={data['type']}")
            
            # Create JSON data for the dashboard_data table
            json_data = json.dumps({
                "instrument": data['instrument'],
                "price": data['price'],
                "profit": data['profit'],
                "units": data['units'],
                "t": data['type']
            })
            
            # Check if a record with this timestamp, id, and type already exists
            check_query = """
            SELECT id FROM trading.dashboard_data 
            WHERE id = %s AND timestamp = %s AND mode = %s AND data_type = %s
            """
            
            existing = self.execute_read_fetchone_with_retries(
                check_query, 
                (
                    data['log_id'],
                    data['timestamp'],
                    data['mode'],
                    data['type']
                )
            )
            if existing:
                # Update existing record
                update_query = """
                UPDATE trading.dashboard_data 
                SET json_data = %s
                WHERE id = %s AND timestamp = %s AND mode = %s AND data_type = %s
                """
                
                self.execute_write_query_with_retries(
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
                
                self.execute_write_query_with_retries(
                    insert_query, 
                    (
                        data['log_id'],
                        data['timestamp'],
                        data['mode'],
                        data['type'],
                        json_data
                    )
                )
            
            logger.debug(f"Stored trade data for {data['log_id']} at {data['timestamp']}")
        except Exception as e:
            logger.error(f"Failed to store trade data: {str(e)}")

    def store_strand_metadata(self, strand_id: str, config: dict, name: str) -> bool:
        """
        Store or update strand metadata in the database.
        
        Args:
            strand_id: Unique identifier for the strand
            config: Strategy configuration dictionary
            name: Name of the strategy
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Store config as JSON string
            json_data = json.dumps(config)
            
            # Check if record exists
            logger.info(f"Checking if strand metadata exists for strand_id: {strand_id}")
            record = self.execute_read_fetchone_with_retries(
                "SELECT id FROM dashboard_metadata WHERE id = %s AND component = 'strand'",
                (strand_id,)
            )
            logger.info(f"Record: {record}")
            
            if record:
                # Update existing record
                logger.info(f"Updating existing strand metadata for strand_id: {strand_id}")
                self.execute_write_query_with_retries(
                    "UPDATE dashboard_metadata SET name = %s, data = %s WHERE id = %s AND component = 'strand'",
                    (name, json_data, strand_id)
                )
                logger.info(f"Updated existing strand metadata for strand_id: {strand_id}")
            else:
                # Insert new record
                logger.info(f"Inserting new strand metadata for strand_id: {strand_id}")
                self.execute_write_query_with_retries(
                    "INSERT INTO dashboard_metadata (id, component, name, data) VALUES (%s, %s, %s, %s)",
                    (strand_id, 'strand', name, json_data)
                )
                logger.info(f"Inserted new strand metadata for strand_id: {strand_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing strand metadata: {str(e)}")
            return False

    def store_trading_instance(self, instance_id: str, mode: str, config: dict) -> bool:
        """
        Store a trading instance in the database if it doesn't exist.
        
        Args:
            instance_id: The instance identifier (log_id)
            mode: The trading mode
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if we've seen this instance before
            instance_key = (instance_id, mode)
            if instance_key in self.known_instances:
                return True
            
            # Prepare data JSON if we have config
            data_json = None
            if config:
                data_json = json.dumps({'config': config})
            
            # New instance found, add to database
            self.execute_write_query_with_retries(
                "INSERT IGNORE INTO trading_instances (id, mode, data) VALUES (%s, %s, %s)",
                (instance_id, mode, data_json)
            )
            
            # Add to known instances set
            self.known_instances.add(instance_key)
            return True
            
        except Exception as e:
            logger.error(f"Error storing trading instance: {str(e)}")
            return False

    def store_equity_batch(self, records: List[Dict[str, Any]]) -> None:
        """Store a batch of equity records in the database."""
        
        if not records:
            return
        
        # logger.info(f"Storing batch of {len(records)} equity records")

        try:
            # Prepare SQL query
            query = """
                INSERT INTO dashboard_equity 
                (id, timestamp, mode, equity, candle) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                equity = VALUES(equity),
                candle = VALUES(candle)
            """
            
            # Prepare batch data
            batch_data = []
            for record in records:
                candle_data = None
                if 'value' in record and isinstance(record['value'], dict):
                    if 'candle' in record['value']:
                        candle_data = json.dumps(record['value']['candle'])
                
                batch_data.append((
                    record['log_id'],
                    record['timestamp'],
                    record['mode'],
                    record['value'].get('equity', 0.0),
                    candle_data
                ))
            
            # logger.info(f"Executing batch query with {len(batch_data)} records")
            self.execute_write_batch_query_with_retries(query, batch_data)
            
            # logger.info(f"Successfully stored batch of {len(records)} equity records")
            
        except mariadb.Error as e:
            logger.error(f"Error storing equity batch: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error storing equity batch: {str(e)}")
            logger.error(traceback.format_exc())
