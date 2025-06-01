"""
Mode configuration cache for managing trading mode configurations.
"""

# pylint: disable=W1203,C0303,C0301,C0304

import logging
import json
from datetime import datetime
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class ModeConfigCache:
    """Cache for mode configurations to avoid repeated database lookups."""
    
    def __init__(self, db_handler):
        """
        Initialize the cache.
        
        Args:
            db_handler: DatabaseHandler instance for database operations
        """
        self.db_handler = db_handler
        self._cache: Dict[str, Dict[str, Any]] = {}
        
    def get_config(self, mode: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a mode from cache or database.
        
        Args:
            mode: The mode identifier
            
        Returns:
            Configuration dictionary if found, None otherwise
        """
        # Check cache first
        if mode in self._cache:
            return self._cache[mode]
            
        # Try to load from database
        config = self.load_from_database(mode)
        if config:
            # Clear cachedStats if it exists to force a new calculation
            logger.info(f"Clearing cachedStats for mode: {mode}")
            if 'cachedStats' in config:
                del config['cachedStats']

            # Save the config to the database
            self.update_database(mode, config)

            self._cache[mode] = config
            return config
            
        return None
        
    def add_config(self, mode: str, config: Dict[str, Any]) -> None:
        """
        Add or update configuration in cache and database.
        
        Args:
            mode: The mode identifier
            config: Configuration dictionary
        """
        self._cache[mode] = config
        self.update_database(mode, config)
        
    def update_database(self, mode: str, config: Dict[str, Any]) -> None:
        """
        Update configuration in database.
        
        Args:
            mode: The mode identifier
            config: Configuration dictionary
        """
        try:
            # Extract required fields from config
            component = config.get('designator', 'strand_simulation')
            strand_id = config.get('strand_id', '')

            # Update the processed_at field to the current timestamp
            logger.info(f"Updating database and resetting processed_at with config for mode: {mode}")
            config['processed_at'] = datetime.now().isoformat()
            
            # Store in database
            self.db_handler.store_trading_instance(mode, component, strand_id, config, check_existing=False)
            logger.info(f"Updated database with config for mode: {mode}")
            
        except Exception as e:
            logger.error(f"Error updating database with config for mode {mode}: {str(e)}")
            raise e
            
    def load_from_database(self, mode: str) -> Optional[Dict[str, Any]]:
        """
        Load configuration from database.
        
        Args:
            mode: The mode identifier
            
        Returns:
            Configuration dictionary if found, None otherwise
        """
        try:
            # Query database for mode config
            query = "SELECT data FROM trading_instances WHERE mode = %s"
            result = self.db_handler.execute_read_fetchone_with_retries(query, (mode,))
            
            if result and result[0]:
                return json.loads(result[0])
                
            logger.warning(f"No configuration found in database for mode: {mode}")
            return None
            
        except Exception as e:
            logger.error(f"Error loading config from database for mode {mode}: {str(e)}")
            return None
            
    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear() 