import fcntl
import os
import logging
import time
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class FileLock:
    """A class to handle file locking for JSNL processing."""
    
    def __init__(self, lock_dir: str):
        """
        Initialize the file lock handler.
        
        Args:
            lock_dir: Directory where lock files will be stored
        """
        self.lock_dir = lock_dir
        os.makedirs(lock_dir, exist_ok=True)
        
    def _get_lock_path(self, file_path: str) -> str:
        """Get the path for the lock file."""
        file_name = os.path.basename(file_path)
        return os.path.join(self.lock_dir, f"{file_name}.lock")
        
    @contextmanager
    def acquire_lock(self, file_path: str, timeout: int = 30) -> Optional[int]:
        """
        Acquire a lock for the given file.
        
        Args:
            file_path: Path to the file to lock
            timeout: Maximum time to wait for lock in seconds
            
        Yields:
            File descriptor if lock acquired, None otherwise
        """
        lock_path = self._get_lock_path(file_path)
        fd = None
        
        try:
            # Open lock file
            fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
            
            # Try to acquire lock with timeout
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    logger.info(f"Acquired lock for {file_path}")
                    yield fd
                    return
                except IOError:
                    time.sleep(0.1)
                    
            logger.warning(f"Could not acquire lock for {file_path} within {timeout} seconds")
            yield None
            
        except Exception as e:
            logger.error(f"Error acquiring lock for {file_path}: {str(e)}")
            yield None
            
        finally:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                except Exception as e:
                    logger.error(f"Error releasing lock for {file_path}: {str(e)}")