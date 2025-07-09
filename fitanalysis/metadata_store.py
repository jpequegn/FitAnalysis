import duckdb
import pandas as pd
import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class MetadataStoreError(Exception):
    """Base exception for metadata store errors."""
    pass


class DatabaseConnectionError(MetadataStoreError):
    """Raised when database connection fails."""
    pass


class DatabaseOperationError(MetadataStoreError):
    """Raised when database operation fails."""
    pass

class MetadataStore:
    """
    A class to store and retrieve activity metadata from a DuckDB database.

    This class can be used as a context manager to ensure that the database
    connection is properly closed.
    """
    def __init__(self, db_path: str = 'fit_metadata.db'):
        """
        Initializes the MetadataStore.

        Args:
            db_path: The path to the DuckDB database file.
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self) -> "MetadataStore":
        """Opens the database connection."""
        try:
            self.conn = duckdb.connect(database=self.db_path, read_only=False)
            logger.info(f"Connected to database: {self.db_path}")
            self._create_table()
            return self
        except Exception as e:
            error_msg = f"Failed to connect to database {self.db_path}: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the database connection."""
        self.close()

    def _create_table(self):
        """Create the activities table if it doesn't exist."""
        try:
            if self.conn is None:
                raise DatabaseConnectionError("Database connection is not available")
                
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS activities (
                        activity_id VARCHAR PRIMARY KEY,
                        activity_name VARCHAR,
                        activity_type VARCHAR,
                        start_time_gmt TIMESTAMP,
                        start_time_local TIMESTAMP,
                        distance DOUBLE,
                        duration DOUBLE,
                        calories INTEGER,
                        average_hr INTEGER,
                        max_hr INTEGER,
                        average_power INTEGER,
                        max_power INTEGER,
                        file_path VARCHAR
                    )
                """)
            logger.debug(f"Activities table created/verified in {self.db_path}")
        except Exception as e:
            error_msg = f"Failed to create activities table in {self.db_path}: {e}"
            logger.error(error_msg)
            raise DatabaseOperationError(error_msg) from e

    def store_activity_metadata(self, activity_data: Dict[str, Any], file_path: str):
        """Store activity metadata in the database."""
        try:
            if self.conn is None:
                raise DatabaseConnectionError("Database connection is not available")
                
            activity_id = str(activity_data.get('activityId'))
            if not activity_id or activity_id == 'None':
                raise DatabaseOperationError("Activity ID is required and cannot be None")
            
            with self.conn.cursor() as cursor:
                result = cursor.execute(
                    "SELECT activity_id FROM activities WHERE activity_id = ?", 
                    (activity_id,)
                ).fetchone()
                
                if result:
                    logger.info(f"Activity {activity_id} already exists in metadata store. Skipping.")
                    return

                cursor.execute("""
                    INSERT INTO activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    activity_id,
                    activity_data.get('activityName'),
                    activity_data.get('activityType'),
                    activity_data.get('startTimeGMT'),
                    activity_data.get('startTimeLocal'),
                    activity_data.get('distance'),
                    activity_data.get('duration'),
                    activity_data.get('calories'),
                    activity_data.get('averageHR'),
                    activity_data.get('maxHR'),
                    activity_data.get('averagePower'),
                    activity_data.get('maxPower'),
                    file_path
                ))
            logger.info(f"Stored metadata for activity {activity_id} in {self.db_path}")
            
        except DatabaseConnectionError:
            raise
        except Exception as e:
            error_msg = f"Failed to store metadata for activity {activity_data.get('activityId', 'unknown')}: {e}"
            logger.error(error_msg)
            raise DatabaseOperationError(error_msg) from e

    def get_all_activities(self) -> pd.DataFrame:
        """Retrieve all activities from the database."""
        try:
            if self.conn is None:
                raise DatabaseConnectionError("Database connection is not available")
            
            with self.conn.cursor() as cursor:
                result = cursor.execute("SELECT * FROM activities").fetchdf()
            logger.debug(f"Retrieved {len(result)} activities from {self.db_path}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to retrieve activities from {self.db_path}: {e}"
            logger.error(error_msg)
            raise DatabaseOperationError(error_msg) from e

    def close(self):
        """Close the database connection."""
        try:
            if self.conn is not None:
                self.conn.close()
                self.conn = None
                logger.info(f"Database connection to {self.db_path} closed")
        except Exception as e:
            logger.warning(f"Error closing database connection to {self.db_path}: {e}")

