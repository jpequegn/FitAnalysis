import pytest
import tempfile
import os
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock

from fitanalysis.metadata_store import MetadataStore


class TestMetadataStore:
    """Test suite for MetadataStore class."""

    def setup_method(self):
        """Set up test database for each test."""
        # Create a temporary database path (but don't create the file)
        import tempfile
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)  # Close the file descriptor
        os.unlink(self.db_path)  # Remove the empty file so DuckDB can create it properly
        self.metadata_store = MetadataStore(db_path=self.db_path)

    def teardown_method(self):
        """Clean up test database after each test."""
        self.metadata_store.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_init_creates_database_and_table(self):
        """Test that initialization creates database and activities table."""
        # Check that database file was created
        assert os.path.exists(self.db_path)
        
        # Check that activities table exists
        result = self.metadata_store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='activities'"
        ).fetchone()
        assert result is not None

    def test_store_activity_metadata_new_activity(self):
        """Test storing metadata for a new activity."""
        activity_data = {
            'activityId': 12345,
            'activityName': 'Morning Run',
            'activityType': 'Running',
            'startTimeGMT': '2024-01-01T08:00:00Z',
            'startTimeLocal': '2024-01-01T09:00:00+01:00',
            'distance': 5000.0,
            'duration': 1800.0,
            'calories': 300,
            'averageHR': 150,
            'maxHR': 180,
            'averagePower': 200,
            'maxPower': 250
        }
        file_path = '/path/to/activity_12345.fit'
        
        with patch('fitanalysis.metadata_store.logger') as mock_logger:
            self.metadata_store.store_activity_metadata(activity_data, file_path)
            
            # Check that success message was logged
            mock_logger.info.assert_called_with("Stored metadata for activity 12345")
        
        # Verify data was stored
        result = self.metadata_store.conn.execute(
            "SELECT * FROM activities WHERE activity_id = '12345'"
        ).fetchone()
        
        assert result is not None
        assert result[1] == 'Morning Run'  # activity_name
        assert result[2] == 'Running'      # activity_type
        assert result[5] == 5000.0         # distance
        assert result[12] == file_path     # file_path

    def test_store_activity_metadata_duplicate_activity(self):
        """Test storing metadata for an existing activity (should skip)."""
        activity_data = {
            'activityId': 12345,
            'activityName': 'Morning Run',
            'activityType': 'Running'
        }
        file_path = '/path/to/activity_12345.fit'
        
        # Store the activity first time
        self.metadata_store.store_activity_metadata(activity_data, file_path)
        
        # Try to store the same activity again
        with patch('fitanalysis.metadata_store.logger') as mock_logger:
            self.metadata_store.store_activity_metadata(activity_data, file_path)
            
            # Check that skip message was logged
            mock_logger.info.assert_called_with("Activity 12345 already exists in metadata store. Skipping.")
        
        # Verify only one record exists
        result = self.metadata_store.conn.execute(
            "SELECT COUNT(*) FROM activities WHERE activity_id = '12345'"
        ).fetchone()
        assert result[0] == 1

    def test_store_activity_metadata_with_none_values(self):
        """Test storing metadata with None values."""
        activity_data = {
            'activityId': 12346,
            'activityName': 'Cycling',
            'activityType': 'Cycling',
            'startTimeGMT': None,
            'distance': None,
            'averageHR': None,
            'maxPower': None
        }
        file_path = '/path/to/activity_12346.fit'
        
        self.metadata_store.store_activity_metadata(activity_data, file_path)
        
        # Verify data was stored with None values
        result = self.metadata_store.conn.execute(
            "SELECT * FROM activities WHERE activity_id = '12346'"
        ).fetchone()
        
        assert result is not None
        assert result[1] == 'Cycling'      # activity_name
        assert result[3] is None           # start_time_gmt
        assert result[5] is None           # distance
        assert result[8] is None           # average_hr
        assert result[11] is None          # max_power

    def test_get_all_activities_empty(self):
        """Test get_all_activities with empty database."""
        df = self.metadata_store.get_all_activities()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert len(df.columns) == 13  # Number of columns in activities table

    def test_get_all_activities_with_data(self):
        """Test get_all_activities with stored data."""
        # Store multiple activities
        activities = [
            {
                'activityId': 12345,
                'activityName': 'Morning Run',
                'activityType': 'Running',
                'distance': 5000.0,
                'duration': 1800.0
            },
            {
                'activityId': 12346,
                'activityName': 'Evening Bike',
                'activityType': 'Cycling',
                'distance': 15000.0,
                'duration': 3600.0
            }
        ]
        
        for activity in activities:
            self.metadata_store.store_activity_metadata(
                activity, f"/path/to/activity_{activity['activityId']}.fit"
            )
        
        df = self.metadata_store.get_all_activities()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert '12345' in df['activity_id'].values
        assert '12346' in df['activity_id'].values
        assert 'Morning Run' in df['activity_name'].values
        assert 'Evening Bike' in df['activity_name'].values

    def test_database_connection_error(self):
        """Test handling of database connection errors."""
        # Try to create MetadataStore with invalid database path
        with pytest.raises(Exception):
            MetadataStore(db_path='/invalid/path/database.db')

    def test_close_connection(self):
        """Test closing database connection."""
        # Connection should be active initially
        assert self.metadata_store.conn is not None
        
        # Close connection
        self.metadata_store.close()
        
        # Verify connection is closed (attempting to execute should raise an error)
        with pytest.raises(Exception):
            self.metadata_store.conn.execute("SELECT 1")

    def test_custom_database_path(self):
        """Test creating MetadataStore with custom database path."""
        custom_path = tempfile.mktemp(suffix='.db')
        
        try:
            custom_store = MetadataStore(db_path=custom_path)
            assert custom_store.db_path == custom_path
            assert os.path.exists(custom_path)
            custom_store.close()
        finally:
            if os.path.exists(custom_path):
                os.unlink(custom_path)

    def test_activity_data_types(self):
        """Test that activity data types are handled correctly."""
        activity_data = {
            'activityId': 12347,
            'activityName': 'Test Activity',
            'activityType': 'Training',
            'startTimeGMT': '2024-01-01T10:00:00Z',
            'startTimeLocal': '2024-01-01T11:00:00+01:00',
            'distance': 10000.5,
            'duration': 3600.75,
            'calories': 500,
            'averageHR': 145,
            'maxHR': 175,
            'averagePower': 220,
            'maxPower': 300
        }
        file_path = '/path/to/activity_12347.fit'
        
        self.metadata_store.store_activity_metadata(activity_data, file_path)
        
        # Retrieve and verify data types
        df = self.metadata_store.get_all_activities()
        row = df[df['activity_id'] == '12347'].iloc[0]
        
        assert isinstance(row['activity_name'], str)
        assert isinstance(row['distance'], float)
        assert isinstance(row['duration'], float)
        assert isinstance(row['calories'], int)
        assert isinstance(row['average_hr'], int)
        assert isinstance(row['max_hr'], int)
        assert isinstance(row['average_power'], int)
        assert isinstance(row['max_power'], int)

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts are handled safely."""
        # Attempt SQL injection through activity ID
        malicious_activity_data = {
            'activityId': "12345'; DROP TABLE activities; --",
            'activityName': 'Malicious Activity',
            'activityType': 'Hacking'
        }
        file_path = '/path/to/malicious.fit'
        
        # This should not cause any issues due to parameterized queries
        self.metadata_store.store_activity_metadata(malicious_activity_data, file_path)
        
        # Verify table still exists and contains the data
        result = self.metadata_store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='activities'"
        ).fetchone()
        assert result is not None
        
        # Verify the malicious string was stored as data, not executed
        activities = self.metadata_store.get_all_activities()
        assert len(activities) == 1
        assert activities.iloc[0]['activity_name'] == 'Malicious Activity'

    def test_large_dataset_handling(self):
        """Test handling of large datasets."""
        # Store a large number of activities
        num_activities = 1000
        
        for i in range(num_activities):
            activity_data = {
                'activityId': i,
                'activityName': f'Activity {i}',
                'activityType': 'Training',
                'distance': i * 1000.0,
                'duration': i * 60.0
            }
            self.metadata_store.store_activity_metadata(
                activity_data, f"/path/to/activity_{i}.fit"
            )
        
        # Retrieve all activities
        df = self.metadata_store.get_all_activities()
        
        assert len(df) == num_activities
        assert df['distance'].sum() == sum(i * 1000.0 for i in range(num_activities))