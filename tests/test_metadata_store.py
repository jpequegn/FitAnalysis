import pytest
import tempfile
import os
import pandas as pd
from datetime import datetime
from unittest.mock import patch, MagicMock

from fitanalysis.metadata_store import MetadataStore, DatabaseConnectionError


class TestMetadataStore:
    """Test suite for MetadataStore class."""

    def setup_method(self):
        """Set up test database for each test."""
        fd, self.db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.unlink(self.db_path)

    def teardown_method(self):
        """Clean up test database after each test."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_init_creates_database_and_table(self):
        """Test that initialization creates database and activities table."""
        with MetadataStore(db_path=self.db_path) as store:
            assert os.path.exists(self.db_path)
            
            with store.conn.cursor() as cursor:
                result = cursor.execute(
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
        
        with MetadataStore(db_path=self.db_path) as store:
            with patch('fitanalysis.metadata_store.logger') as mock_logger:
                store.store_activity_metadata(activity_data, file_path)
                mock_logger.info.assert_called_with(f"Stored metadata for activity 12345 in {self.db_path}")
            
            with store.conn.cursor() as cursor:
                result = cursor.execute(
                    "SELECT * FROM activities WHERE activity_id = '12345'"
                ).fetchone()
            
            assert result is not None
            assert result[1] == 'Morning Run'
            assert result[2] == 'Running'
            assert result[5] == 5000.0
            assert result[12] == file_path

    def test_store_activity_metadata_duplicate_activity(self):
        """Test storing metadata for an existing activity (should skip)."""
        activity_data = {
            'activityId': 12345,
            'activityName': 'Morning Run',
            'activityType': 'Running'
        }
        file_path = '/path/to/activity_12345.fit'
        
        with MetadataStore(db_path=self.db_path) as store:
            store.store_activity_metadata(activity_data, file_path)
            
            with patch('fitanalysis.metadata_store.logger') as mock_logger:
                store.store_activity_metadata(activity_data, file_path)
                mock_logger.info.assert_called_with("Activity 12345 already exists in metadata store. Skipping.")
            
            with store.conn.cursor() as cursor:
                result = cursor.execute(
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
        
        with MetadataStore(db_path=self.db_path) as store:
            store.store_activity_metadata(activity_data, file_path)
            
            with store.conn.cursor() as cursor:
                result = cursor.execute(
                    "SELECT * FROM activities WHERE activity_id = '12346'"
                ).fetchone()
            
            assert result is not None
            assert result[1] == 'Cycling'
            assert result[3] is None
            assert result[5] is None
            assert result[8] is None
            assert result[11] is None

    def test_get_all_activities_empty(self):
        """Test get_all_activities with empty database."""
        with MetadataStore(db_path=self.db_path) as store:
            df = store.get_all_activities()
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 0
            assert len(df.columns) == 13

    def test_get_all_activities_with_data(self):
        """Test get_all_activities with stored data."""
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
        
        with MetadataStore(db_path=self.db_path) as store:
            for activity in activities:
                store.store_activity_metadata(
                    activity, f"/path/to/activity_{activity['activityId']}.fit"
                )
            
            df = store.get_all_activities()
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert '12345' in df['activity_id'].values
            assert '12346' in df['activity_id'].values
            assert 'Morning Run' in df['activity_name'].values
            assert 'Evening Bike' in df['activity_name'].values

    def test_database_connection_error(self):
        """Test handling of database connection errors."""
        with pytest.raises(DatabaseConnectionError):
            with MetadataStore(db_path='/invalid/path/database.db') as store:
                pass

    def test_close_connection(self):
        """Test closing database connection."""
        with MetadataStore(db_path=self.db_path) as store:
            assert store.conn is not None
        
        assert store.conn is None

    def test_custom_database_path(self):
        """Test creating MetadataStore with custom database path."""
        custom_path = tempfile.mktemp(suffix='.db')
        
        try:
            with MetadataStore(db_path=custom_path) as custom_store:
                assert custom_store.db_path == custom_path
                assert os.path.exists(custom_path)
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
        
        with MetadataStore(db_path=self.db_path) as store:
            store.store_activity_metadata(activity_data, file_path)
            
            df = store.get_all_activities()
            row = df[df['activity_id'] == '12347'].iloc[0]
            
            assert isinstance(row['activity_name'], str)
            assert isinstance(row['distance'], float)
            assert isinstance(row['duration'], float)
            assert pd.api.types.is_integer_dtype(row['calories'])
            assert pd.api.types.is_integer_dtype(row['average_hr'])
            assert pd.api.types.is_integer_dtype(row['max_hr'])
            assert pd.api.types.is_integer_dtype(row['average_power'])
            assert pd.api.types.is_integer_dtype(row['max_power'])

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts are handled safely."""
        malicious_activity_data = {
            'activityId': "12345'; DROP TABLE activities; --",
            'activityName': 'Malicious Activity',
            'activityType': 'Hacking'
        }
        file_path = '/path/to/malicious.fit'
        
        with MetadataStore(db_path=self.db_path) as store:
            store.store_activity_metadata(malicious_activity_data, file_path)
            
            with store.conn.cursor() as cursor:
                result = cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='activities'"
                ).fetchone()
            assert result is not None
            
            activities = store.get_all_activities()
            assert len(activities) == 1
            assert activities.iloc[0]['activity_name'] == 'Malicious Activity'

    def test_large_dataset_handling(self):
        """Test handling of large datasets."""
        num_activities = 1000
        
        with MetadataStore(db_path=self.db_path) as store:
            for i in range(num_activities):
                activity_data = {
                    'activityId': i,
                    'activityName': f'Activity {i}',
                    'activityType': 'Training',
                    'distance': i * 1000.0,
                    'duration': i * 60.0
                }
                store.store_activity_metadata(
                    activity_data, f"/path/to/activity_{i}.fit"
                )
            
            df = store.get_all_activities()
            
            assert len(df) == num_activities
            assert df['distance'].sum() == sum(i * 1000.0 for i in range(num_activities))
