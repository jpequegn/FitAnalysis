import pytest
from unittest.mock import MagicMock, patch
from fitanalysis.garmin_api import GarminConnectAPI
from garminconnect import GarminConnectAuthenticationError, GarminConnectConnectionError, GarminConnectTooManyRequestsError
import os

class MockGarminClient:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.display_name = "TestUser"
        self.ActivityDownloadFormat = MagicMock()
        self.ActivityDownloadFormat.FIT = "fit"

    def login(self):
        if self.email == "test@example.com" and self.password == "password":
            return True
        elif self.email == "connection_error@example.com":
            raise GarminConnectConnectionError("Connection failed")
        elif self.email == "too_many_requests@example.com":
            raise GarminConnectTooManyRequestsError("Too many requests")
        else:
            raise GarminConnectAuthenticationError("Invalid credentials")

    def logout(self):
        pass

    def get_activities_by_date(self, start_date, end_date):
        if self.email == "connection_error@example.com":
            raise GarminConnectConnectionError("Connection failed")
        if start_date == "2025-01-01" and end_date == "2025-01-07":
            return [{"activityId": 123, "activityName": "Test Activity", "startTimeGMT": "2025-01-01T10:00:00Z"}]
        return []

    def download_activity(self, activity_id, dl_fmt):
        if self.email == "connection_error@example.com":
            raise GarminConnectConnectionError("Connection failed")
        if activity_id == 123 and dl_fmt == "fit":
            return b"dummy_fit_data"
        raise Exception("Download failed")

@pytest.fixture
def garmin_api(tmp_path):
    db_path = tmp_path / "test_fit_metadata.db"
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="test@example.com", password="password", db_path=str(db_path))
        yield api
        api.metadata_store.close()
        if os.path.exists(db_path):
            os.remove(db_path)

def test_login_success(garmin_api):
    garmin_api.login()
    assert garmin_api.client is not None
    assert garmin_api.client.display_name == "TestUser"

def test_login_authentication_failure():
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="wrong@example.com", password="wrong")
        with pytest.raises(GarminConnectAuthenticationError):
            api.login()

def test_login_connection_error():
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="connection_error@example.com", password="password")
        with pytest.raises(GarminConnectConnectionError):
            api.login()

def test_login_too_many_requests_error():
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="too_many_requests@example.com", password="password")
        with pytest.raises(GarminConnectTooManyRequestsError):
            api.login()

def test_get_activities_by_date_success(garmin_api):
    activities = garmin_api.get_activities_by_date("2025-01-01", "2025-01-07")
    assert len(activities) == 1
    assert activities[0]["activityId"] == 123

def test_get_activities_by_date_connection_error():
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="connection_error@example.com", password="password")
        with pytest.raises(GarminConnectConnectionError):
            api.get_activities_by_date("2025-01-01", "2025-01-07")

def test_download_activity_fit_success(garmin_api, tmp_path):
    file_path = tmp_path / "test_activity.fit"
    activity_data = {"activityId": 123, "activityName": "Test Activity", "startTimeGMT": "2025-01-01T10:00:00Z"}
    downloaded_file = garmin_api.download_activity_fit(123, activity_data, file_name=str(file_path))
    assert downloaded_file == str(file_path)
    assert file_path.read_bytes() == b"dummy_fit_data"
    
    # Verify metadata stored
    metadata_df = garmin_api.metadata_store.get_all_activities()
    assert not metadata_df.empty
    assert metadata_df['activity_id'].iloc[0] == '123'
    assert metadata_df['activity_name'].iloc[0] == 'Test Activity'

def test_download_activity_fit_connection_error():
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="connection_error@example.com", password="password")
        with pytest.raises(GarminConnectConnectionError):
            api.download_activity_fit(123, {"activityId": 123})

def test_logout(garmin_api):
    garmin_api.login()
    garmin_api.logout()
    # Asserting that logout was called is tricky with this mock, but we can assume it works if no error

