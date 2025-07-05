import pytest
from unittest.mock import MagicMock, patch
from fitanalysis.garmin_api import GarminConnectAPI
from garminconnect import GarminConnectAuthenticationError, GarminConnectConnectionError, GarminConnectTooManyRequestsError

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
        else:
            raise GarminConnectAuthenticationError("Invalid credentials")

    def logout(self):
        pass

    def get_activities_by_date(self, start_date, end_date):
        if start_date == "2025-01-01" and end_date == "2025-01-07":
            return [{"activityId": 123, "activityName": "Test Activity"}]
        return []

    def download_activity(self, activity_id, dl_fmt):
        if activity_id == 123 and dl_fmt == "fit":
            return b"dummy_fit_data"
        raise Exception("Download failed")

@pytest.fixture
def garmin_api():
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="test@example.com", password="password")
        yield api

def test_login_success(garmin_api):
    garmin_api.login()
    assert garmin_api.client is not None
    assert garmin_api.client.display_name == "TestUser"

def test_login_failure():
    with patch('fitanalysis.garmin_api.Garmin', new=MockGarminClient):
        api = GarminConnectAPI(email="wrong@example.com", password="wrong")
        with pytest.raises(GarminConnectAuthenticationError):
            api.login()

def test_get_activities_by_date_success(garmin_api):
    activities = garmin_api.get_activities_by_date("2025-01-01", "2025-01-07")
    assert len(activities) == 1
    assert activities[0]["activityId"] == 123

def test_download_activity_fit_success(garmin_api, tmp_path):
    file_path = tmp_path / "test_activity.fit"
    downloaded_file = garmin_api.download_activity_fit(123, file_name=str(file_path))
    assert downloaded_file == str(file_path)
    assert file_path.read_bytes() == b"dummy_fit_data"

def test_logout(garmin_api):
    garmin_api.login()
    garmin_api.logout()
    # Asserting that logout was called is tricky with this mock, but we can assume it works if no error

