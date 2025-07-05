import logging
import datetime
import os
from getpass import getpass

from garminconnect import (
    Garmin,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
    GarminConnectAuthenticationError,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GarminConnectAPI:
    def __init__(self, email=None, password=None):
        self.email = email if email else os.getenv("GARMIN_EMAIL")
        self.password = password if password else os.getenv("GARMIN_PASSWORD")
        self.client = None

    def login(self):
        if not self.email:
            self.email = input("Enter your Garmin Connect email: ")
        if not self.password:
            self.password = getpass("Enter your Garmin Connect password: ")

        try:
            self.client = Garmin(self.email, self.password)
            self.client.login()
            logger.info("Successfully logged in to Garmin Connect.")
        except (
            GarminConnectConnectionError,
            GarminConnectTooManyRequestsError,
            GarminConnectAuthenticationError,
        ) as e:
            logger.error(f"An error occurred during login: {e}")
            raise

    def get_activities_by_date(self, start_date, end_date):
        if not self.client:
            self.login()
        try:
            activities = self.client.get_activities_by_date(start_date, end_date)
            return activities
        except Exception as e:
            logger.error(f"An error occurred while fetching activities: {e}")
            raise

    def download_activity_fit(self, activity_id, file_name=None):
        if not self.client:
            self.login()
        try:
            fit_data = self.client.download_activity(
                activity_id, dl_fmt=self.client.ActivityDownloadFormat.FIT
            )
            if not file_name:
                file_name = f"activity_{activity_id}.fit"
            with open(file_name, "wb") as f:
                f.write(fit_data)
            logger.info(f"FIT file saved as '{file_name}'")
            return file_name
        except Exception as e:
            logger.error(f"An error occurred while downloading FIT file: {e}")
            raise

    def logout(self):
        if self.client and self.client.display_name:
            self.client.logout()
            logger.info("Logged out from Garmin Connect.")

