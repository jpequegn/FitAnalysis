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
        except GarminConnectAuthenticationError as e:
            logger.error(f"Authentication failed: {e}")
            raise
        except GarminConnectConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise
        except GarminConnectTooManyRequestsError as e:
            logger.error(f"Too many requests: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during login: {e}")
            raise

    def get_activities_by_date(self, start_date, end_date):
        if not self.client:
            self.login()
        try:
            activities = self.client.get_activities_by_date(start_date, end_date)
            logger.info(f"Fetched {len(activities)} activities from {start_date} to {end_date}.")
            return activities
        except GarminConnectConnectionError as e:
            logger.error(f"Connection error while fetching activities: {e}")
            raise
        except GarminConnectTooManyRequestsError as e:
            logger.error(f"Too many requests while fetching activities: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching activities: {e}")
            raise

    def download_activity_fit(self, activity_id, file_name=None):
        if not self.client:
            self.login()
        try:
            fit_data = self.client.download_activity(
                activity_id, dl_fmt=self.client.ActivityDownloadFormat.FIT
            )
            if not fit_data:
                logger.warning(f"No FIT data received for activity ID {activity_id}.")
                return None

            if not file_name:
                file_name = f"activity_{activity_id}.fit"
            with open(file_name, "wb") as f:
                f.write(fit_data)
            logger.info(f"FIT file saved as '{file_name}'")
            return file_name
        except GarminConnectConnectionError as e:
            logger.error(f"Connection error while downloading FIT file for activity ID {activity_id}: {e}")
            raise
        except GarminConnectTooManyRequestsError as e:
            logger.error(f"Too many requests while downloading FIT file for activity ID {activity_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while downloading FIT file for activity ID {activity_id}: {e}")
            raise

    def logout(self):
        if self.client and self.client.display_name:
            self.client.logout()
            logger.info("Logged out from Garmin Connect.")

