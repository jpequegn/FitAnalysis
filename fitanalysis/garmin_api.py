import logging
import datetime
import os
from getpass import getpass
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from fitanalysis.metadata_store import MetadataStore
from fitanalysis.config import get_config, FitAnalysisConfig

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
    """A wrapper for the Garmin Connect API to download FIT files."""

    def __init__(self, email: Optional[str] = None, password: Optional[str] = None, db_path: Optional[str] = None, config: Optional[FitAnalysisConfig] = None):
        """
        Initializes the GarminConnectAPI.

        Args:
            email: Garmin Connect email.
            password: Garmin Connect password.
            db_path: Path to the metadata database.
            config: A FitAnalysisConfig object.
        """
        load_dotenv()  # Load environment variables from .env file

        if config is None:
            config = get_config()

        self._config = config
        self._email = email
        self._password = password
        self._client: Optional[Garmin] = None
        
        db_path = db_path or self._config.database.path
        self.metadata_store = MetadataStore(db_path=db_path)

    @property
    def client(self) -> Garmin:
        """
        Returns a logged-in Garmin Connect client.
        If the client is not logged in, it will prompt for credentials and log in.
        """
        if self._client is None:
            email = self._email or self._config.garmin.email or os.getenv("GARMIN_EMAIL")
            password = self._password or self._config.garmin.password or os.getenv("GARMIN_PASSWORD")

            if not email:
                email = input("Enter your Garmin Connect email: ")
            if not password:
                password = getpass("Enter your Garmin Connect password: ")

            try:
                self._client = Garmin(email, password)
                self._client.login()
                logger.info(f"Successfully logged in to Garmin Connect as {self._client.display_name}.")
            except GarminConnectAuthenticationError as e:
                logger.error(f"Authentication failed for user {email}: {e}")
                raise
            except GarminConnectConnectionError as e:
                logger.error(f"Connection error during login: {e}")
                raise
            except GarminConnectTooManyRequestsError as e:
                logger.error(f"Too many requests during login: {e}")
                raise
            except Exception as e:
                logger.error(f"An unexpected error occurred during login: {e}")
                raise
        return self._client

    def get_activities_by_date(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Fetches activities from Garmin Connect between two dates.

        Args:
            start_date: The start date in YYYY-MM-DD format.
            end_date: The end date in YYYY-MM-DD format.

        Returns:
            A list of activities.
        """
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

    def download_activity_fit(self, activity_id: int, activity_data: Dict[str, Any], file_name: Optional[str] = None) -> Optional[str]:
        """
        Downloads a FIT file for a given activity ID.

        Args:
            activity_id: The ID of the activity to download.
            activity_data: The activity data dictionary.
            file_name: The name of the file to save the FIT data to.

        Returns:
            The path to the downloaded file, or None if the download failed.
        """
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
            logger.info(f"FIT file for activity {activity_id} saved as '{file_name}'")
            
            self.metadata_store.store_activity_metadata(activity_data, file_name)
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
        """Logs out from Garmin Connect."""
        if self._client and self._client.display_name:
            display_name = self._client.display_name
            self._client.logout()
            self._client = None
            logger.info(f"Logged out {display_name} from Garmin Connect.")

