from typing import Optional, Dict, Any, Iterator

import pandas as pd
from fitparse import FitFile, FitParseError
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)


class FitDataError(Exception):
    """Base exception for FIT data processing errors."""
    pass


class FitFileNotFoundError(FitDataError):
    """Raised when FIT file is not found."""
    pass


class FitFileCorruptedError(FitDataError):
    """Raised when FIT file is corrupted or cannot be parsed."""
    pass


class FitDataNotLoadedError(FitDataError):
    """Raised when trying to access data that hasn't been loaded."""
    pass


class FitDataLoader:
    """
    Loader for Garmin FIT files.

    This class provides methods to load, access, and analyze data from a FIT file.
    """

    def __init__(self, file_path: str) -> None:
        """
        Initializes the FitDataLoader.

        Args:
            file_path: The path to the FIT file.
        """
        self.file_path = file_path
        self._data: Optional[pd.DataFrame] = None

        if not os.path.exists(file_path):
            raise FitFileNotFoundError(f"FIT file not found: {file_path}")

        if not file_path.lower().endswith('.fit'):
            logger.warning(f"File {file_path} does not have a .fit extension")

        logger.info(f"FitDataLoader initialized for file: {file_path}")

    @property
    def data(self) -> pd.DataFrame:
        """
        Loads the FIT file data into a pandas DataFrame.

        The data is loaded on the first access and then cached.

        Returns:
            A pandas DataFrame containing the FIT file data.
        """
        if self._data is None:
            self._data = self._load()
        return self._data

    def _load(self) -> pd.DataFrame:
        """Loads the FIT file and parses records into a DataFrame."""
        logger.info(f"Loading FIT file: {self.file_path}")

        try:
            fit = FitFile(self.file_path)
            records = list(self._get_records_generator(fit))
            if not records:
                return pd.DataFrame()
            
            df = pd.DataFrame.from_records(records)
            if 'timestamp' in df.columns:
                df.set_index('timestamp', inplace=True)

            if df.empty:
                logger.warning(f"No records found in FIT file: {self.file_path}")
            else:
                logger.info(f"Successfully loaded {len(df)} records from FIT file: {self.file_path}")

            return df

        except FitParseError as e:
            error_msg = f"Error parsing FIT file {self.file_path}: {e}"
            logger.error(error_msg)
            raise FitFileCorruptedError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error loading FIT file {self.file_path}: {e}"
            logger.error(error_msg)
            raise FitDataError(error_msg) from e

    @staticmethod
    def _get_records_generator(fit: FitFile) -> Iterator[Dict[str, Any]]:
        """A generator that yields records from a FIT file."""
        for record in fit.get_messages('record'):
            rec = {'timestamp': record.get_value('timestamp')}
            hr = record.get_value('heart_rate')
            if hr is not None:
                rec['heart_rate'] = hr
            power = record.get_value('power')
            if power is not None:
                rec['power'] = power
            yield rec

    def get_heart_rate(self) -> pd.Series:
        """
        Return heart rate time series as pandas Series.

        Returns:
            A pandas Series containing the heart rate data.
        """
        return self.data.get('heart_rate', pd.Series(dtype='float64'))

    def get_power(self) -> pd.Series:
        """
        Return power time series as pandas Series.

        Returns:
            A pandas Series containing the power data.
        """
        return self.data.get('power', pd.Series(dtype='float64'))

    def get_normalized_power(self) -> float:
        r"""
        Calculate Normalized Power (NP).

        Formula: NP = (mean(rolling_avg(power^4, 30s)))^0.25

        Returns:
            The Normalized Power value.
        """
        power_series = self.get_power()
        if power_series.empty:
            return 0.0

        power_numeric = pd.to_numeric(power_series, errors='coerce').dropna()

        if power_numeric.empty:
            return 0.0

        rolling_avg_power_4 = power_numeric.pow(4).rolling(window='30s', min_periods=1).mean()

        if not rolling_avg_power_4.empty:
            np_value = np.power(rolling_avg_power_4.mean(), 0.25)
            return np_value
        else:
            return 0.0

    def get_intensity_factor(self, ftp: float) -> float:
        """
        Calculate Intensity Factor (IF).

        Formula: IF = NP / FTP

        Args:
            ftp: Functional Threshold Power.

        Returns:
            The Intensity Factor value.
        """
        if ftp <= 0:
            raise ValueError("FTP must be a positive value.")
        np_value = self.get_normalized_power()
        if np_value == 0.0:
            return 0.0
        return np_value / ftp

    def get_training_stress_score(self, ftp: float) -> float:
        r"""
        Calculate Training Stress Score (TSS).

        Formula: TSS = (duration_seconds * NP * IF) / (FTP * 3600) * 100

        Args:
            ftp: Functional Threshold Power.

        Returns:
            The Training Stress Score value.
        """
        if ftp <= 0:
            raise ValueError("FTP must be a positive value.")

        power_series = self.get_power()
        if power_series.empty:
            return 0.0

        duration_seconds = (power_series.index[-1] - power_series.index[0]).total_seconds()
        if_value = self.get_intensity_factor(ftp)
        np_value = self.get_normalized_power()

        if np_value == 0.0 or if_value == 0.0:
            return 0.0

        tss = (duration_seconds * np_value * if_value) / (ftp * 3600) * 100
        return tss

    @staticmethod
    def max_power_by_time(file_path: str) -> pd.DataFrame:
        """
        Calculates the maximum power at each time step from a FIT file.

        Args:
            file_path: The path to the FIT file.

        Returns:
            A DataFrame with the maximum power for each time step.
        """
        fitfile = FitFile(file_path)
        records = []
        for record in fitfile.get_messages('record'):
            records.append({
                'timestamp': record.get_value('timestamp'),
                'power': record.get_value('power')
            })
        df = pd.DataFrame(records)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        max_power = df.groupby(df.index.time)['power'].max()
        return max_power

