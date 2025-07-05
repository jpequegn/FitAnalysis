from typing import Optional, Dict, Any, List

import pandas as pd
from fitparse import FitFile, FitParseError
import numpy as np


class FitDataLoader:
    """Loader for Garmin FIT files."""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.data: Optional[pd.DataFrame] = None

    def load(self) -> pd.DataFrame:
        """Load FIT file and parse records into a DataFrame."""
        try:
            fit = FitFile(self.file_path)
            records: List[Dict[str, Any]] = []
            for record in fit.get_messages('record'):
                rec: Dict[str, Any] = {'timestamp': record.get_value('timestamp')}
                hr = record.get_value('heart_rate')
                if hr is not None:
                    rec['heart_rate'] = hr
                power = record.get_value('power')
                if power is not None:
                    rec['power'] = power
                records.append(rec)
            df = pd.DataFrame(records)
            if not df.empty:
                df.set_index('timestamp', inplace=True)
            self.data = df
            return df
        except FitParseError as e:
            # Here you could log the error or handle it as you see fit
            raise IOError(f"Error parsing FIT file: {e}") from e

    def get_heart_rate(self) -> pd.Series:
        """Return heart rate time series as pandas Series."""
        if self.data is None:
            self.load()
        # We need to assert that self.data is not None to satisfy mypy
        assert self.data is not None
        return self.data.get('heart_rate', pd.Series(dtype='float64'))

    def get_power(self) -> pd.Series:
        """Return power time series as pandas Series."""
        if self.data is None:
            self.load()
        # We need to assert that self.data is not None to satisfy mypy
        assert self.data is not None
        return self.data.get('power', pd.Series(dtype='float64'))

    def get_normalized_power(self) -> float:
        """Calculate Normalized Power (NP)."""
        if self.data is None:
            self.load()
        assert self.data is not None
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
        """Calculate Intensity Factor (IF)."""
        if ftp <= 0:
            raise ValueError("FTP must be a positive value.")
        np_value = self.get_normalized_power()
        if np_value == 0.0:
            return 0.0
        return np_value / ftp

    def get_training_stress_score(self, ftp: float) -> float:
        """Calculate Training Stress Score (TSS)."""
        if ftp <= 0:
            raise ValueError("FTP must be a positive value.")
        if self.data is None:
            self.load()
        assert self.data is not None
        power_series = self.get_power()
        if power_series.empty:
            return 0.0

        duration_seconds = (power_series.index[-1] - power_series.index[0]).total_seconds()
        duration_hours = duration_seconds / 3600.0

        np_value = self.get_normalized_power()
        if_value = self.get_intensity_factor(ftp)

        if np_value == 0.0 or if_value == 0.0:
            return 0.0

        tss = (duration_seconds * np_value * if_value * 100) / (ftp * 3600)
        return tss

def max_power_by_time(file_path):
    """
    Calculates the maximum power at each time step from a FIT file.

    Args:
        file_path (str): The path to the FIT file.

    Returns:
        pandas.DataFrame: A DataFrame with the maximum power for each time step.
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
