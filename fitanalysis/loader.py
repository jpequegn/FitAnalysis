import pandas as pd
from fitparse import FitFile

class FitDataLoader:
    """Loader for Garmin FIT files."""
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def load(self):
        """Load FIT file and parse records into a DataFrame."""
        fit = FitFile(self.file_path)
        records = []
        for record in fit.get_messages('record'):
            rec = {'timestamp': record.get_value('timestamp')}
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

    def get_heart_rate(self):
        """Return heart rate time series as pandas Series."""
        if self.data is None:
            self.load()
        return self.data['heart_rate'] if 'heart_rate' in self.data else pd.Series(dtype='float64')

    def get_power(self):
        """Return power time series as pandas Series."""
        if self.data is None:
            self.load()
        return self.data['power'] if 'power' in self.data else pd.Series(dtype='float64')
