
import pandas as pd
from fitparse import FitParseError

class DummyRecord:
    def __init__(self, timestamp, hr=None, power=None):
        self._values = {'timestamp': pd.to_datetime(timestamp)}
        if hr is not None:
            self._values['heart_rate'] = hr
        if power is not None:
            self._values['power'] = power

    def get_value(self, key):
        return self._values.get(key)


import os

class DummyFitFile:
    def __init__(self, file_path):
        self.file_path = file_path

    def get_messages(self, msg_type):
        file_name = os.path.basename(self.file_path)
        if file_name == 'dummy.fit':
            return [
                DummyRecord('2020-01-01T00:00:00Z', hr=100, power=150),
                DummyRecord('2020-01-01T00:00:01Z', hr=101, power=151),
                DummyRecord('2020-01-01T00:00:02Z', hr=None, power=152),
            ]
        elif file_name == 'power_only.fit':
            return [DummyRecord('2025-07-05T10:00:00Z', power=200), DummyRecord('2025-07-05T10:00:01Z', power=201)]
        elif file_name == 'hr_only.fit':
            return [DummyRecord('2025-07-05T10:00:00Z', hr=120), DummyRecord('2025-07-05T10:00:01Z', hr=121)]
        elif file_name == 'empty.fit':
            return []
        elif file_name == 'corrupt.fit':
            raise FitParseError("Corrupted file")
        return []
