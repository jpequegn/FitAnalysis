
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


class DummyFitFile:
    def __init__(self, file_path):
        self.file_path = file_path

    def get_messages(self, msg_type):
        if self.file_path == 'dummy.fit':
            return [
                DummyRecord('2020-01-01T00:00:00Z', hr=100, power=150),
                DummyRecord('2020-01-01T00:00:01Z', hr=101, power=151),
                DummyRecord('2020-01-01T00:00:02Z', hr=None, power=152),
            ]
        elif self.file_path == 'power_only.fit':
            return [DummyRecord('2025-07-05T10:00:00Z', power=200), DummyRecord('2025-07-05T10:00:01Z', power=201)]
        elif self.file_path == 'hr_only.fit':
            return [DummyRecord('2025-07-05T10:00:00Z', hr=120), DummyRecord('2025-07-05T10:00:01Z', hr=121)]
        elif self.file_path == 'empty.fit':
            return []
        elif self.file_path == 'corrupt.fit':
            raise FitParseError("Corrupted file")
        return []
