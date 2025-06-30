import pandas as pd
import pytest
from fitanalysis.loader import FitDataLoader, FitParseError
import fitanalysis.loader as loader_module


class DummyRecord:
    def __init__(self, timestamp, hr=None, power=None):
        self._values = {'timestamp': timestamp}
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
            return [DummyRecord('t1', power=200), DummyRecord('t2', power=201)]
        elif self.file_path == 'hr_only.fit':
            return [DummyRecord('t1', hr=120), DummyRecord('t2', hr=121)]
        elif self.file_path == 'empty.fit':
            return []
        elif self.file_path == 'corrupt.fit':
            raise FitParseError("Corrupted file")
        return []


@pytest.fixture(autouse=True)
def patch_fitfile(monkeypatch):
    monkeypatch.setattr(loader_module, 'FitFile', DummyFitFile)


def test_load_creates_dataframe():
    loader = FitDataLoader('dummy.fit')
    df = loader.load()
    assert list(df.index) == pd.to_datetime(['2020-01-01T00:00:00Z', '2020-01-01T00:00:01Z', '2020-01-01T00:00:02Z']).tolist()
    assert 'heart_rate' in df.columns
    assert 'power' in df.columns
    assert df.loc[pd.Timestamp('2020-01-01T00:00:00Z'), 'heart_rate'] == 100
    assert df.loc[pd.Timestamp('2020-01-01T00:00:01Z'), 'power'] == 151
    assert pd.isna(df.loc[pd.Timestamp('2020-01-01T00:00:02Z'), 'heart_rate'])


def test_get_heart_rate_series():
    loader = FitDataLoader('dummy.fit')
    hr = loader.get_heart_rate()
    assert isinstance(hr, pd.Series)
    assert hr.name == 'heart_rate'
    assert hr.iloc[0] == 100
    assert hr.iloc[1] == 101
    assert pd.isna(hr.iloc[2])


def test_get_power_series():
    loader = FitDataLoader('dummy.fit')
    power = loader.get_power()
    assert isinstance(power, pd.Series)
    assert power.name == 'power'
    assert list(power) == [150, 151, 152]


def test_empty_file():
    loader = FitDataLoader('empty.fit')
    df = loader.load()
    assert df.empty
    assert loader.get_heart_rate().empty
    assert loader.get_power().empty


def test_power_only():
    loader = FitDataLoader('power_only.fit')
    df = loader.load()
    assert 'heart_rate' not in df.columns
    assert 'power' in df.columns
    hr = loader.get_heart_rate()
    assert hr.empty
    power = loader.get_power()
    assert list(power) == [200, 201]


def test_hr_only():
    loader = FitDataLoader('hr_only.fit')
    df = loader.load()
    assert 'heart_rate' in df.columns
    assert 'power' not in df.columns
    hr = loader.get_heart_rate()
    assert list(hr) == [120, 121]
    power = loader.get_power()
    assert power.empty


def test_corrupt_file():
    loader = FitDataLoader('corrupt.fit')
    with pytest.raises(IOError, match="Error parsing FIT file: Corrupted file"):
        loader.load()
