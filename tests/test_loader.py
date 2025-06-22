import pandas as pd
import pytest
from fitanalysis.loader import FitDataLoader
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
        return [
            DummyRecord('2020-01-01T00:00:00Z', hr=100, power=150),
            DummyRecord('2020-01-01T00:00:01Z', hr=101, power=151),
            DummyRecord('2020-01-01T00:00:02Z', hr=None, power=152),
        ]

@pytest.fixture(autouse=True)
def patch_fitfile(monkeypatch):
    # Patch FitFile used in loader to our dummy
    monkeypatch.setattr(loader_module, 'FitFile', DummyFitFile)


def test_load_creates_dataframe():
    loader = FitDataLoader('dummy.fit')
    df = loader.load()
    assert list(df.index) == ['2020-01-01T00:00:00Z', '2020-01-01T00:00:01Z', '2020-01-01T00:00:02Z']
    assert 'heart_rate' in df.columns
    assert 'power' in df.columns
    assert df.loc['2020-01-01T00:00:00Z', 'heart_rate'] == 100
    assert df.loc['2020-01-01T00:00:01Z', 'power'] == 151
    assert pd.isna(df.loc['2020-01-01T00:00:02Z', 'heart_rate'])


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


def test_empty_file(monkeypatch):
    class EmptyFitFile:
        def __init__(self, file_path):
            pass

        def get_messages(self, msg_type):
            return []

    monkeypatch.setattr(loader_module, 'FitFile', EmptyFitFile)
    loader = FitDataLoader('dummy.fit')
    df = loader.load()
    assert df.empty
    assert loader.get_heart_rate().empty
    assert loader.get_power().empty


def test_power_only(monkeypatch):
    class PowerOnlyFitFile:
        def __init__(self, file_path):
            pass

        def get_messages(self, msg_type):
            return [DummyRecord('t1', power=200), DummyRecord('t2', power=201)]

    monkeypatch.setattr(loader_module, 'FitFile', PowerOnlyFitFile)
    loader = FitDataLoader('dummy.fit')
    df = loader.load()
    assert 'heart_rate' not in df.columns
    assert 'power' in df.columns
    hr = loader.get_heart_rate()
    assert hr.empty
    power = loader.get_power()
    assert list(power) == [200, 201]
