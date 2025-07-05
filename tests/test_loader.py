import pandas as pd
import pytest
from fitanalysis.loader import FitDataLoader, FitParseError, max_power_by_time
import fitanalysis.loader as loader_module
from fitanalysis.dummy_data import DummyFitFile


@pytest.fixture(autouse=True)
def patch_fitfile(monkeypatch):
    monkeypatch.setattr(loader_module, 'FitFile', DummyFitFile)


def test_load_creates_dataframe():
    loader = FitDataLoader('dummy.fit')
    df = loader.load()
    assert list(df.index) == [pd.Timestamp('2020-01-01T00:00:00Z'), pd.Timestamp('2020-01-01T00:00:01Z'), pd.Timestamp('2020-01-01T00:00:02Z')]
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

def test_max_power_by_time():
    """
    Tests the max_power_by_time function.
    """
    max_power = max_power_by_time('dummy.fit')
    assert isinstance(max_power, pd.Series)
    assert max_power.to_dict() == {
        pd.to_datetime('2020-01-01T00:00:00Z').time(): 150,
        pd.to_datetime('2020-01-01T00:00:01Z').time(): 151,
        pd.to_datetime('2020-01-01T00:00:02Z').time(): 152,
    }
