import pandas as pd
import pytest
from fitanalysis.loader import FitDataLoader, FitParseError, FitFileNotFoundError, FitFileCorruptedError
import fitanalysis.loader as loader_module
from fitanalysis.dummy_data import DummyFitFile
import os

@pytest.fixture
def dummy_fit_files(tmp_path):
    fit_files = {
        "dummy.fit": b"",
        "power_only.fit": b"",
        "hr_only.fit": b"",
        "empty.fit": b"",
        "corrupt.fit": b"",
    }
    for name, content in fit_files.items():
        (tmp_path / name).write_bytes(content)
    return tmp_path

@pytest.fixture(autouse=True)
def patch_fitfile(monkeypatch):
    monkeypatch.setattr(loader_module, 'FitFile', DummyFitFile)

def test_load_creates_dataframe(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'dummy.fit'))
    df = loader.data
    assert list(df.index) == [pd.Timestamp('2020-01-01T00:00:00Z'), pd.Timestamp('2020-01-01T00:00:01Z'), pd.Timestamp('2020-01-01T00:00:02Z')]
    assert 'heart_rate' in df.columns
    assert 'power' in df.columns
    assert df.loc[pd.Timestamp('2020-01-01T00:00:00Z'), 'heart_rate'] == 100
    assert df.loc[pd.Timestamp('2020-01-01T00:00:01Z'), 'power'] == 151
    assert pd.isna(df.loc[pd.Timestamp('2020-01-01T00:00:02Z'), 'heart_rate'])

def test_get_heart_rate_series(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'dummy.fit'))
    hr = loader.get_heart_rate()
    assert isinstance(hr, pd.Series)
    assert hr.name == 'heart_rate'
    assert hr.iloc[0] == 100
    assert hr.iloc[1] == 101
    assert pd.isna(hr.iloc[2])

def test_get_power_series(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'dummy.fit'))
    power = loader.get_power()
    assert isinstance(power, pd.Series)
    assert power.name == 'power'
    assert list(power) == [150, 151, 152]

def test_empty_file(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'empty.fit'))
    df = loader.data
    assert df.empty
    assert loader.get_heart_rate().empty
    assert loader.get_power().empty

def test_power_only(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'power_only.fit'))
    df = loader.data
    assert 'heart_rate' not in df.columns
    assert 'power' in df.columns
    hr = loader.get_heart_rate()
    assert hr.empty
    power = loader.get_power()
    assert list(power) == [200, 201]

def test_hr_only(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'hr_only.fit'))
    df = loader.data
    assert 'heart_rate' in df.columns
    assert 'power' not in df.columns
    hr = loader.get_heart_rate()
    assert list(hr) == [120, 121]
    power = loader.get_power()
    assert power.empty

def test_corrupt_file(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'corrupt.fit'))
    with pytest.raises(FitFileCorruptedError, match="Error parsing FIT file.*Corrupted file"):
        _ = loader.data

def test_max_power_by_time(dummy_fit_files):
    """
    Tests the max_power_by_time function.
    """
    max_power = FitDataLoader.max_power_by_time(str(dummy_fit_files / 'dummy.fit'))
    assert isinstance(max_power, pd.Series)
    assert max_power.to_dict() == {
        pd.to_datetime('2020-01-01T00:00:00Z').time(): 150,
        pd.to_datetime('2020-01-01T00:00:01Z').time(): 151,
        pd.to_datetime('2020-01-01T00:00:02Z').time(): 152,
    }

def test_get_normalized_power(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'dummy.fit'))
    np_value = loader.get_normalized_power()
    assert np_value == pytest.approx(150.66, rel=1e-2)

def test_get_intensity_factor(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'dummy.fit'))
    ftp = 200
    if_value = loader.get_intensity_factor(ftp)
    assert if_value == pytest.approx(0.7533, rel=1e-2)

def test_get_training_stress_score(dummy_fit_files):
    loader = FitDataLoader(str(dummy_fit_files / 'dummy.fit'))
    ftp = 200
    tss_value = loader.get_training_stress_score(ftp)
    assert tss_value == pytest.approx(0.0315, rel=1e-2)

def test_file_not_found():
    with pytest.raises(FitFileNotFoundError):
        FitDataLoader('non_existent_file.fit')
