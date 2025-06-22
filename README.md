# FitAnalysis

A Python library to load and analyze Garmin FIT files.

## Installation

Install via pip:

```bash
pip install fitanalysis
```

Or install from source:

```bash
git clone <repo_url>
cd FitAnalysis
pip install -e .
```

## Usage

```python
from fitanalysis import FitDataLoader

# Load the FIT file
loader = FitDataLoader('path/to/file.fit')
df = loader.load()

# Extract heart rate and power time series
hr_series = loader.get_heart_rate()
power_series = loader.get_power()

# Display
print(hr_series.head())
print(power_series.head())
```

The DataFrame `df` returned by `load()` contains all parsed records with timestamps as the index.
