# FitAnalysis

[![Python package](https://github.com/jpequegn/FitAnalysis/actions/workflows/python-package.yml/badge.svg)](https://github.com/jpequegn/FitAnalysis/actions/workflows/python-package.yml)

A Python library to load and analyze Garmin FIT files.

## Features

*   Load FIT files into a pandas DataFrame.
*   Easily access heart rate and power data as time series.
*   Handles missing data points gracefully.

## Installation

Install via pip:

```bash
pip install fitanalysis
```

Or install from source for development:

```bash
git clone https://github.com/jpequegn/FitAnalysis.git
cd FitAnalysis
pip install -e .
```

## Usage

Here's a quick example of how to use `FitAnalysis` to load a FIT file and get some data.

```python
from fitanalysis import FitDataLoader

# Load the FIT file
loader = FitDataLoader('path/to/your/file.fit')
df = loader.load()

# The `load` method returns a pandas DataFrame
print("All data:")
print(df.head())

# You can also get specific time series
hr_series = loader.get_heart_rate()
power_series = loader.get_power()

print("\nHeart Rate:")
print(hr_series.head())

print("\nPower:")
print(power_series.head())
```

## Contributing

Contributions are welcome! If you'd like to contribute, please follow these steps:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Make your changes and add tests.
4.  Run the tests to ensure everything is working correctly:
    ```bash
    pytest
    ```
5.  Submit a pull request.

## License

This project is licensed under the MIT License.