# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Development Setup
```bash
# Install for development
pip install -e .

# Install with web API dependencies
pip install -e .[web]
```

### Testing
```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v
```

### Web API
```bash
# Run the FastAPI web server
uvicorn main:app --reload

# Access the web interface at http://127.0.0.1:8000
```

## Architecture Overview

### Core Components

**FitDataLoader** (`fitanalysis/loader.py`): Central class for loading and analyzing FIT files. Provides methods for:
- Loading FIT files into pandas DataFrames
- Extracting heart rate and power time series
- Calculating advanced power metrics (Normalized Power, Intensity Factor, TSS)
- Max power analysis by time

**GarminConnectAPI** (`fitanalysis/garmin_api.py`): Handles authentication and data retrieval from Garmin Connect:
- Manages login/logout with environment variable support
- Fetches activities by date range
- Downloads FIT files for activities
- Integrates with metadata storage

**MetadataStore** (`fitanalysis/metadata_store.py`): DuckDB-based storage for activity metadata:
- Stores activity details (name, type, duration, power/HR stats)
- Links metadata to downloaded FIT files
- Provides DataFrame interface for querying activities

**Web API** (`main.py`): FastAPI server for FIT file upload and analysis with basic HTML interface.

### Data Flow

1. **Garmin Connect Integration**: Use `GarminConnectAPI` to authenticate and fetch activities
2. **FIT File Download**: Activities are downloaded as FIT files with metadata stored in DuckDB
3. **Analysis**: Use `FitDataLoader` to parse FIT files and extract time series data
4. **Metrics Calculation**: Advanced power metrics (NP, IF, TSS) calculated from power data
5. **Web Interface**: Upload and analyze FIT files via FastAPI endpoint

### Key Dependencies

- `fitparse`: FIT file parsing
- `pandas`: Data manipulation and time series analysis
- `garminconnect`: Garmin Connect API integration
- `duckdb`: Metadata storage
- `fastapi`: Web API framework
- `python-dotenv`: Environment variable management

### Configuration

- Environment variables for Garmin credentials (`GARMIN_EMAIL`, `GARMIN_PASSWORD`)
- DuckDB database for metadata storage (default: `fit_metadata.db`)
- Temporary file handling for web uploads (`/tmp/` directory)

### Testing Structure

Tests are located in `tests/` directory with pytest configuration. The `conftest.py` sets up path imports for testing the local package.

## Large Codebase Analysis

When analyzing large codebases or multiple files that might exceed context limits, use the Gemini CLI with its large context window:

```bash
# Use Gemini CLI for comprehensive codebase analysis
gemini -p <prompt> <files or directories>

# Examples:
gemini -p "Analyze the entire codebase architecture" .
gemini -p "Compare these modules for consistency" fitanalysis/
gemini -p "Find all dependencies between components" fitanalysis/ tests/
```

This is particularly useful for:
- Cross-file dependency analysis
- Large refactoring tasks
- Understanding complex interactions between components
- Comparing multiple large files for consistency
- Analyzing the entire codebase when context limits are reached