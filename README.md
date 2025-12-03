# OCADS Fetcher and Extractor

## Overview

The **OCADS Fetcher and Extractor** is a Python toolset designed to automate the retrieval and extraction of ocean acidification and carbon data from the Ocean Carbon and Acidification Data System (OCADS) managed by NOAA. This tool is useful for researchers and scientists working with large volumes of oceanographic data.

The project consists of two main scripts:
1. `ocads_fetcher.py`: Queries and retrieves metadata and data links from OCADS.
2. `ocads_extractor.py`: Downloads the actual data & metadata files (XML, FTP, or HTTP) based on the results from the fetcher.

## Features
- Fetches data from NOAA's OCADS API based on flexible query criteria.
- Supports bounding box, keywords, and additional search terms.
- Handles retries and rate limits to ensure robust data fetching.
- Downloads data files from HTTP and FTP sources.
- Organized data extraction and storage in structured directories.

## Prerequisites
- Python 3.13+
- [uv](https://github.com/astral-sh/uv) - A fast Python package installer and resolver

### Python Libraries
Install dependencies using uv:
```bash
uv sync
```

## Usage

### Configuration

Create a `.env` file in the project root by copying `.env.example`:

```bash
cp .env.example .env
```

Then edit `.env` with your desired search parameters:

```env
BBOX=-150.00000,40.00000,-40.00000,90.00000
EXTRA_TERMS=pH
KEYWORDS=
TIME_RANGE=
RESULTS_FILENAME=ocads_results.json
DATASETS_DIR=datasets
```

**Configuration options:**
- `BBOX`: Bounding box coordinates (longitude, latitude) in format `west_lon,south_lat,east_lon,north_lat`
- `EXTRA_TERMS`: Comma-separated search terms
- `KEYWORDS`: Specific keywords for filtering
- `TIME_RANGE`: Time range in format `YYYY-MM-DD/YYYY-MM-DD`
- `RESULTS_FILENAME`: Output filename for fetched results (default: `ocads_results.json`)
- `DATASETS_DIR`: Output directory for extracted data (default: `datasets`)

### Fetching OCADS Results
To run the data fetching script, execute:
```bash
uv run python ocads_fetcher.py
```
You can customize the query by modifying the parameters in the `fetch_ocads_results` function call.

**Parameters:**
- `bbox` (str): Bounding box coordinates (longitude, latitude).
- `extra_terms` (list): Additional search terms.
- `keywords` (str): Specific keywords for filtering.
- `time_range` (str): Time range in the format `YYYY-MM-DD/YYYY-MM-DD`.
- `filename` (str): Output filename (default is `ocads_results.json`).

### Extracting Data from Results
To download and extract the data files:
```bash
uv run python ocads_extractor.py
```
This will process the `ocads_results.json` file and download the relevant files into the `datasets/` directory.

## Example
An example run to fetch data related to pH measurements in the North Pacific:
```python
fetch_ocads_results(
    bbox="-150.00000,40.00000,-40.00000,90.00000",
    extra_terms=["pH"],
    keywords="carbon data"
)
```

