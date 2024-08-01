# ETL Pipeline for Sensor Data

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline for processing and analyzing sensor data. It includes connectors for Google Cloud Storage and BigQuery. The pipeline performs the following tasks:
- Reads data from CSV files
- Validates data using Great Expectations
- Transforms and merges data
- Saves the results in multiple formats
- Queries transformed data using DuckDB

## Project Structure

- **`config.py`**: Contains configuration settings for file paths.
- **`etl.py`**: Implements the `RazonLabs` class with methods for the ETL process.
- **`main_etl.py`**: Entry point for executing the ETL pipeline.
- **`requirements.txt`**: Lists the required Python packages and their versions.
- **`utils/`**: Contains utility modules for connectors to Google Cloud Storage and BigQuery.

## Getting Started

### Set up Configuration

1. Ensure `config.py` has the correct paths for input and output directories. The default configuration is set to use local directories.

### Install Dependencies

1. Install the required Python packages listed in `requirements.txt` using the following command:

    ```bash
    pip install -r requirements.txt
    ```

### Run ETL Pipeline

1. Execute the ETL process by running `main_etl.py`:

    ```bash
    python main_etl.py
    ```

2. The script will read data from the input directory, process it, and save the results in the target directory.

### Check Outputs

1. Verify the processed data and reports in the target directory specified in `config.py`.

### Use Utilities

- **Google Cloud Storage Connector**:
  - **`utils/gcs_connector.py`**: Contains functions to upload and download files from Google Cloud Storage.
  
- **BigQuery Connector**:
  - **`utils/bigquery_connector.py`**: Contains functions to execute queries and load data into BigQuery.

## Dependencies

The project requires the following Python packages:

- `pandas==2.0.3` - Data manipulation and analysis library.
- `duckdb==1.0.0` - In-memory SQL database.
- `great_expectations==0.18.19` - Data validation library.
- `google-cloud-bigquery` - Google BigQuery client library.
- `google-auth` - Google authentication library.
- `google-auth-oauthlib` - OAuth 2.0 library for Google.
- `google-cloud-storage` - Google Cloud Storage client library.
