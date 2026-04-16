"""
Open-Meteo Air Quality Data Collector

This module fetches historical air quality data from the Open-Meteo Air Quality API
for Aarhus, Denmark and stores it as a CSV file in the data/raw/ directory.

API Documentation: https://open-meteo.com/en/docs/air-quality-api
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_air_quality_data() -> Optional[pd.DataFrame]:
    """
    Fetch historical air quality data from Open-Meteo API for Aarhus, Denmark.

    Retrieves PM2.5 and NO2 (nitrogen dioxide) hourly measurements for the past 30 days
    and saves them to a CSV file in the data/raw/ directory.

    Parameters
    ----------
    None

    Returns
    -------
    Optional[pd.DataFrame]
        DataFrame with columns: timestamp (datetime), pm2_5, no2.
        Returns None if the operation fails.

    Raises
    ------
    requests.exceptions.RequestException
        If the API request fails.
    """
    # API configuration
    API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
    LATITUDE = 56.1567
    LONGITUDE = 10.2108
    PAST_DAYS = 30
    TIMEZONE = "Europe/Berlin"

    # Parameters for the API request
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "pm2_5,nitrogen_dioxide",
        "past_days": PAST_DAYS,
        "timezone": TIMEZONE,
    }

    logger.info(f"Fetching air quality data for Aarhus, Denmark (lat={LATITUDE}, lon={LONGITUDE})")
    logger.info(f"Parameters: past_days={PAST_DAYS}, timezone={TIMEZONE}")

    try:
        # Make API request
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()  # Raise exception for HTTP errors
        logger.info(f"API request successful. Status code: {response.status_code}")

    except requests.exceptions.Timeout:
        logger.error("API request timed out after 10 seconds")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        return None

    try:
        # Parse JSON response
        data = response.json()
        logger.info("JSON response parsed successfully")

        # Extract hourly data
        hourly_data = data.get("hourly", {})
        timestamps = hourly_data.get("time", [])
        pm2_5_values = hourly_data.get("pm2_5", [])
        no2_values = hourly_data.get("nitrogen_dioxide", [])

        if not timestamps:
            logger.warning("No data returned from API")
            return None

        # Create DataFrame
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(timestamps),
            "pm2_5": pm2_5_values,
            "no2": no2_values,
        })

        logger.info(f"DataFrame created with {len(df)} rows")
        logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

        # Define output path using pathlib
        # Resolve path relative to the script's location
        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parent.parent  # Navigate from src/collectors to project root
        data_raw_dir = project_root / "data" / "raw"

        # Create data/raw directory if it doesn't exist
        data_raw_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Data directory ensured: {data_raw_dir}")

        # Save to CSV
        output_file = data_raw_dir / "aarhus_air_quality_raw.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Data saved to {output_file}")
        logger.info(f"Total rows written: {len(df)}")

        return df

    except KeyError as e:
        logger.error(f"Key error while parsing API response: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while processing data: {e}")
        return None


if __name__ == "__main__":
    logger.info("Starting air quality data collection")
    result_df = fetch_air_quality_data()

    if result_df is not None:
        logger.info("Air quality data collection completed successfully")
        sys.exit(0)
    else:
        logger.error("Air quality data collection failed")
        sys.exit(1)
