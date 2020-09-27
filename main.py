"""
main.py

Loads yesterday's personal Oura Ring data, and loads it into a data warehouse.
"""

import os

from google.cloud import bigquery

from extract import load_daily_data
from transform import transform_sleep_data
from transform import transform_activity_data
from transform import transform_readiness_data
from load import upload_row


def main():
    """Perform ETL on Oura Ring data."""
    data = load_daily_data()

    sleep_data = transform_sleep_data(data['sleep'])
    activity_data = transform_activity_data(data['activity'])
    readiness_data = transform_readiness_data(data['readiness'])

    client = bigquery.Client()

    for row in sleep_data:
        upload_row('oura.sleep', row, client)
    for row in activity_data:
        upload_row('oura.activity', row, client)
    for row in readiness_data:
        upload_row('oura.readiness', row, client)

    
if __name__ == "__main__":
    main()
