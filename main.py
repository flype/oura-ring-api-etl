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

def insert_in_bq(table_name, row, client):
    print('{} => {}'.format(table_name, row))
    upload_row(table_name, row, client)

def main():
    """Perform ETL on Oura Ring data."""
    data = load_daily_data()

    sleep_data = transform_sleep_data(data['sleep'])
    activity_data = transform_activity_data(data['activity'])
    readiness_data = transform_readiness_data(data['readiness'])

    client = bigquery.Client()

    print('loading data into bigquery')

    for row in sleep_data:
        insert_in_bq('oura.sleep', row, client)
    for row in activity_data:
        insert_in_bq('oura.activity', row, client)
    for row in readiness_data:
        insert_in_bq('oura.readiness', row, client)


def oura_etl(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    main()

if __name__ == "__main__":
    main()
