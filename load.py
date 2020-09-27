"""
load.py

Functions to load data into the database.
"""

def upload_row(table_name, row_dict, client):
    errors = client.insert_rows_json(table_name, [row_dict])

    if errors:
        print(errors)
