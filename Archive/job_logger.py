import configparser
from google.cloud import bigquery
from datetime import datetime, timezone
import pytz
import time
import numpy as np
import pandas as pd


# Read the configuration details
config = configparser.ConfigParser()
config.read('config.ini')
project_id = config.get('DEFAULT', 'target_project')
dataset_id = config.get('DEFAULT', 'target_dataset')
log_id = config.get('DEFAULT', 'log_table')
job_id = config.get('DEFAULT', 'job_table')
log_table_id = f'{project_id}.{dataset_id}.{log_id}'
job_table_id = f'{project_id}.{dataset_id}.{job_id}'

# Create a BigQuery client
client = bigquery.Client()

# Function to log messages to BigQuery
def log_message(job_id, job_type, message):
    log_table_ref = bigquery.TableReference.from_string(log_table_id)
    log_table = client.get_table(log_table_ref)

    rows_to_insert = [(job_id, job_type, message)]
    rows_to_insert = [tuple(value if not isinstance(value, type(None)) else 'None' for value in row) for row in rows_to_insert]

    errors = client.insert_rows(log_table, rows_to_insert)

    retries = 3
    retry_delay = 1

    while errors and retries > 0:
        time.sleep(retry_delay)
        errors = client.insert_rows(log_table, rows_to_insert)
        retries -= 1

    if errors:
        print(f"Failed to log message: {message}. Errors: {errors}")
    else:
        print(f"Message logged: {message}")

def create_new_job(job_type):
    job_table_schema = [
        bigquery.SchemaField("Job", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("Job Type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Start Time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("End Time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("Success", "STRING", mode="NULLABLE"),
    ]

    job_table_ref = bigquery.TableReference.from_string(job_table_id)
    job_table = bigquery.Table(job_table_ref, schema=job_table_schema)

    # Retrieve the maximum job ID from the table
    query = f'SELECT MAX(Job) FROM `{job_table_id}`'
    job_id_query = client.query(query)
    job_id_result = job_id_query.result().to_dataframe()

    job_id = job_id_result.iloc[0, 0]

    if pd.isnull(job_id):
        job_id = 1
    else:
        job_id = int(job_id) + 1

    start_time = datetime.now(pytz.timezone('US/Eastern')).replace(microsecond=0)
    start_time_utc = start_time.astimezone(timezone.utc)

    rows_to_insert = [(job_id, job_type, start_time_utc, None, 'In progress')]
    
    errors = client.insert_rows(job_table, rows_to_insert)

    if errors:
        print(f"Failed to create new job: {job_type}. Errors: {errors}")
        log_message(None, job_type, f"Failed to create new job: {job_type}")
    else:
        log_message(job_id, job_type, f"New job created: {job_type}")

    return job_id

def update_job_table(job_id, success_status):
    end_time = datetime.now(pytz.timezone('US/Eastern')).isoformat()

    update_query = f"""
        UPDATE `{job_table_id}`
        SET `End Time` = TIMESTAMP "{end_time}", Success = "{success_status}"
        WHERE `Job` = {job_id}
    """


    client.query(update_query).result()
