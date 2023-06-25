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

# Global variable to store the last created job ID
job_id_memory = None

def log_message(job_id, job_type, message):
    """
    Function to log a message in the log table in BigQuery.

    Args:
        job_id (int): The ID of the job.
        job_type (str): The type of the job.
        message (str): The message to be logged.

    Returns:
        None
    """
    # Get the reference to the log table
    log_table_ref = bigquery.TableReference.from_string(log_table_id)
    log_table = client.get_table(log_table_ref)

    # Prepare the row to be inserted
    rows_to_insert = [(job_id, job_type, message)]
    rows_to_insert = [tuple(value if not isinstance(value, type(None)) else 'None' for value in row) for row in rows_to_insert]

    # Insert the row into the log table and handle potential errors
    errors = client.insert_rows(log_table, rows_to_insert)
    retries = 3  # Number of retry attempts
    retry_delay = 1  # Delay between retries in seconds

    while errors and retries > 0:
        time.sleep(retry_delay)
        errors = client.insert_rows(log_table, rows_to_insert)
        retries -= 1

    if errors:
        print(f"Failed to log message: {message}. Errors: {errors}")
    else:
        print(f"Message logged: {message}")

def create_new_job(job_type):
    """
    Function to generate a new job ID and store the job details in memory.

    Args:
        job_type (str): The type of the job.

    Returns:
        job_id (int): The ID of the new job.
    """
    # Get the reference to the job table
    job_table_ref = bigquery.TableReference.from_string(job_table_id)
    job_table = client.get_table(job_table_ref)

    # Retrieve the maximum job ID from the table
    query = f'SELECT MAX(Job) FROM `{job_table_id}`'
    job_id_query = client.query(query)
    job_id_result = job_id_query.result().to_dataframe()

    # Get the new job ID
    job_id = job_id_result.iloc[0, 0]
    job_id = 1 if pd.isnull(job_id) else int(job_id) + 1

    # Get the current time in UTC
    start_time = datetime.now(pytz.timezone('US/Eastern')).replace(microsecond=0)
    start_time_utc = start_time.astimezone(timezone.utc)

    # Store the job ID and start time in memory for future use
    global job_id_memory, start_time_memory
    job_id_memory = job_id
    start_time_memory = start_time_utc

    return job_id

def update_job_table(job_id, job_type, success_status):
    """
    Function to insert a new row into the job table in BigQuery when a job is complete.

    Args:
        job_id (int): The ID of the job.
        success_status (str): The status of the job ('Yes' or 'No').

    Returns:
        None
    """
    # Validate the success_status
    if success_status not in ['Yes', 'No']:
        print("Invalid success status. Only 'Yes' or 'No' are allowed.")
        return

    # Get the current time in UTC
    end_time = datetime.now(pytz.timezone('US/Eastern')).isoformat()

    job_table_ref = bigquery.TableReference.from_string(job_table_id)
    job_table = client.get_table(job_table_ref)

    # Prepare the row to be inserted into the job table
    rows_to_insert = [(job_id, job_type, start_time_memory, end_time, success_status)]

    # Insert the row into the job table
    errors = client.insert_rows(job_table, rows_to_insert)

    if errors:
        print(f"Failed to update job table for job: {job_id}. Errors: {errors}")
    else:
        print(f"Job table updated for job: {job_id}")
