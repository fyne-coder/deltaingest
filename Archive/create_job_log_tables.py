import configparser
import google.cloud.exceptions
from google.cloud import bigquery

def create_table_if_not_exists(client, table_id, schema):
    try:
        # Check if the table exists
        client.get_table(table_id)
    except google.cloud.exceptions.NotFound:
        # Create the table if it does not exist
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Created table {table_id}")

def main(config_path):
    # Parse the config file
    config = configparser.ConfigParser()
    config.read(config_path)

    # Get the project id and dataset id from the config file
    project_id = config.get('DEFAULT', 'target_project')
    dataset_id = config.get('DEFAULT', 'target_dataset')

    # Create a BigQuery client
    client = bigquery.Client()

    # Define the schema for the job table
    job_schema = [
        bigquery.SchemaField("Job", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Job Type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Start Time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("End Time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("Success", "STRING", mode="REQUIRED"),
    ]

    # Define the schema for the log table
    log_schema = [
        bigquery.SchemaField("Job", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Log", "STRING", mode="REQUIRED"),
    ]

    # Define the table IDs
    job_table_id = f'{project_id}.{dataset_id}.job_table'
    log_table_id = f'{project_id}.{dataset_id}.log_table'

    # Check if the tables exist and create them if they do not
    create_table_if_not_exists(client, job_table_id, job_schema)
    create_table_if_not_exists(client, log_table_id, log_schema)

if __name__ == "__main__":
    main('config.ini')  # replace 'config.ini' with the path to your config file
