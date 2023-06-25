import configparser
import google.cloud.exceptions
from google.cloud import bigquery

def create_table_if_not_exists(client, table_id, schema):
    """
    Function to create a BigQuery table if it does not exist.
    
    Args:
        client (google.cloud.bigquery.client.Client): BigQuery Client.
        table_id (str): Full ID of the table.
        schema (List[google.cloud.bigquery.schema.SchemaField]): Schema of the table.

    Returns:
        None
    """
    try:
        # Check if the table exists
        client.get_table(table_id)
    except google.cloud.exceptions.NotFound:
        # Create the table if it does not exist
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Created table {table_id}")

def main(config_path):
    """
    Main function to create job and log tables in BigQuery.
    
    Args:
        config_path (str): Path to the configuration file.
    
    Returns:
        None
    """
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