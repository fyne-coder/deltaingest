import configparser
from google.cloud import bigquery
import google.cloud.exceptions

def initialize_table_for_incremental(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)

    source_project_id = config.get('DEFAULT', 'source_project')
    source_dataset = config.get('DEFAULT', 'source_dataset')
    source_table = config.get('DEFAULT', 'source_table')
    target_project_id = config.get('DEFAULT', 'target_project')
    target_dataset = config.get('DEFAULT', 'target_dataset')
    target_table = config.get('DEFAULT', 'target_table')
    tracking_table = config.get('DEFAULT', 'tracking_table')

    client = bigquery.Client(project=source_project_id)
    table_ref = client.dataset(target_dataset, project=target_project_id).table(target_table)
    try:
        client.get_table(table_ref)
        table_exists = True
    except google.cloud.exceptions.NotFound:
        table_exists = False

    if table_exists:
        truncate_query = f"""
            TRUNCATE TABLE `{target_project_id}.{target_dataset}.{target_table}`
        """
        client.query(truncate_query).result()

        insert_query = f"""
            INSERT INTO `{target_project_id}.{target_dataset}.{target_table}`
            SELECT *, NULL AS source_table_name, NULL AS source_creation_time
            FROM `{source_project_id}.{source_dataset}.{source_table}`
        """
        client.query(insert_query).result()
    else:
        create_query = f"""
            CREATE TABLE `{target_project_id}.{target_dataset}.{target_table}` AS
            SELECT *
            FROM `{source_project_id}.{source_dataset}.{source_table}`
        """
        client.query(create_query).result()
        
        # Define the new columns
        new_columns = [
            {"name": "source_table_name", "field_type": "STRING", "mode": "NULLABLE"},
            {"name": "source_creation_time", "field_type": "TIMESTAMP", "mode": "NULLABLE"}
        ]

        # Retrieve the existing table
        table = client.get_table(table_ref)

        # Add new columns to the schema
        table.schema = list(table.schema) + [bigquery.SchemaField(name=column['name'], field_type=column['field_type'], mode=column['mode']) for column in new_columns]

        # Update the table with the new schema
        client.update_table(table, ["schema"])

    # Compare the number of rows in the source and target tables
    source_count_query = f"""
        SELECT COUNT(*)
        FROM `{source_project_id}.{source_dataset}.{source_table}`
    """
    target_count_query = f"""
        SELECT COUNT(*)
        FROM `{target_project_id}.{target_dataset}.{target_table}`
    """
    source_count = client.query(source_count_query).result().total_rows
    target_count = client.query(target_count_query).result().total_rows

    # Output the result
    if source_count == target_count:
        print('Row count matches in source and target tables.')
    else:
        print('Row count mismatch between source and target tables.')

    # Truncate the tracking table
    truncate_tracking_query = f"""
        TRUNCATE TABLE `{target_project_id}.{target_dataset}.{tracking_table}`
    """
    client.query(truncate_tracking_query).result()

