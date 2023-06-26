import configparser
import datetime
import pytz
import job_logger
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

def add_incremental_delta_files(config_path, job_id):
    # Parse the config file
    config = configparser.ConfigParser()
    config.read(config_path)

    # Read the configuration details for the source and target tables
    source_project_id = config.get('DEFAULT', 'source_project')
    target_project_id = config.get('DEFAULT', 'target_project')
    target_dataset_id = config.get('DEFAULT', 'target_dataset')
    target_table_id = config.get('DEFAULT', 'target_table')
    tracking_table_id = config.get('DEFAULT', 'tracking_table')
    log_table_id = config.get('DEFAULT', 'log_table')
    source_dataset_id = config.get('DEFAULT', 'source_dataset')
    date_after = config.get('DEFAULT', 'date_after')
    table_prefix = config.get('DEFAULT', 'table_prefix')

    # Set timezone
    timezone = pytz.timezone('UTC')

    # Create a BigQuery client
    client = bigquery.Client(project=source_project_id)

    # Get the references to the target and tracking tables
    target_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(target_table_id)
    target_table = client.get_table(target_table_ref)
    tracking_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(tracking_table_id)
    tracking_table = client.get_table(tracking_table_ref)
    log_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(log_table_id)
    log_table = client.get_table(log_table_ref)

    # Parse the date after which to consider updates
    date_after = date_after.strip('"')
    date_after = datetime.datetime.strptime(date_after, '%Y-%m-%d %H:%M:%S')
    date_after = date_after.replace(tzinfo=pytz.UTC)  # Use correct timezone
   
    # Get the list of tables in the source dataset
    source_dataset_ref = client.dataset(source_dataset_id)

    # Use SQL to query BigQuery's metadata
    query = (
        f"SELECT table_name "
        f"FROM `{source_project_id}.{source_dataset_id}.INFORMATION_SCHEMA.TABLES` "
        f"WHERE table_name LIKE '{table_prefix}%'"
        f"AND creation_time > TIMESTAMP '{date_after.isoformat()}'"
    )
    query_job = client.query(query)

    # Get a list of table names that start with the prefix and were created after date_after
    source_table_names = [row.table_name for row in query_job]

    # Get table references from the table names
    source_tables = [client.get_table(source_dataset_ref.table(table_name)) for table_name in source_table_names]

    job_logger.log_message(job_id, 'add_incremental', f"Table prefix: {table_prefix}")

    # Check if tracking table is empty
    query = f"SELECT COUNT(*) FROM `{target_project_id}.{target_dataset_id}.{tracking_table_id}`"
    result = client.query(query).result()
    row_count = next(result)[0]

    if row_count == 0:
        job_logger.log_message(job_id, 'add_incremental', "Tracking table is empty. Inserting all tables.")
        for table in source_tables:
            # Check if table exists in the tracking table
            tracking_table_exists = False
            query = f"SELECT COUNT(*) FROM `{target_project_id}.{target_dataset_id}.{tracking_table_id}` WHERE table_id = '{table.table_id}'"
            result = client.query(query).result()
            row_count = next(result)[0]
            tracking_table_exists = row_count > 0

            if tracking_table_exists:
                job_logger.log_message(job_id, 'add_incremental', f"Table {table.table_id} already exists in the tracking table. Skipping inserts.")
                continue

            full_table = client.get_table(table)
            rows_to_insert = [(full_table.table_id, full_table.num_rows, full_table.created)]
            errors = client.insert_rows(tracking_table, rows_to_insert)
            if errors:
                job_logger.log_message(job_id, 'add_incremental', f"Errors occurred during tracking table insertion: {errors}")

            source_table_ref = source_dataset_ref.table(table.table_id)
            source_table = client.get_table(source_table_ref)
            rows = client.list_rows(source_table)

            # Convert each row to a dictionary and add additional fields
            rows_list = [dict(row.items()) for row in rows]
            rows_list = [{**row, "source_table_name": table.table_id, "source_creation_time": table.created.isoformat()} for row in rows_list]

            # If there are rows, insert them into the target table
            if rows_list:
                errors = client.insert_rows_json(target_table_ref, rows_list)
                if errors:
                    job_logger.log_message(job_id, 'add_incremental', f"Errors occurred during target table insertion: {errors}")
    else:
        # Code for when the tracking table is not empty
        query = f"SELECT table_id FROM `{target_project_id}.{target_dataset_id}.{tracking_table_id}` ORDER BY table_id DESC LIMIT 1"
        result = client.query(query).result()
        last_table_id = next(result)[0]

        # Extract the current suffix (e.g., "1820" from "source-09-20230308143021-0000000189-1820")
        last_suffix = int(last_table_id.split('-')[-1])

        # Start from the next suffix
        next_suffix = last_suffix + 1

        while True:
            next_table_id = table_prefix + '-' + str(next_suffix).zfill(4)
            next_table_ref = source_dataset_ref.table(next_table_id)

            try:
                next_table = client.get_table(next_table_ref)
            except Exception as e:
                job_logger.log_message(job_id, 'add_incremental', f"No more tables found after {next_table_id}. Stopping.")
                break

            full_table = client.get_table(next_table)
            rows_to_insert = [(full_table.table_id, full_table.num_rows, full_table.created)]
            errors = client.insert_rows(tracking_table, rows_to_insert)
            if errors:
                job_logger.log_message(job_id, 'add_incremental', f"Errors occurred during tracking table insertion: {errors}")

            source_table_ref = source_dataset_ref.table(next_table.table_id)
            source_table = client.get_table(source_table_ref)
            rows = client.list_rows(source_table)

            # Convert each row to a dictionary and add additional fields
            rows_list = [dict(row.items()) for row in rows]
            rows_list = [{**row, "source_table_name": next_table.table_id, "source_creation_time": next_table.created.isoformat()} for row in rows_list]
        
            # If there are rows, insert them into the target table
            if rows_list:
                errors = client.insert_rows_json(target_table_ref, rows_list)
                if errors:
                    job_logger.log_message(job_id, 'add_incremental', f"Errors occurred during target table insertion:The remainder of the script is as follows: {errors}")

            # Remember to increment the suffix for the next iteration
            next_suffix += 1