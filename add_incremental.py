import configparser
import datetime
import pytz
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


def add_incremental_delta_files(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)

    source_project_id = config.get('DEFAULT', 'source_project')
    target_project_id = config.get('DEFAULT', 'target_project')
    target_dataset_id = config.get('DEFAULT', 'target_dataset')
    target_table_id = config.get('DEFAULT', 'target_table')
    tracking_table_id = config.get('DEFAULT', 'tracking_table')
    log_table_id = config.get('DEFAULT', 'log_table')
    source_dataset_id = config.get('DEFAULT', 'source_dataset')
    date_after = config.get('DEFAULT', 'date_after')
    table_prefix = config.get('DEFAULT', 'table_prefix')

    timezone = pytz.timezone('UTC')

    client = bigquery.Client(project=source_project_id)
    target_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(target_table_id)
    target_table = client.get_table(target_table_ref)
    tracking_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(tracking_table_id)
    tracking_table = client.get_table(tracking_table_ref)
    log_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(log_table_id)
    log_table = client.get_table(log_table_ref)
    date_after = date_after.strip('"')
    date_after = datetime.datetime.strptime(date_after, '%Y-%m-%d %H:%M:%S')
    date_after = date_after.replace(tzinfo=pytz.UTC)  # Use correct timezone
    source_dataset_ref = client.dataset(source_dataset_id)
    source_tables = list(client.list_tables(source_dataset_ref))

    print(f"Table prefix: {table_prefix}")

    # Check if tracking table is empty
    query = f"SELECT COUNT(*) FROM `{target_project_id}.{target_dataset_id}.{tracking_table_id}`"
    result = client.query(query).result()
    row_count = next(result)[0]

    if row_count == 0:
        print("Tracking table is empty. Inserting all tables.")
        for table in source_tables:
            print(f"Table created date: {table.created}")
            print(f"Date after: {date_after}")

            # Check if table matches the prefix
            if not table.table_id.startswith(table_prefix):
                print(f"Table {table.table_id} does not match the prefix. Skipping.")
                continue

            # Check if table was created after date_after
            if table.created < date_after:
                print(f"Table {table.table_id} was created before {date_after}. Skipping.")
                continue

            # Check if table exists in the tracking table
            tracking_table_exists = False
            query = f"SELECT COUNT(*) FROM `{target_project_id}.{target_dataset_id}.{tracking_table_id}` WHERE table_id = '{table.table_id}'"
            result = client.query(query).result()
            row_count = next(result)[0]
            tracking_table_exists = row_count > 0

            if tracking_table_exists:
                print(f"Table {table.table_id} already exists in the tracking table. Skipping inserts.")
                continue

            full_table = client.get_table(table)
            rows_to_insert = [(full_table.table_id, full_table.num_rows, full_table.created)]
            errors = client.insert_rows(tracking_table, rows_to_insert)
            if errors:
                print(f"Errors occurred during tracking table insertion: {errors}")

            source_table_ref = source_dataset_ref.table(table.table_id)
            source_table = client.get_table(source_table_ref)
            rows = client.list_rows(source_table)
            # Continue with your existing processing for the rows
            ...

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
                print(f"No more tables found after {next_table_id}. Stopping.")
                break

            full_table = client.get_table(next_table)
            rows_to_insert = [(full_table.table_id, full_table.num_rows, full_table.created)]
            errors = client.insert_rows(tracking_table, rows_to_insert)
            if errors:
                print(f"Errors occurred during tracking table insertion: {errors}")

            source_table_ref = source_dataset_ref.table(next_table.table_id)
            source_table = client.get_table(source_table_ref)
            rows = client.list_rows(source_table)
            # Continue with your existing processing for the rows
            ...

            # Remember to increment the suffix for the next iteration
            next_suffix += 1