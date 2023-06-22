from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import datetime

def add_incremental_delta_files(target_dataset_id, target_table_id, tracking_table_id, log_table_id, source_dataset_id, date_after):
    client = bigquery.Client()
    target_table_ref = client.dataset(target_dataset_id).table(target_table_id)
    target_table = client.get_table(target_table_ref)
    tracking_table_ref = client.dataset(target_dataset_id).table(tracking_table_id)
    tracking_table = client.get_table(tracking_table_ref)
    log_table_ref = client.dataset(target_dataset_id).table(log_table_id)
    log_table = client.get_table(log_table_ref)
    date_after = datetime.datetime.strptime(date_after, '%Y-%m-%d %H:%M:%S')
    source_dataset_ref = client.dataset(source_dataset_id)
    source_tables = list(client.list_tables(source_dataset_ref))

    filtered_tables = [
        table
        for table in source_tables
        if (
            table.table_id.startswith("source-09-20230308143021-0000000189")
            and table.created > date_after
        )
    ]

    for table in filtered_tables:
        full_table = client.get_table(table)
        rows_to_insert = [(full_table.table_id, full_table.num_rows, full_table.created)]
        errors = client.insert_rows(tracking_table, rows_to_insert)

        source_table_ref = source_dataset_ref.table(table.table_id)
        source_table = client.get_table(source_table_ref)
        rows = client.list_rows(source_table)

        rows_list = [dict(row.items()) for row in rows]
        rows_list = [{**row, "source_table_name": table.table_id, "source_creation_time": table.created.isoformat()} for row in rows_list]
        
        if rows_list:
            errors = client.insert_rows_json(target_table_ref, rows_list)
