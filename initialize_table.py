from google.cloud import bigquery
import google.cloud.exceptions

def initialize_table_for_incremental(source_dataset, source_table, target_dataset, target_table):
    client = bigquery.Client()
    table_ref = client.dataset(target_dataset).table(target_table)
    try:
        client.get_table(table_ref)
        table_exists = True
    except google.cloud.exceptions.NotFound:
        table_exists = False
    if table_exists:
        delete_query = f"""
            DELETE
            FROM `{target_dataset}.{target_table}`
            WHERE TRUE
        """
        client.query(delete_query).result()
        insert_query = f"""
            INSERT INTO `{target_dataset}.{target_table}`
            SELECT *
            FROM `{source_dataset}.{source_table}`
        """
        client.query(insert_query).result()
    else:
        create_query = f"""
            CREATE TABLE `{target_dataset}.{target_table}` AS
            SELECT *
            FROM `{source_dataset}.{source_table}`
        """
        client.query(create_query).result()
