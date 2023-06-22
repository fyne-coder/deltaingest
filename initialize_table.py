import configparser
from google.cloud import bigquery
import google.cloud.exceptions

def initialize_table_for_incremental():
    config = configparser.ConfigParser()
    config.read('config.ini')

    source_project_id = config.get('DEFAULT', 'source_project_id')
    source_dataset = config.get('DEFAULT', 'source_dataset')
    source_table = config.get('DEFAULT', 'source_table')
    target_project_id = config.get('DEFAULT', 'target_project_id')
    target_dataset = config.get('DEFAULT', 'target_dataset')
    target_table = config.get('DEFAULT', 'target_table')

    client = bigquery.Client(project=source_project_id)
    table_ref = client.dataset(target_dataset, project=target_project_id).table(target_table)
    try:
        client.get_table(table_ref)
        table_exists = True
    except google.cloud.exceptions.NotFound:
        table_exists = False

    if table_exists:
        delete_query = f"""
            DELETE
            FROM `{target_project_id}.{target_dataset}.{target_table}`
            WHERE TRUE
        """
        client.query(delete_query).result()

        insert_query = f"""
            INSERT INTO `{target_project_id}.{target_dataset}.{target_table}`
            SELECT *
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