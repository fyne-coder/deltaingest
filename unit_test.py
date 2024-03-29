import configparser
from google.cloud import bigquery

def unit_test_on_query_to_create_updated_table():
    config = configparser.ConfigParser()
    config.read('config.ini')

    target_project_id = config.get('DEFAULT', 'target_project_id')
    dataset_id = config.get('DEFAULT', 'target_dataset_id')
    table_id = config.get('DEFAULT', 'target_table_id')

    client = bigquery.Client(project=target_project_id)
    query1 = f"""
    SELECT COUNT(*) as row_count
    FROM `{target_project_id}.{dataset_id}.{table_id}`
    """

    query2 = f"""
    SELECT COUNT(*) as total_row_count
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_creation_time DESC) AS row_num
        FROM `{target_project_id}.{dataset_id}.{table_id}`
    ) t
    WHERE row_num = 1
    """

    query_job1 = client.query(query1)
    results1 = query_job1.result()

    for row in results1:
        original_row_count = row.row_count
        print(f"Row count from `{table_id}` table: {original_row_count}")

    query_job2 = client.query(query2)
    results2 = query_job2.result()

    for row in results2:
        total_row_count = row.total_row_count
        print(f"Total Row Count from Query: {total_row_count}")

    difference = original_row_count - total_row_count
    print(f"Difference: {difference}")
