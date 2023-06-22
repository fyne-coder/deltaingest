import configparser
from google.cloud import bigquery

def create_table_and_handle_duplicates():
    config = configparser.ConfigParser()
    config.read('config.ini')

    target_project_id = config.get('DEFAULT', 'target_project_id')
    dataset_id = config.get('DEFAULT', 'target_dataset_id')
    table_id = config.get('DEFAULT', 'target_table_id')
    table_id_latest = config.get('DEFAULT', 'target_table_id_latest')

    client = bigquery.Client(project=target_project_id)
    query1 = f"""
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_creation_time DESC) AS row_num
        FROM `{target_project_id}.{dataset_id}.{table_id}`
    ) t
    WHERE row_num = 1
    """

    query2 = f"""
    CREATE OR REPLACE TABLE `{target_project_id}.{dataset_id}.{table_id_latest}` AS
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_creation_time DESC) AS row_num
        FROM `{target_project_id}.{dataset_id}.{table_id}`
    ) t
    WHERE row_num = 1
    """

    query3 = f"""
    SELECT COUNT(*) AS num_rows
    FROM (
        SELECT source_key
        FROM `{target_project_id}.{dataset_id}.{table_id_latest}`
        GROUP BY source_key
        HAVING COUNT(*) > 1
    )
    """

    query_job1 = client.query(query1)
    results1 = query_job1.result()

    if results1.total_rows > 0:
        table_ref = client.dataset(dataset_id).table(table_id_latest)
        client.delete_table(table_ref, not_found_ok=True)

        query_job2 = client.query(query2)
        query_job2.result()
        print(f"Table `{table_id_latest}` created successfully.")
    else:
        print(f"No rows found in the target table. Skipping creation of `{table_id_latest}` table.")

    query_job3 = client.query(query3)
    results3 = query_job3.result()

    for row in results3:
        count_value = row[0]
        print(f"Duplicate count in new table: {count_value}")