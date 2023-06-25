import configparser
from google.cloud import bigquery

def create_table_and_handle_duplicates():
    # Parse the config file
    config = configparser.ConfigParser()
    config.read('config.ini')
     
    # Read the configuration details for the target table
    target_project_id = config.get('DEFAULT', 'target_project')
    dataset_id = config.get('DEFAULT', 'target_dataset')
    table_id = config.get('DEFAULT', 'target_table')
    table_id_latest = config.get('DEFAULT', 'target_table_latest')

    # Create a BigQuery client
    client = bigquery.Client(project=target_project_id)
    
    # Prepare a SQL query to select the most recent row for each source key from the target table
    query1 = f"""
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_creation_time DESC) AS row_num
        FROM `{target_project_id}.{dataset_id}.{table_id}`
    ) t
    WHERE row_num = 1
    """
    # Prepare a SQL query to create a new table with the most recent row for each source key from the target table
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

    # Prepare a SQL query to count the number of source keys that appear more than once in the new table
    query3 = f"""
    SELECT COUNT(*) AS num_rows
    FROM (
        SELECT source_key
        FROM `{target_project_id}.{dataset_id}.{table_id_latest}`
        GROUP BY source_key
        HAVING COUNT(*) > 1
    )
    """
    
    # Execute the first query and get the results
    query_job1 = client.query(query1)
    results1 = query_job1.result()

    if results1.total_rows > 0:
        # If there are any rows, delete the current table (if it exists)
        table_ref = client.dataset(dataset_id).table(table_id_latest)
        client.delete_table(table_ref, not_found_ok=True)

        # Execute the second query to create the new table
        query_job2 = client.query(query2)
        query_job2.result()
        print(f"Table `{table_id_latest}` created successfully.")
    else:
        # If there are no rows, skip the creation of the new table
        print(f"No rows found in the target table. Skipping creation of `{table_id_latest}` table.")

     # Execute the third query to get the number of duplicates in the new table
    query_job3 = client.query(query3)
    results3 = query_job3.result()

    # Print the number of duplicates
    for row in results3:
        count_value = row[0]
        print(f"Duplicate count in new table: {count_value}")