from google.cloud import bigquery

def create_table_and_handle_duplicates():
    client = bigquery.Client()

    query1 = """
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_creation_time DESC) AS row_num
        FROM `dnbibmaba94079d5d54dcfb48d6eda.saas_cus_dnb_incr_ibm.test1`
    ) t
    WHERE row_num = 1
    """

    query2 = """
    CREATE OR REPLACE TABLE `dnbibmaba94079d5d54dcfb48d6eda.saas_cus_dnb_incr_ibm.test1_latest` AS
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_creation_time DESC) AS row_num
        FROM `dnbibmaba94079d5d54dcfb48d6eda.saas_cus_dnb_incr_ibm.test1`
    ) t
    WHERE row_num = 1
    """

    query3 = """
    SELECT COUNT(*) AS num_rows
    FROM (
        SELECT source_key
        FROM `dnbibmaba94079d5d54dcfb48d6eda.saas_cus_dnb_incr_ibm.test1_latest`
        GROUP BY source_key
        HAVING COUNT(*) > 1
    )
    """

    query_job1 = client.query(query1)
    results1 = query_job1.result()

    if results1.total_rows > 0:
        table_ref = client.dataset("saas_cus_dnb_incr_ibm").table("test1_latest")
        client.delete_table(table_ref, not_found_ok=True)

        query_job2 = client.query(query2)
        query_job2.result()
        print("Table `test1_latest` created successfully.")
    else:
        print("No rows found in the target table. Skipping creation of `test1_latest` table.")

    query_job3 = client.query(query3)
    results3 = query_job3.result()

    for row in results3:
        count_value = row[0]
        print(f"Duplicate count in new table: {count_value}")
