from google.cloud import bigquery

def check_output_in_new_table():
    client = bigquery.Client()

    query1 = """
    SELECT COUNT(*) as row_count
    FROM `dnbibmaba94079d5d54dcfb48d6eda.saas_cus_dnb_incr_ibm.test1`
    """

    query2 = """
    SELECT COUNT(*) as total_row_count
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY source_key ORDER BY source_creation_time DESC) AS row_num
        FROM `dnbibmaba94079d5d54dcfb48d6eda.saas_cus_dnb_incr_ibm.test1`
    ) t
    WHERE row_num = 1
    """

    query_job1 = client.query(query1)
    results1 = query_job1.result()

    for row in results1:
        original_row_count = row.row_count
        print(f"Row count from `test1` table: {original_row_count}")

    query_job2 = client.query(query2)
    results2 = query_job2.result()

    for row in results2:
        total_row_count = row.total_row_count
        print(f"Total Row Count from Query: {total_row_count}")

    difference = original_row_count - total_row_count
    print(f"Difference: {difference}")
