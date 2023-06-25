import configparser
from initialize_table import initialize_table_for_incremental
from google.cloud import bigquery
import job_logger  # make sure job_logger.py is in the same directory or it's in your PYTHONPATH

def main():
    config_path = 'config.ini'  # Define config_path
    job_id = job_logger.create_new_job('initialize')

    if job_id is not None:  # Ensure job_id is not None before proceeding
        initialize_table_for_incremental(config_path, job_id)
        job_logger.update_job_table(job_id, 'Yes')
    else:
        print("Failed to create job_id.")

    job_id = job_logger.create_new_job('initialize')

    # Pass the path to your config file
    config_path = 'config.ini'
    initialize_table_for_incremental(config_path, job_id)

    # Log the end of the job and update the job table
    # Note: Add your job status update code here. The 'Yes' string is a placeholder for the success status of the job.
    # You might need to use a try-except-finally block to make sure the job status is updated even if an error occurs.
    job_logger.log_message(job_id, 'initialize', 'Job ended.')
    job_logger.update_job_table(job_id, 'Yes')

# Run the script
if __name__ == "__main__":
    main()