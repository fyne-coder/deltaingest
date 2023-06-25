import configparser
from initialize_table import initialize_table_for_incremental
from google.cloud import bigquery
import job_logger  # make sure job_logger.py is in the same directory or it's in your PYTHONPATH

def main():
    config_path = 'config.ini'  # Define config_path

    # Create a new job and store the job ID in job_id_memory
    #job_id = job_logger.create_new_job('initialize')

    #if job_id is not None:  # Ensure job_id is not None before proceeding
    #    initialize_table_for_incremental(config_path, job_id)
    #    # Log the end of the job and update the job table
    #    job_logger.log_message(job_id, 'initialize', 'Job ended.')
    #    job_logger.update_job_table(job_id, 'initialize', 'Yes')
    #else:
    #    print("Failed to create job_id.")

    # Create another new job and store the job ID in job_id_memory
    job_id = job_logger.create_new_job('initialize')

    # Pass the path to your config file
    config_path = 'config.ini'
    initialize_table_for_incremental(config_path, job_id)

    # Log the end of the job and update the job table
    job_logger.log_message(job_id, 'initialize', 'Job ended.')
    job_logger.update_job_table(job_id, 'initialize', 'Yes')

# Run the script
if __name__ == "__main__":
    main()
