import configparser
from add_incremental import add_incremental_delta_files
from create_table import create_table_and_handle_duplicates
from check_output import check_output_in_new_table
import job_logger  # make sure job_logger.py is in the same directory or it's in your PYTHONPATH

def main():
    config_path = 'config.ini'  # Define config_path

    try:
        # Create a new job and store the job ID in job_id_memory
        job_id = job_logger.create_new_job('incremental')

        add_incremental_delta_files(config_path, job_id)
        job_logger.update_job_table(job_id, 'incremental', 'Yes')
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        job_logger.update_job_table(job_id, 'incremental', 'No')  # Update job status as failure

    try:
        # Call the function to check the output in the new table
        job_id = job_logger.create_new_job('check_output')
        check_output_in_new_table(config_path, job_id)
        job_logger.update_job_table(job_id, 'check_output', 'Yes')
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        job_logger.update_job_table(job_id, 'check_output', 'No')  # Update job status as failure

if __name__ == "__main__":
    main()