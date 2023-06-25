import configparser
from add_incremental import add_incremental_delta_files
from create_table import create_table_and_handle_duplicates
from check_output import check_output_in_new_table

def main():
    # Pass the path to the config file
    config_path = 'config.ini'

    # Call the function to add incremental updates to the table
    add_incremental_delta_files(config_path)

    # Call the function to create a new table and handle duplicates
    create_table_and_handle_duplicates()

    # Call the function to check the output in the new table
    check_output_in_new_table(config_path)

# Run the script
if __name__ == "__main__":
    main()