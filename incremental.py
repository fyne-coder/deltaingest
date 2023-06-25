import configparser
from add_incremental import add_incremental_delta_files
from create_table import create_table_and_handle_duplicates
from check_output import check_output_in_new_table

def main():
    # Pass the path to your config file
    config_path = 'config.ini'
    add_incremental_delta_files(config_path)

    # Call the create_table_and_handle_duplicates function after add_incremental_delta_files
    create_table_and_handle_duplicates()

    # Call the check_output_in_new_table function after create_table_and_handle_duplicates
    check_output_in_new_table(config_path)

# Run the script
if __name__ == "__main__":
    main()