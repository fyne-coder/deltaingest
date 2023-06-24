import configparser
from initialize_table import initialize_table_for_incremental

def main():
    # Pass the path to your config file
    config_path = 'config.ini'
    initialize_table_for_incremental(config_path)

# Run the script
if __name__ == "__main__":
    main()