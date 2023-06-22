import configparser
from initialize_table import initialize_table_for_incremental

# Load parameters from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

def main():
    initialize_table_for_incremental(config['DEFAULT']['source_dataset'], config['DEFAULT']['source_table'], config['DEFAULT']['target_dataset'], config['DEFAULT']['target_table'])

# Run the script
main()