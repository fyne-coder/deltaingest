# Google BigQuery Incremental Updates

This project is designed to manage incremental updates on Google BigQuery tables. It provides scripts for both the initial setup of a target table based on a source table, and for periodically updating the target table with incremental data from multiple source tables.

## Description

The project consists of two main Python scripts:

1. `initiation.py`: This script initializes a target table based on a source table in Google BigQuery. If the target table already exists, it deletes all rows from the target table and inserts records from the source table. If the target table doesn't exist, it creates a new table based on the source table schema. This script is intended to be run only once for setup.

2. `incremental.py`: This script performs incremental updates on the target table. It filters source tables that match a specific naming pattern and were created after a certain date, then adds the incremental data from these source tables into the target table. It also handles duplicate records by keeping only the latest record for each unique key. This script is intended to be run periodically (e.g., daily) to perform incremental updates.

## Setup

1. Clone the repository.

2. Install the necessary Python libraries with the following command: 
pip install google-cloud-bigquery configparser


3. Replace the placeholders in the `config.ini` file with your actual parameters.

## Usage

1. Run the `initiation.py` script for initial setup: python initiation.py


2. Schedule the `incremental.py` script to run periodically (e.g., daily) for incremental updates: python incremental.py


## Configuration

The configuration parameters for the scripts are stored in the `config.ini` file. Here's an example of what the configuration file might look like:

[DEFAULT] \
source_project_id = your_project_id \
source_dataset = your_source_dataset \
source_table = your_source_table \
target_project_id = your_project_id \
target_dataset = your_target_dataset \
target_table = your_target_table \
target_dataset_id = your_target_dataset_id \
target_table_id = your_target_table_id \
target_table_id_latest = your_target_table_id_latest \
tracking_table_id = your_tracking_table_id \
log_table_id = your_log_table_id \
source_dataset_id = your_source_dataset_id \
date_after = "2023-06-01 00:00:00" \
table_prefix = "source-09-20230308143021-0000000189" 

## Security

Make sure to secure the config.ini file appropriately, as it contains sensitive information.
