import configparser
import datetime
import pytz
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


config_path = "./config.ini"
config = configparser.ConfigParser()
config.read(config_path)

source_project_id = config.get('DEFAULT', 'source_project')
target_project_id = config.get('DEFAULT', 'target_project')
target_dataset_id = config.get('DEFAULT', 'target_dataset')
target_table_id = config.get('DEFAULT', 'target_table')
tracking_table_id = config.get('DEFAULT', 'tracking_table')
log_table_id = config.get('DEFAULT', 'log_table')
source_dataset_id = config.get('DEFAULT', 'source_dataset')
date_after = config.get('DEFAULT', 'date_after')
table_prefix = config.get('DEFAULT', 'table_prefix')

timezone = pytz.timezone('UTC')

client = bigquery.Client(project=source_project_id)
target_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(target_table_id)
target_table = client.get_table(target_table_ref)
tracking_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(tracking_table_id)
tracking_table = client.get_table(tracking_table_ref)
log_table_ref = client.dataset(target_dataset_id, project=target_project_id).table(log_table_id)
log_table = client.get_table(log_table_ref)
date_after = date_after.strip('"')
date_after = datetime.datetime.strptime(date_after, '%Y-%m-%d %H:%M:%S')
date_after = date_after.replace(tzinfo=pytz.UTC)  # Use correct timezone
source_dataset_ref = client.dataset(source_dataset_id)
source_tables = list(client.list_tables(source_dataset_ref))

print(f"Table prefix: {table_prefix}")
