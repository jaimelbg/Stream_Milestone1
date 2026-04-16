# Copy this file to config.py and fill in your credentials. Do NOT commit config.py.
# config.py
# Centralized credentials and paths for Milestone 2 ingestion pipeline.
# Fill in every field before running the Spark job. Producers receive
# their credentials via CLI args (same pattern as the professor's
# avro_producer.py), so they do not import this file.

# Azure Event Hubs
event_hub_namespace = ''  # e.g., 'iesstsabbadbaa-grp-01-05'

# Order Lifecycle Topic
order_topic = ''                  # e.g., 'group_03_order_lifecycle'
order_producer_conn_str = ''      # SAS connection string, Send claim
order_consumer_conn_str = ''      # SAS connection string, Listen claim

# Courier Location Topic
courier_topic = ''                # e.g., 'group_03_courier_location'
courier_producer_conn_str = ''
courier_consumer_conn_str = ''

# Azure Blob Storage
account_name = ''                 # e.g., 'iesstsabbadbaa'
account_key = ''
container_name = ''               # e.g., 'group03output'
