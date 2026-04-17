# Copy this file to config.py and fill in your credentials. Do NOT commit config.py.
# config.py
# Centralized credentials and paths for Milestone 2 ingestion pipeline.
# Fill in every field before running the Spark job. Producers receive
# their credentials via CLI args (same pattern as the professor's
# avro_producer.py), so they do not import this file.

# Azure Event Hubs
event_hub_namespace = 'iesstsabbadbaa-grp-01-05'

# Order Lifecycle Topic
order_topic = 'group_03_orders'
order_producer_conn_str = ''      # SAS connection string, Send claim, ends with ;EntityPath=group_03_orders
order_consumer_conn_str = ''      # SAS connection string, Listen claim, ends with ;EntityPath=group_03_orders

# Courier Location Topic
courier_topic = 'group_03_couriers'
courier_producer_conn_str = ''    # ends with ;EntityPath=group_03_couriers
courier_consumer_conn_str = ''    # ends with ;EntityPath=group_03_couriers

# Azure Blob Storage
account_name = 'iesstsabbadbaa'
account_key = ''
container_name = 'group03output'
