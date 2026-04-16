# spark_streaming_to_blob.py
# Spark Structured Streaming job: reads Avro-encoded Kafka messages from
# Azure Event Hubs and lands Parquet files in Azure Blob Storage.
#
# Structured as sequential blocks (Colab-cell style). Each BLOCK N can be
# pasted into its own Colab cell. Paste them in order — BLOCK 2 builds the
# SparkSession, BLOCK 10 launches the long-running writers, BLOCK 11 is the
# live-display polling loop, BLOCK 13 is the cleanup.
#
# All patterns come from the professor's Notebook 2. Cell numbers from that
# notebook are referenced inline so you can cross-check.

# =============================================================================
# BLOCK 1 — Configuration variables
# =============================================================================
# Pull everything from config.py. Fill that file in before running this script.
# In Colab you can either:
#    %run config.py
# or paste the config values directly into this cell.

from config import (
    event_hub_namespace,
    order_topic, order_consumer_conn_str,
    courier_topic, courier_consumer_conn_str,
    account_name, account_key, container_name,
)


# =============================================================================
# BLOCK 2 — Spark setup (Notebook 2, Cells 7-8 pattern)
# =============================================================================
# %%writefile is optional — useful if you want Colab to persist this cell to disk.

from pyspark.sql import SparkSession

jar_dependencies = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
    "org.apache.spark:spark-avro_2.13:4.1.1",
    "org.apache.hadoop:hadoop-azure:3.3.1",
    "com.microsoft.azure:azure-storage:8.6.6"
])

spark = SparkSession.builder \
    .appName("StreamingAVROFromKafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", jar_dependencies) \
    .config(f"fs.azure.account.key.{account_name}.blob.core.windows.net", account_key) \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# =============================================================================
# BLOCK 3 — Define Avro schemas as strings (Notebook 2, Cell 6 pattern)
# =============================================================================
# from_avro() takes a JSON schema string. These must match the producer
# schemas byte-for-byte (same field names, order, and namespaces).

order_schema_str = """
{
  "type": "record",
  "name": "OrderLifecycleEvent",
  "namespace": "fooddelivery.orders",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "restaurant_id", "type": "string"},
    {"name": "courier_id", "type": ["null", "string"], "default": null},
    {"name": "zone_id", "type": "string"},
    {"name": "event_type", "type": {
        "type": "enum", "name": "OrderEventType",
        "symbols": ["ORDER_PLACED","ORDER_CONFIRMED","ORDER_PREPARING",
                    "COURIER_ASSIGNED","COURIER_PICKED_UP",
                    "ORDER_DELIVERED","ORDER_CANCELLED"]
    }},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ingestion_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "order_value_cents", "type": "int"},
    {"name": "delivery_fee_cents", "type": "int"},
    {"name": "item_count", "type": "int"},
    {"name": "payment_method", "type": "string"},
    {"name": "platform", "type": "string"},
    {"name": "is_duplicate", "type": "boolean"},
    {"name": "estimated_delivery_time", "type": ["null", "long"], "default": null},
    {"name": "actual_delivery_time", "type": ["null", "long"], "default": null},
    {"name": "prep_duration_seconds", "type": ["null", "int"], "default": null},
    {"name": "delivery_duration_seconds", "type": ["null", "int"], "default": null},
    {"name": "customer_rating", "type": ["null", "int"], "default": null},
    {"name": "cancellation_reason", "type": ["null", "string"], "default": null}
  ]
}
"""

courier_schema_str = """
{
  "type": "record",
  "name": "CourierLocation",
  "namespace": "fooddelivery.couriers",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "courier_id", "type": "string"},
    {"name": "order_id", "type": ["null", "string"], "default": null},
    {"name": "zone_id", "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ingestion_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "latitude", "type": "double"},
    {"name": "longitude", "type": "double"},
    {"name": "speed_kmh", "type": "double"},
    {"name": "heading_degrees", "type": "double"},
    {"name": "courier_status", "type": "string"},
    {"name": "vehicle_type", "type": "string"},
    {"name": "battery_pct", "type": "int"},
    {"name": "network_type", "type": "string"},
    {"name": "is_duplicate", "type": "boolean"}
  ]
}
"""


# =============================================================================
# BLOCK 4 — Kafka config for order topic (Notebook 2, Cell 9 pattern)
# =============================================================================
order_kafka_conf = {
    "kafka.bootstrap.servers": f"{event_hub_namespace}.servicebus.windows.net:9093",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{order_consumer_conn_str}";',
    "subscribe": order_topic,
    "startingOffsets": "latest",
    "enable.auto.commit": "true ",
    "groupIdPrefix": "Stream_Analytics_",
    "auto.commit.interval.ms": "5000"
}


# =============================================================================
# BLOCK 5 — Read + deserialize + flatten for orders (Cells 10-13)
# =============================================================================
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

order_df = spark.readStream.format("kafka").options(**order_kafka_conf).load()
order_df = order_df.select(from_avro(order_df.value, order_schema_str).alias("data"))
order_df = order_df.select(
    col("data.event_id"),
    col("data.order_id"),
    col("data.restaurant_id"),
    col("data.courier_id"),
    col("data.zone_id"),
    col("data.event_type"),
    col("data.event_time"),
    col("data.ingestion_time"),
    col("data.order_value_cents"),
    col("data.delivery_fee_cents"),
    col("data.item_count"),
    col("data.payment_method"),
    col("data.platform"),
    col("data.is_duplicate"),
    col("data.estimated_delivery_time"),
    col("data.actual_delivery_time"),
    col("data.prep_duration_seconds"),
    col("data.delivery_duration_seconds"),
    col("data.customer_rating"),
    col("data.cancellation_reason"),
)


# =============================================================================
# BLOCK 6 — Kafka config for courier topic (Cell 9 pattern, different topic)
# =============================================================================
courier_kafka_conf = {
    "kafka.bootstrap.servers": f"{event_hub_namespace}.servicebus.windows.net:9093",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{courier_consumer_conn_str}";',
    "subscribe": courier_topic,
    "startingOffsets": "latest",
    "enable.auto.commit": "true ",
    "groupIdPrefix": "Stream_Analytics_",
    "auto.commit.interval.ms": "5000"
}


# =============================================================================
# BLOCK 7 — Read + deserialize + flatten for couriers (Cells 10-13)
# =============================================================================
courier_df = spark.readStream.format("kafka").options(**courier_kafka_conf).load()
courier_df = courier_df.select(from_avro(courier_df.value, courier_schema_str).alias("data"))
courier_df = courier_df.select(
    col("data.event_id"),
    col("data.courier_id"),
    col("data.order_id"),
    col("data.zone_id"),
    col("data.event_time"),
    col("data.ingestion_time"),
    col("data.latitude"),
    col("data.longitude"),
    col("data.speed_kmh"),
    col("data.heading_degrees"),
    col("data.courier_status"),
    col("data.vehicle_type"),
    col("data.battery_pct"),
    col("data.network_type"),
    col("data.is_duplicate"),
)


# =============================================================================
# BLOCK 8 — Define Blob output + checkpoint paths (Cell 14 pattern)
# =============================================================================
order_output_path     = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/stream-output/orders/"
order_checkpoint_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/checkpoint/orders/"
courier_output_path     = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/stream-output/couriers/"
courier_checkpoint_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/checkpoint/couriers/"


# =============================================================================
# BLOCK 9 — Memory sinks for live display (both feeds)
# =============================================================================
# In-memory temp views so we can SELECT * FROM ... in the display loop below.
query_orders_mem = order_df.writeStream \
    .format("memory") \
    .queryName("order_events_live") \
    .outputMode("append") \
    .start()

query_couriers_mem = courier_df.writeStream \
    .format("memory") \
    .queryName("courier_events_live") \
    .outputMode("append") \
    .start()


# =============================================================================
# BLOCK 10 — Parquet sinks to Blob (Cell 15 pattern, trigger 5 seconds)
# =============================================================================
query_orders_parquet = order_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", order_checkpoint_path) \
    .option("path", order_output_path) \
    .queryName("orders_to_blob") \
    .trigger(processingTime="5 seconds") \
    .start()

query_couriers_parquet = courier_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", courier_checkpoint_path) \
    .option("path", courier_output_path) \
    .queryName("couriers_to_blob") \
    .trigger(processingTime="5 seconds") \
    .start()


# =============================================================================
# BLOCK 11 — Display loop (Cell 28 polling pattern)
# =============================================================================
from time import sleep
from IPython.display import clear_output

sleep(15)  # let the first micro-batches land

N = 10
for i in range(N):
    clear_output(wait=True)
    print(f"--- Batch {i + 1}/{N} ---")
    print("ORDERS (most recent 20):")
    spark.sql(
        "SELECT event_id, order_id, zone_id, event_type, event_time "
        "FROM order_events_live ORDER BY event_time DESC"
    ).show(20, truncate=False)
    print("COURIERS (most recent 20):")
    spark.sql(
        "SELECT event_id, courier_id, zone_id, courier_status, speed_kmh, event_time "
        "FROM courier_events_live ORDER BY event_time DESC"
    ).show(20, truncate=False)
    sleep(10)


# =============================================================================
# BLOCK 12 — Verification: read Parquet back from Blob (Cell 22 pattern)
# =============================================================================
df_orders_result = spark.read.parquet(order_output_path).filter("event_type = 'ORDER_DELIVERED'")
df_orders_result.show(5)

df_couriers_result = spark.read.parquet(courier_output_path).filter("courier_status = 'ONLINE_DELIVERING'")
df_couriers_result.show(5)


# =============================================================================
# BLOCK 13 — Active queries + cleanup (Cell 23 pattern)
# =============================================================================
for q in spark.streams.active:
    print("active:", q.name, "id=" + q.id)

# To stop individual queries (run these when ready to shut down):
# query_orders_parquet.stop()
# query_couriers_parquet.stop()
# query_orders_mem.stop()
# query_couriers_mem.stop()

# Or stop everything at once:
# for q in spark.streams.active:
#     q.stop()
# spark.stop()
