# Milestone 2 — Data Ingestion Pipeline (Group 03, BBADBA A)

Real-time streaming layer for the Food Delivery platform. Order lifecycle
events and courier GPS pings flow from Python producers → Azure Event Hubs
(Kafka protocol) → Spark Structured Streaming → partitioned Parquet files in
Azure Blob Storage.

## 🚴 Live Analytics Dashboard
**[→ Open Dashboard](https://streamdashboard-group3.streamlit.app/)**
Real-time visualization · Azure Event Hubs + Spark Structured Streaming · Section 2.4

---


Event Hub namespace: **`iesstsabbadbaa-grp-01-05`**.

## Architecture

```
+----------------------+                     +----------------------+
|  order_producer.py   |   confluent-kafka   |  Event Hubs topic    |
|  (Avro, fastavro)    | ------------------> |  group_03_orders     |
|                      |   SASL_SSL / PLAIN  |                      |
+----------------------+                     +----------+-----------+
                                                        |
+----------------------+                                |
|  courier_producer.py |                                |
|  (Avro, fastavro)    | -------------------------+     |
+----------------------+                          |     |
                                                  v     v
                                         +------------------------+
                                         |  Event Hubs topic      |
                                         |  group_03_couriers     |
                                         |                        |
                                         +-----------+------------+
                                                     |
                             spark.readStream.format("kafka")
                                                     |
                                                     v
                                         +------------------------+
                                         | spark_streaming_to_    |
                                         |     blob.py            |
                                         |  from_avro + flatten   |
                                         |  local[*]              |
                                         +-----------+------------+
                                                     |
                                  writeStream.format("parquet")
                                                     |
                                                     v
                                         +------------------------+
                                         |  Azure Blob Storage    |
                                         |  wasbs://group03output |
                                         |    /stream-output/     |
                                         |       orders/          |
                                         |       couriers/        |
                                         +------------------------+
```

## Repository layout

| File                          | Purpose                                                              |
|-------------------------------|----------------------------------------------------------------------|
| `order_producer.py`           | Kafka producer for order lifecycle events (Avro).                    |
| `courier_producer.py`         | Kafka producer for courier GPS pings (Avro).                         |
| `spark_streaming_to_blob.py`  | Spark job (13 Colab-friendly blocks). Reads Avro from Kafka, writes Parquet to Blob. |
| `config.py`                   | Central credentials + topic/path configuration. All fields start empty. |
| `README.md`                   | This file.                                                           |

## 1. Prerequisites

- **Python 3.10+**
- Local packages for the producers:
  ```bash
  pip install confluent-kafka fastavro
  ```
- **PySpark** via Google Colab (recommended) — the Spark job pulls its jars
  via `spark.jars.packages`, so Colab's bundled Spark 4.1.1 / Java 21
  environment is sufficient with no extra install steps.
- An Azure subscription with:
  - Existing Event Hubs namespace `iesstsabbadbaa-grp-01-05` (Standard tier).
  - A storage account (e.g. `iesstsabbadbaa`) with access-key auth enabled.

## 2. Azure setup

### 2.1 Event Hubs (two topics)

In the portal, navigate to the namespace `iesstsabbadbaa-grp-01-05` →
*Event Hubs* → *+ Event Hub*, and create:

| Event Hub name               | Partition count | Message retention |
|------------------------------|-----------------|-------------------|
| `group_03_orders`   | **4**           | **3 days**        |
| `group_03_couriers`  | **4**           | **3 days**        |

Naming convention: `group_03_*`, matching the module's group allocation.

### 2.2 SAS policies (per topic)

For each Event Hub create two Shared access policies:

- `send-policy` — claim `Send` only. Copy its connection string into
  `order_producer_conn_str` / `courier_producer_conn_str`.
- `listen-policy` — claim `Listen` only. Copy its connection string into
  `order_consumer_conn_str` / `courier_consumer_conn_str`.

Using per-topic SAS (not the namespace-level RootManageSharedAccessKey) gives
us least privilege and isolates revocation per feed.

### 2.3 Blob container

In the storage account `iesstsabbadbaa`:
- Create a container named `group03output` (access level: *private*).
- Note the account name and the account key (Security + networking → Access
  keys → key1).

## 3. Fill in `config.py`

Open `config.py` and populate every empty string. Example:

```python
event_hub_namespace = 'iesstsabbadbaa-grp-01-05'

order_topic = 'group_03_orders'
order_producer_conn_str = 'Endpoint=sb://iesstsabbadbaa-grp-01-05.servicebus.windows.net/;SharedAccessKeyName=send-policy;SharedAccessKey=...;EntityPath=group_03_orders'
order_consumer_conn_str = 'Endpoint=sb://iesstsabbadbaa-grp-01-05.servicebus.windows.net/;SharedAccessKeyName=listen-policy;SharedAccessKey=...;EntityPath=group_03_orders'

courier_topic = 'group_03_couriers'
courier_producer_conn_str = '...'
courier_consumer_conn_str = '...'

account_name = 'iesstsabbadbaa'
account_key = '...'
container_name = 'group03output'
```

Do **not** commit a filled-in `config.py` to Git. Add it to `.gitignore` or
maintain a `config.example.py` instead.

## 4. Run the producers

Each producer is self-contained and takes its credentials on the CLI — same
as the professor's `avro_producer.py`. Launch them as background processes
with `nohup`:

INSERT PRODUCER KEYS INSIDE THE CODE BELOW!!! "Endpoint...": Insert YOUR Key

```bash
# Order lifecycle feed
nohup python order_producer.py \
    iesstsabbadbaa-grp-01-05 \
    group_03_orders \
    "Endpoint=sb://...;SharedAccessKey=...;EntityPath=group_03_orders" \
    --orders-per-hour 120 --speed-multiplier 1.0 \
    > order_producer.out 2>&1 &

# Courier GPS feed
nohup python courier_producer.py \
    iesstsabbadbaa-grp-01-05 \
    group_03_couriers \
    "Endpoint=sb://...;SharedAccessKey=...;EntityPath=group_03_couriers" \
    --num-couriers 30 --speed-multiplier 1.0 \
    > courier_producer.out 2>&1 &
```

Tail the logs to see throughput/demand stats every 10 seconds:
```bash
tail -f order_producer.out courier_producer.out
```

For an accelerated demo, pass `--speed-multiplier 10.0` to both producers.

## 5. Run the Spark job (Colab)

The Spark script is structured as 13 `BLOCK N` sections. In Colab:

> **Important — Colab compatibility:** Colab now ships Spark 4.x, which is
> incompatible with the Kafka/Avro connectors we need (you'll see
> `NoSuchFieldError: TASK_ATTEMPT_ID`). Before BLOCK 2, run in its own cell:
>
> ```python
> !pip install -q pyspark==3.5.1
> ```
>
> Then **Restart runtime** (Runtime → Restart). BLOCK 2 pins the Maven
> packages to Spark 3.5.1 / Scala 2.12 to match.

1. Upload `config.py` and `spark_streaming_to_blob.py` to the runtime.

2. Paste each `BLOCK N` into its own notebook cell **in order**.
   - BLOCK 1 imports `config.py` — make sure the file is in the same directory.
   - BLOCK 2 builds the `SparkSession` with the Kafka + Avro + Azure Blob jars.
   - BLOCKS 4-7 set up the two Kafka sources and deserialize Avro.
   - BLOCK 10 launches the long-running Parquet writers. Leave this running.
   - BLOCK 11 is the live display loop — will print both feeds every 10 s.
   - BLOCK 12 verifies data landed.
   - BLOCK 13 lists and (optionally) stops active queries.

To run the whole file as a script (e.g. on a VM), simply:
```bash
python spark_streaming_to_blob.py
```
BLOCK 11 uses `IPython.display.clear_output`, which is a no-op in a plain
terminal; you'll still see the `show()` tables printed linearly.

## 6. Verify Parquet files in Blob

After the Spark job has been running for a few minutes, files should appear
under the `stream-output/` prefix in the `group03output` container.

**Azure CLI:**
```bash
az storage blob list \
    --account-name iesstsabbadbaa \
    --container-name group03output \
    --prefix stream-output/orders/ \
    --query "[].name" --output table
```

**Spark (Colab, cell after BLOCK 12):**
```python
spark.read.parquet(order_output_path).count()
spark.read.parquet(order_output_path).show(5, truncate=False)
spark.read.parquet(courier_output_path).count()
```

If the counts grow over successive reads, the streaming job is working.

## 7. Partition strategy — quantitative justification

Both Event Hubs use **4 partitions** with the Kafka message key set to the
**`zone_id`** (UTF-8 encoded).

**Why 4 partitions:**

| Concern                   | Numbers                                                                                                                                                         |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Zone cardinality          | 5 zones (downtown / midtown / uptown / brooklyn / queens). Using 4 partitions avoids a dead partition while still giving per-zone locality for most consumers.  |
| Per-partition throughput  | Standard tier caps at **1 MB/s or 1000 msg/s per partition**.                                                                                                   |
| Projected peak throughput | Dinner peak (hour multiplier 1.9 × weekend 1.3 ≈ 2.47) at baseline 120 orders/h × 6 lifecycle events ≈ **2.0 orders events/s**. Couriers: 30 × 1 ping / 15 s = **2 pings/s**. Combined peak ≤ 5 msg/s across 2 topics — well under the 4 × 1000 msg/s ceiling. |
| Spark parallelism         | `spark.sql.shuffle.partitions=4` matches the Kafka partition count, keeping the Kafka→Spark mapping 1:1.                                                        |
| Ordering guarantee        | Kafka guarantees per-partition ordering. Keying by `zone_id` preserves lifecycle ordering within each zone, which is what zone-level aggregations rely on.      |

4 partitions leave roughly **200× headroom** for bursts. We sized for
burstiness and future scale-out, not steady-state throughput.

## 8. Retention rationale — 3 days

Event Hubs retention is set to **3 days** (72 hours) on both topics.

1. **Spark replay window.** The job's checkpoint lives in Blob
   (`checkpoint/orders/`, `checkpoint/couriers/`). If the Colab runtime or
   driver dies on a Friday evening and is detected only on Monday morning,
   3 days still covers the gap without losing events.
2. **Cost.** Standard tier includes 1 day; each extra day is billed per GB.
   Typical daily volume ≈ 200 MB/day/topic at current rates; 3 days × 2 topics
   × ~$0.03/GB ≈ a few cents per month. Negligible.
3. **Blob is the source of truth.** Parquet in Blob has effectively unlimited
   retention. Event Hubs only has to act as a replay buffer, not an archive.
4. **PII (GPS pings).** Shorter retention on the buffer keeps the
   data-minimisation story straightforward for any downstream compliance
   review. 3 days is the upper bound of "operational" replay needs for this
   project.

## 9. Handoff contract — analytics team

Once the Spark job has been running, the analytics team can consume from the
following stable locations. Schemas and paths will not change silently; any
breaking change will be published under a versioned path
(`stream-output/orders_v2/`).

### Order lifecycle feed

- **Parquet path**: `wasbs://group03output@iesstsabbadbaa.blob.core.windows.net/stream-output/orders/`
- **Recommended consumer-group id**: `analytics-orders-v1`
- **Schema** (see `order_producer.py` for the authoritative Avro schema):

| Column                    | Type      | Notes                                                            |
|---------------------------|-----------|------------------------------------------------------------------|
| event_id                  | string    | unique per event (`evt_<uuid4>`)                                 |
| order_id                  | string    | `ord_<hex10>`                                                    |
| restaurant_id             | string    | `rest_NNN`                                                       |
| courier_id                | string    | null before COURIER_ASSIGNED                                     |
| zone_id                   | string    | one of 5 NYC zones; partition key                                |
| event_type                | string    | PLACED/CONFIRMED/PREPARING/ASSIGNED/PICKED_UP/DELIVERED/CANCELLED|
| event_time                | long      | epoch millis — business time                                     |
| ingestion_time            | long      | epoch millis — edge-time                                         |
| order_value_cents         | int       |                                                                  |
| delivery_fee_cents        | int       |                                                                  |
| item_count                | int       |                                                                  |
| payment_method            | string    | CREDIT_CARD / DEBIT_CARD / DIGITAL_WALLET / CASH                 |
| platform                  | string    | IOS / ANDROID / WEB                                              |
| is_duplicate              | boolean   | flag set on ~3% of events (plus the second copy)                 |
| estimated_delivery_time   | long      | nullable                                                         |
| actual_delivery_time      | long      | populated on DELIVERED                                           |
| prep_duration_seconds     | int       | populated on COURIER_ASSIGNED; ~1% are < 0 or > 7200 (anomaly)   |
| delivery_duration_seconds | int       | populated on DELIVERED; ~1% anomalous                            |
| customer_rating           | int       | 1-5 or null                                                      |
| cancellation_reason       | string    | populated on CANCELLED                                           |

### Courier location feed

- **Parquet path**: `wasbs://group03output@iesstsabbadbaa.blob.core.windows.net/stream-output/couriers/`
- **Recommended consumer-group id**: `analytics-gps-v1`
- **Schema**:

| Column            | Type    | Notes                                                           |
|-------------------|---------|-----------------------------------------------------------------|
| event_id          | string  | `loc_<uuid4>`                                                   |
| courier_id        | string  | `courier_NNN`                                                   |
| order_id          | string  | nullable (ONLINE_IDLE / OFFLINE couriers have no order)         |
| zone_id           | string  | partition key                                                   |
| event_time        | long    | epoch millis                                                    |
| ingestion_time    | long    | epoch millis                                                    |
| latitude          | double  | WGS-84                                                          |
| longitude         | double  | WGS-84                                                          |
| speed_kmh         | double  | > 200 is an anomaly (~2%)                                       |
| heading_degrees   | double  | 0-360                                                           |
| courier_status    | string  | ONLINE_IDLE / ONLINE_PICKUP / ONLINE_DELIVERING / OFFLINE       |
| vehicle_type      | string  | BICYCLE / MOTORCYCLE / CAR / SCOOTER                            |
| battery_pct       | int     | 5-100                                                           |
| network_type      | string  | 5G / 4G / 3G / NO_SIGNAL                                        |
| is_duplicate      | boolean | ~3% duplicates                                                  |

### Edge cases the analytics team must handle

| Edge case                                    | Rate  | Guidance                                                                                               |
|----------------------------------------------|-------|--------------------------------------------------------------------------------------------------------|
| Late arrivals (`ingestion_time >> event_time`) | ~5%  | Use `event_time` for business-time windows; compare to `ingestion_time` to bound lag.                  |
| Duplicates                                   | ~3%   | Dedupe on `event_id`. `is_duplicate=True` marks the generated case but retries may produce others.     |
| Missing lifecycle step                       | ~2%   | Do not assume every lifecycle state is present. Join on `order_id` and reconstruct.                    |
| Impossible durations                         | ~1%   | Filter `prep_duration_seconds BETWEEN 0 AND 7200` for averages / KPIs.                                 |
| Courier offline mid-delivery                 | ~3%   | Gaps > 30 s in `event_time` per `courier_id` can be legitimate.                                        |
| Anomalous GPS speed                          | ~2%   | Filter `speed_kmh <= 120` for realistic-travel metrics.                                                |

### Explicitly **not** in our scope

- Business-layer KPIs / metrics / alerts — the **analytics team** owns these.
- Dashboards and any UI — the **UI team** owns these.
- Schema evolution — coordinate with the ingestion team before producer
  changes; we will publish a versioned path rather than mutate columns in
  place.
