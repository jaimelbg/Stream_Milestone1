# Food Delivery Streaming Simulator

A production-style streaming data generator for a simulated food delivery platform.

The project produces two correlated feeds — order lifecycle events and courier GPS telemetry — designed for testing event-time pipelines, stream joins, watermarking, deduplication, and anomaly detection.

Built with pure Python (stdlib only). Outputs JSON and Avro (Object Container File).

---

## Overview

This generator simulates a full day of food delivery operations across multiple zones with realistic demand patterns and operational noise.

Unlike clean demo datasets, this project intentionally injects production-style edge cases:

- Out-of-order events (event time vs ingestion time)
- Duplicate event emissions
- Missing lifecycle steps
- Negative or extreme duration values
- Courier offline interruptions mid-delivery
- GPS anomalies (impossible speeds)

The goal is to provide realistic data suitable for serious stream-processing experiments.

---

## Files

```
generate.py                    — data generator (run this)
README.md                      — documentation
output/                        — created automatically on first run
```

Generated on execution:

```
order_events_sample.json
courier_locations_sample.json
order_events_sample.avro
courier_locations_sample.avro
run_YYYYMMDD_HHMMSS.txt
```

---

## Run

```bash
python generate.py
python generate.py --orders 500
python generate.py --couriers 50 --seed 7
python generate.py --orders 500 --couriers 50 --seed 42
```

### CLI Parameters

| Flag         | Description                          | Default |
|--------------|--------------------------------------|----------|
| `--orders`   | Number of simulated orders           | 150      |
| `--couriers` | Number of couriers in the fleet      | 20       |
| `--seed`     | Random seed for reproducibility      | None     |
| `--out`      | Output directory                     | `output` |

**Requirements:** Python 3.10+, no external libraries.

All timestamps are Unix epoch milliseconds (UTC).

---

# The Two Feeds

## Feed 1 — Order Lifecycle Events

One event per order state transition:

```
ORDER_PLACED
→ ORDER_CONFIRMED
→ ORDER_PREPARING
→ COURIER_ASSIGNED
→ COURIER_PICKED_UP
→ ORDER_DELIVERED
or → ORDER_CANCELLED
```

Orders may cancel after confirmation and include structured cancellation reasons.

### Why This Feed Exists

Captures all business-critical moments of an order. Enables:

- Revenue aggregation
- SLA monitoring
- Cancellation rate analysis
- Delivery time tracking
- Event-time windowing
- Stream-stream joins with courier telemetry
- Stream-table joins (zones, restaurants)

### Key Fields

```
event_id
order_id
restaurant_id
courier_id (nullable until assigned)
zone_id
event_type
event_time
ingestion_time
order_value_cents
delivery_fee_cents
item_count
payment_method
platform
estimated_delivery_time
actual_delivery_time
prep_duration_seconds
delivery_duration_seconds
cancellation_reason
is_duplicate
```

Monetary values are stored in cents to prevent floating-point rounding issues.

---

## Feed 2 — Courier Location Events

One GPS ping every 15 seconds per active courier during delivery windows.

### Why This Feed Exists

Enables:

- Real-time ETA calculation
- Courier utilisation metrics
- Zone-level demand heatmaps
- Session window testing
- Telemetry anomaly detection
- Stream-stream joins with order events

### Key Fields

```
event_id
courier_id
order_id
zone_id
event_time
ingestion_time
latitude
longitude
speed_kmh
heading_degrees
courier_status
vehicle_type
battery_pct
network_type
is_duplicate
```

---

## Demand Model

Orders are distributed using realistic temporal and geographic skew:

- Hourly multipliers  
  - Lunch peak (~12:00)  
  - Dinner peak (~18:00–19:00)  
  - Low overnight demand  

- Weekend uplift  
  - +30% total order volume on Saturday and Sunday  

- Weighted zone demand  
  - Downtown receives higher order density than outer zones  

This creates non-uniform traffic patterns similar to real delivery platforms.

---

## Edge Cases Injected

All injected with configurable probabilities.

| Edge Case | How It Appears | Why It Matters |
|------------|----------------|----------------|
| Late arrivals | `ingestion_time > event_time + 60s` | Tests watermark tolerance |
| Duplicates | `is_duplicate=True` re-emitted event | Tests deduplication logic |
| Missing step | `COURIER_PICKED_UP` skipped | Tests lifecycle validation |
| Impossible durations | Negative or extreme duration values | Tests anomaly side outputs |
| Courier offline | `courier_status=OFFLINE` mid-delivery | Tests session gap handling |
| Anomalous speed | `speed_kmh > 200` | Tests telemetry validation |

A summary of injected anomalies is printed after each run.

---

## Schema Design Notes

- Two timestamps per event  
  `event_time` and `ingestion_time` allow correct event-time windowing and watermark simulation. The delta between them represents out-of-order delay.

- Money stored in cents  
  Prevents floating-point aggregation errors.

- Nullable union fields  
  `courier_id` and similar fields use `["null", "string"]` and remain null until assigned.

- Enumerated fields  
  Order statuses, courier statuses, vehicle types, and cancellation reasons use Avro enums for strict state control.

- Logical timestamp types  
  Avro schemas use `timestamp-millis` for compatibility with stream processors.

- Duplicate flag  
  `is_duplicate` allows downstream deduplication without producer-side state tracking.

---

## Output

Written to the configured output directory:

```
order_events_sample.json
courier_locations_sample.json
order_events_sample.avro
courier_locations_sample.avro
run_YYYYMMDD_HHMMSS.txt
```

JSON files contain the first 200 events per feed for inspection.

Avro files use the Object Container File (OCF) format and include logical timestamp types.

---

## Example Streaming Analytics

This dataset supports realistic streaming exercises:

1. 5-minute tumbling window — orders and revenue per zone  
2. SLA alert — `actual_delivery_time > estimated_delivery_time + 10 min`  
3. Courier utilisation — delivering vs idle ratio  
4. Anomaly stream — impossible durations, speeds, offline interruptions  
5. Sliding window heatmap — 15-minute rolling order density  
6. Late-event monitoring — ingestion delay distribution  

---

## Use Cases

Designed for:

- Kafka topic simulation (two-topic architecture)
- Flink or Spark Structured Streaming experiments
- Event-time correctness testing
- Deduplication and watermark tuning
- Stream-stream join demonstrations
- Data engineering portfolio projects

---

## Project Structure

```
.
├── generate.py
├── README.md
└── output/
```

The `output/` directory is generated automatically and should typically be excluded from version control.

---

## Purpose

This project demonstrates practical understanding of:

- Event-driven system design
- Streaming data modeling
- Watermark-aware pipelines
- Data quality and anomaly injection
- Schema evolution readiness
- Realistic production failure modes
