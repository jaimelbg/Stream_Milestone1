# Food Delivery Streaming — Milestone 1

Generates two streaming data feeds for a simulated food delivery platform.

---

## Files

```
generate.py   — data generator (run this)
schemas.json  — Avro schema definitions for both feeds
README.md     — this file
output/       — created on first run
```

---

## Run

```bash
python generate.py                         # defaults: 150 orders, 20 couriers
python generate.py --orders 500            # more orders
python generate.py --couriers 50 --seed 7  # bigger courier fleet, different seed
```

**Requirements:** Python 3.10+, no external libraries.

**Output** (written to `output/`):
- `order_events_sample.json` — 200-event JSON sample, Feed 1
- `courier_locations_sample.json` — 200-event JSON sample, Feed 2
- `order_events_sample.avro` — 200-event Avro OCF, Feed 1
- `courier_locations_sample.avro` — 200-event Avro OCF, Feed 2

---

## The Two Feeds

### Feed 1 — Order Lifecycle Events
One event per **order state transition**:

```
ORDER_PLACED → ORDER_CONFIRMED → ORDER_PREPARING
  → COURIER_ASSIGNED → COURIER_PICKED_UP → ORDER_DELIVERED
  or → ORDER_CANCELLED
```

**Why this feed?** Captures every business-critical moment of an order. Enables revenue analytics, SLA monitoring, cancellation rates, prep-time tracking, and late delivery alerts — all keyed by `order_id`.

**Key fields:** `order_id`, `event_type`, `event_time`, `ingestion_time`, `zone_id`, `restaurant_id`, `courier_id`, `order_value_cents`, `prep_duration_seconds`, `is_duplicate`

---

### Feed 2 — Courier Location Events
One GPS ping **per courier every 15 seconds** while active (~400 events/s at 100 couriers).

**Why this feed?** Drives real-time ETAs, courier utilisation dashboards, zone-level demand heatmaps, and anomaly detection (offline couriers, impossible speeds). Joins to Feed 1 via `order_id` and `courier_id`.

**Key fields:** `courier_id`, `order_id`, `zone_id`, `event_time`, `ingestion_time`, `latitude`, `longitude`, `speed_kmh`, `courier_status`, `is_duplicate`

---

## Demand Model

Orders are distributed using:
- **Hourly multipliers** — lunch peak (12:00), dinner peak (18:00–19:00), quiet overnight
- **Weekend uplift** — +30% total orders on Sat/Sun
- **Zone skew** — downtown gets 3× the orders of suburban zones

---

## Edge Cases

All injected with configurable probabilities:

| Edge Case | How it appears | Why it matters |
|---|---|---|
| **Late arrivals** | `ingestion_time` > `event_time` + 60s | Tests watermark tolerance |
| **Duplicates** | `is_duplicate=True`, same logical event re-emitted | Tests dedup operators |
| **Missing step** | `COURIER_PICKED_UP` skipped | Tests incomplete sequence handling |
| **Impossible durations** | `prep_duration_seconds` < 0 or > 7200 | Tests anomaly side-outputs |
| **Courier offline** | `courier_status=OFFLINE` mid-delivery | Tests session window gaps |
| **Anomalous speed** | `speed_kmh` > 200 | Tests GPS anomaly detection |

---

## Schema Design Notes

- **Two timestamps per event** (`event_time` + `ingestion_time`): enables correct event-time windowing and watermark calculation. The delta between them is the out-of-order delay.
- **Cents for money** (`order_value_cents`): avoids floating-point rounding in aggregations.
- **Nullable union fields**: `courier_id`, `order_id` etc. are `["null", "string"]` — null until assigned, never a sentinel string value.
- **`is_duplicate` flag**: allows downstream deduplication without maintaining a full seen-set on the producer side.
- **Join keys**: `order_id` and `courier_id` appear in both feeds, enabling stream-stream joins. `zone_id` and `restaurant_id` support stream-table joins against reference data.

---

## Planned Analytics (Milestone 2+)

1. 5-min tumbling window: orders/revenue per zone
2. SLA alert: `actual_delivery_time > estimated_delivery_time + 10 min`
3. Courier utilisation: `ONLINE_DELIVERING` vs `ONLINE_IDLE` ratio
4. Anomaly stream: impossible durations, speeds, offline mid-delivery
5. Zone demand heatmap: 15-min sliding window of order density
