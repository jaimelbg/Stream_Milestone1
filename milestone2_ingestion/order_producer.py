# order_producer.py
# Streams order lifecycle events into Azure Event Hubs (Kafka endpoint)
# using confluent-kafka + fastavro Avro serialization.
#
# Matches the professor's Notebook 1, Cell 3 pattern (avro_producer.py):
#   - confluent_kafka.Producer class
#   - fastavro.schemaless_writer into BytesIO
#   - SASL_SSL / PLAIN with "$ConnectionString" username
#   - infinite while loop with time.sleep()
#   - launched via nohup as a background process
#   - CLI args: event_hub_namespace, eventhub_name, eventhub_connection_string
#
# Run:
#   nohup python order_producer.py <namespace> <topic> "<conn_str>" &
#
# Optional flags:
#   --orders-per-hour      baseline new-order rate (default 60)
#   --speed-multiplier     1.0 = real time, 10.0 = 10x for demos

import sys
import time
import uuid
import random
import argparse
from io import BytesIO
from datetime import datetime, timezone

from confluent_kafka import Producer
from fastavro import schemaless_writer


# ---------------------------------------------------------------------------
# Avro schema (inline Python dict, same pattern as Notebook 1, Cells 3 & 14).
# Mirrors Milestone 1's order_lifecycle_event.avsc.
# ---------------------------------------------------------------------------
order_schema = {
    "type": "record",
    "name": "OrderLifecycleEvent",
    "namespace": "fooddelivery.orders",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "restaurant_id", "type": "string"},
        {"name": "courier_id", "type": ["null", "string"], "default": None},
        {"name": "zone_id", "type": "string"},
        {"name": "event_type", "type": {
            "type": "enum",
            "name": "OrderEventType",
            "symbols": [
                "ORDER_PLACED",
                "ORDER_CONFIRMED",
                "ORDER_PREPARING",
                "COURIER_ASSIGNED",
                "COURIER_PICKED_UP",
                "ORDER_DELIVERED",
                "ORDER_CANCELLED"
            ]
        }},
        {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "ingestion_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "order_value_cents", "type": "int"},
        {"name": "delivery_fee_cents", "type": "int"},
        {"name": "item_count", "type": "int"},
        {"name": "payment_method", "type": "string"},
        {"name": "platform", "type": "string"},
        {"name": "is_duplicate", "type": "boolean"},
        {"name": "estimated_delivery_time", "type": ["null", "long"], "default": None},
        {"name": "actual_delivery_time", "type": ["null", "long"], "default": None},
        {"name": "prep_duration_seconds", "type": ["null", "int"], "default": None},
        {"name": "delivery_duration_seconds", "type": ["null", "int"], "default": None},
        {"name": "customer_rating", "type": ["null", "int"], "default": None},
        {"name": "cancellation_reason", "type": ["null", "string"], "default": None}
    ]
}


# ---------------------------------------------------------------------------
# Reference data (ported from Milestone 1 generate.py)
# ---------------------------------------------------------------------------
# zone_id + weight — downtown 3x suburban (zone skew from the demand model)
zones = [
    ("downtown", 3.0),
    ("midtown",  2.0),
    ("uptown",   1.5),
    ("brooklyn", 1.0),
    ("queens",   1.0),
]

restaurants = ["rest_{:03d}".format(i) for i in range(1, 201)]
couriers    = ["courier_{:03d}".format(i) for i in range(1, 501)]

payment_methods = ["CREDIT_CARD", "DEBIT_CARD", "DIGITAL_WALLET", "CASH"]
platforms       = ["IOS", "ANDROID", "WEB"]
cancel_reasons  = [
    "CUSTOMER_CHANGED_MIND", "RESTAURANT_CLOSED", "NO_COURIER_AVAILABLE",
    "PAYMENT_FAILED", "ADDRESS_UNREACHABLE"
]

# Hourly demand multipliers — lunch peak around noon, dinner peak 18-19.
hour_multipliers = {
    0: 0.15, 1: 0.10, 2: 0.08, 3: 0.05, 4: 0.05, 5: 0.10,
    6: 0.25, 7: 0.45, 8: 0.55, 9: 0.50, 10: 0.60, 11: 0.90,
    12: 1.60, 13: 1.45, 14: 0.90, 15: 0.70, 16: 0.75, 17: 1.20,
    18: 1.80, 19: 1.90, 20: 1.60, 21: 1.10, 22: 0.70, 23: 0.40,
}

# Realistic delay ranges per lifecycle transition, in seconds (lo, hi).
transition_delays = {
    "ORDER_PLACED":      (30,   120),   # -> CONFIRMED    (30-120s)
    "ORDER_CONFIRMED":   (20,   90),    # -> PREPARING
    "ORDER_PREPARING":   (300,  900),   # -> COURIER_ASSIGNED (5-15 min)
    "COURIER_ASSIGNED":  (120,  600),   # -> PICKED_UP
    "COURIER_PICKED_UP": (300,  1500),  # -> DELIVERED    (5-25 min)
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def now_ms():
    # Current epoch time in milliseconds (UTC).
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def pick_zone():
    # Weighted random zone selection.
    total = sum(w for _, w in zones)
    r = random.uniform(0, total)
    acc = 0.0
    for zid, w in zones:
        acc += w
        if r <= acc:
            return zid
    return zones[-1][0]


def current_demand_multiplier():
    # Combine hour-of-day factor with weekend (+30%) uplift.
    now = datetime.now()
    base = hour_multipliers.get(now.hour, 1.0)
    if now.weekday() >= 5:  # Saturday / Sunday
        base *= 1.30
    return base


# ---------------------------------------------------------------------------
# Order simulation
# ---------------------------------------------------------------------------
def new_order_seed():
    # Static fields that stay the same across all events for one order.
    return {
        "order_id":           "ord_" + uuid.uuid4().hex[:10],
        "restaurant_id":      random.choice(restaurants),
        "zone_id":            pick_zone(),
        "order_value_cents":  random.randint(800, 9500),
        "delivery_fee_cents": random.randint(199, 599),
        "item_count":         random.randint(1, 6),
        "payment_method":     random.choice(payment_methods),
        "platform":           random.choice(platforms),
        "courier_id":         None,
        "prep_started_at":    None,
        "picked_up_at":       None,
    }


def plan_order_lifecycle(seed, speed_multiplier):
    # Build the full list of (fire_at_ts, event_type, extras) for one order.
    # Applies: ~8% cancellations, ~2% missing intermediate step,
    # ~1% impossible durations on prep/delivery.
    plan = []
    t = time.time()
    plan.append((t, "ORDER_PLACED", {}))

    will_cancel = random.random() < 0.08
    cancel_after = None
    if will_cancel:
        cancel_after = random.choice([
            "ORDER_PLACED", "ORDER_CONFIRMED", "ORDER_PREPARING", "COURIER_ASSIGNED"
        ])

    drop_step = None
    if random.random() < 0.02:
        drop_step = random.choice([
            "ORDER_CONFIRMED", "ORDER_PREPARING", "COURIER_ASSIGNED"
        ])

    # Helper closures
    def advance(state):
        # mutate t by a transition delay
        nonlocal_t = t  # not actually needed, we return new value
        lo, hi = transition_delays[state]
        return nonlocal_t + random.uniform(lo, hi) / speed_multiplier

    if will_cancel and cancel_after == "ORDER_PLACED":
        t = advance("ORDER_PLACED")
        plan.append((t, "ORDER_CANCELLED",
                     {"cancellation_reason": random.choice(cancel_reasons)}))
        return plan

    # CONFIRMED
    t = advance("ORDER_PLACED")
    if drop_step != "ORDER_CONFIRMED":
        plan.append((t, "ORDER_CONFIRMED", {}))
    if will_cancel and cancel_after == "ORDER_CONFIRMED":
        t = advance("ORDER_CONFIRMED")
        plan.append((t, "ORDER_CANCELLED",
                     {"cancellation_reason": random.choice(cancel_reasons)}))
        return plan

    # PREPARING
    t = advance("ORDER_CONFIRMED")
    seed["prep_started_at"] = t
    if drop_step != "ORDER_PREPARING":
        plan.append((t, "ORDER_PREPARING", {}))
    if will_cancel and cancel_after == "ORDER_PREPARING":
        t = advance("ORDER_PREPARING")
        plan.append((t, "ORDER_CANCELLED",
                     {"cancellation_reason": random.choice(cancel_reasons)}))
        return plan

    # COURIER_ASSIGNED
    t = advance("ORDER_PREPARING")
    seed["courier_id"] = random.choice(couriers)
    prep_duration = int(t - seed["prep_started_at"])
    if random.random() < 0.01:
        # impossible durations: negative or > 7200
        prep_duration = random.choice([-random.randint(1, 300),
                                       random.randint(7300, 20000)])
    if drop_step != "COURIER_ASSIGNED":
        plan.append((t, "COURIER_ASSIGNED",
                     {"prep_duration_seconds": prep_duration}))
    if will_cancel and cancel_after == "COURIER_ASSIGNED":
        t = advance("COURIER_ASSIGNED")
        plan.append((t, "ORDER_CANCELLED",
                     {"cancellation_reason": random.choice(cancel_reasons)}))
        return plan

    # PICKED_UP
    t = advance("COURIER_ASSIGNED")
    seed["picked_up_at"] = t
    plan.append((t, "COURIER_PICKED_UP", {}))

    # DELIVERED
    t = advance("COURIER_PICKED_UP")
    delivery_duration = int(t - seed["picked_up_at"])
    if random.random() < 0.01:
        delivery_duration = random.choice([-random.randint(1, 300),
                                           random.randint(7300, 20000)])
    rating = random.choice([None, 3, 4, 4, 5, 5, 5, 2, 1])
    plan.append((t, "ORDER_DELIVERED", {
        "actual_delivery_time":       int(t * 1000),
        "delivery_duration_seconds":  delivery_duration,
        "customer_rating":            rating,
    }))
    return plan


def build_event(seed, event_type, event_time_ms, extras):
    # Assemble the Avro record for one event. Applies late-arrival
    # (~5%) and duplicate-flag (~3%) edge cases.
    is_duplicate = random.random() < 0.03
    if random.random() < 0.05:
        ingestion_time = event_time_ms + random.randint(60000, 900000)  # 1-15 min lag
    else:
        ingestion_time = now_ms()

    record = {
        "event_id":                 "evt_" + uuid.uuid4().hex,
        "order_id":                 seed["order_id"],
        "restaurant_id":            seed["restaurant_id"],
        "courier_id":               seed["courier_id"],
        "zone_id":                  seed["zone_id"],
        "event_type":               event_type,
        "event_time":               event_time_ms,
        "ingestion_time":           ingestion_time,
        "order_value_cents":        seed["order_value_cents"],
        "delivery_fee_cents":       seed["delivery_fee_cents"],
        "item_count":               seed["item_count"],
        "payment_method":           seed["payment_method"],
        "platform":                 seed["platform"],
        "is_duplicate":             is_duplicate,
        "estimated_delivery_time":  None,
        "actual_delivery_time":     None,
        "prep_duration_seconds":    None,
        "delivery_duration_seconds": None,
        "customer_rating":          None,
        "cancellation_reason":      None,
    }
    record.update(extras)
    return record


# ---------------------------------------------------------------------------
# Delivery report callback — same shape as professor's Cell 3.
# ---------------------------------------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        print("[delivery] FAILED:", err, flush=True)
    # Silent on success to keep the log readable (professor's style).


def avro_encode(record, schema):
    # fastavro schemaless_writer into BytesIO — exactly matches Cell 3.
    buf = BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Order lifecycle Avro producer")
    parser.add_argument("event_hub_namespace")
    parser.add_argument("eventhub_name")
    parser.add_argument("eventhub_connection_string")
    parser.add_argument("--orders-per-hour", type=int, default=60)
    parser.add_argument("--speed-multiplier", type=float, default=1.0)
    args = parser.parse_args()

    # Kafka config identical to Notebook 1, Cell 3
    conf = {
        "bootstrap.servers": args.event_hub_namespace + ".servicebus.windows.net:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "sasl.username":     "$ConnectionString",
        "sasl.password":     args.eventhub_connection_string,
        "client.id":         "order-producer",
    }
    producer = Producer(conf)
    topic = args.eventhub_name

    print("[order_producer] connected to", args.event_hub_namespace, "topic=" + topic)
    print("[order_producer] baseline={}/h  speed={}x".format(
        args.orders_per_hour, args.speed_multiplier))

    # Pending lifecycle events waiting to fire: list of (fire_at, seed, evt_type, extras)
    pending = []
    sent_total = 0
    window_count = 0
    last_report = time.time()
    last_new_order = time.time()

    while True:
        now = time.time()

        # Schedule new orders according to the demand model
        demand = current_demand_multiplier()
        effective_rate = args.orders_per_hour * demand * args.speed_multiplier  # per hour
        interval = 3600.0 / max(effective_rate, 1e-6)
        if now - last_new_order >= interval:
            seed = new_order_seed()
            for fire_at, evt_type, extras in plan_order_lifecycle(seed, args.speed_multiplier):
                pending.append((fire_at, seed, evt_type, extras))
            last_new_order = now

        # Fire any events whose scheduled time has arrived
        pending.sort(key=lambda x: x[0])
        due = [p for p in pending if p[0] <= now]
        pending = [p for p in pending if p[0] > now]

        for fire_at, seed, evt_type, extras in due:
            record = build_event(seed, evt_type, int(fire_at * 1000), extras)
            payload = avro_encode(record, order_schema)
            # Message key = zone_id, for partition routing by zone
            producer.produce(
                topic=topic,
                key=seed["zone_id"].encode("utf-8"),
                value=payload,
                on_delivery=delivery_report,
            )
            sent_total += 1
            window_count += 1
            # Duplicate edge case: re-send the exact same payload
            if record["is_duplicate"]:
                producer.produce(
                    topic=topic,
                    key=seed["zone_id"].encode("utf-8"),
                    value=payload,
                    on_delivery=delivery_report,
                )
                sent_total += 1
                window_count += 1

        producer.poll(0)

        # Summary every 10 seconds
        if now - last_report >= 10.0:
            print("[order_producer] sent_total={} last_10s={} pending={} demand={:.2f}".format(
                sent_total, window_count, len(pending), demand), flush=True)
            window_count = 0
            last_report = now

        time.sleep(0.2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[order_producer] interrupted")
        sys.exit(0)
