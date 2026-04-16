# courier_producer.py
# Streams courier GPS pings into Azure Event Hubs (Kafka endpoint)
# using confluent-kafka + fastavro. Same structural pattern as
# order_producer.py and the professor's Notebook 1, Cell 3 avro_producer.py.
#
# Run:
#   nohup python courier_producer.py <namespace> <topic> "<conn_str>" &
#
# Optional flags:
#   --num-couriers         how many active couriers to simulate (default 20)
#   --speed-multiplier     1.0 = real time, higher for demos

import sys
import time
import math
import uuid
import random
import argparse
from io import BytesIO
from datetime import datetime, timezone

from confluent_kafka import Producer
from fastavro import schemaless_writer


# ---------------------------------------------------------------------------
# Avro schema (inline, Notebook 1 Cell 3/14 pattern)
# ---------------------------------------------------------------------------
courier_schema = {
    "type": "record",
    "name": "CourierLocation",
    "namespace": "fooddelivery.couriers",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "courier_id", "type": "string"},
        {"name": "order_id", "type": ["null", "string"], "default": None},
        {"name": "zone_id", "type": "string"},
        {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "ingestion_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "latitude", "type": "double"},
        {"name": "longitude", "type": "double"},
        {"name": "speed_kmh", "type": "double"},
        {"name": "heading_degrees", "type": "double"},
        {"name": "courier_status", "type": "string"},   # ONLINE_IDLE/ONLINE_PICKUP/ONLINE_DELIVERING/OFFLINE
        {"name": "vehicle_type", "type": "string"},     # BICYCLE/MOTORCYCLE/CAR/SCOOTER
        {"name": "battery_pct", "type": "int"},
        {"name": "network_type", "type": "string"},     # 5G/4G/3G/NO_SIGNAL
        {"name": "is_duplicate", "type": "boolean"}
    ]
}


# ---------------------------------------------------------------------------
# NYC zone centers (approximate latitude/longitude)
# ---------------------------------------------------------------------------
zone_centers = {
    "downtown": (40.7075, -74.0090),   # Financial District
    "midtown":  (40.7549, -73.9840),   # Midtown
    "uptown":   (40.7831, -73.9712),   # Upper East Side
    "brooklyn": (40.6782, -73.9442),   # Brooklyn
    "queens":   (40.7282, -73.7949),   # Queens
}

vehicle_types = ["BICYCLE", "MOTORCYCLE", "CAR", "SCOOTER"]
network_types = ["5G", "4G", "3G", "NO_SIGNAL"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def realistic_speed(status, vehicle_type):
    # Return a km/h speed compatible with the courier's status/vehicle.
    if status == "OFFLINE":
        return 0.0
    ranges = {
        "BICYCLE":    (8, 22),
        "SCOOTER":    (15, 35),
        "MOTORCYCLE": (25, 55),
        "CAR":        (20, 60),
    }
    lo, hi = ranges[vehicle_type]
    if status == "ONLINE_IDLE":
        return random.uniform(0, lo)
    return random.uniform(lo, hi)


def init_courier(index):
    # Place a courier near a random zone center (small jitter ~1-2 km).
    zone_id = random.choice(list(zone_centers.keys()))
    lat, lon = zone_centers[zone_id]
    return {
        "courier_id":    "courier_{:03d}".format(index),
        "zone_id":       zone_id,
        "lat":           lat + random.uniform(-0.012, 0.012),
        "lon":           lon + random.uniform(-0.012, 0.012),
        "heading":       random.uniform(0, 360),
        "vehicle_type":  random.choice(vehicle_types),
        "battery_pct":   random.randint(60, 100),
        "status":        "ONLINE_IDLE",
        "order_id":      None,
        "offline_until": 0.0,
    }


def drift(state, dt_seconds):
    # Move the courier dt seconds along their heading at a realistic speed.
    # 1 deg lat ~= 111 km; longitude scales by cos(lat).
    speed = realistic_speed(state["status"], state["vehicle_type"])
    distance_km = speed * (dt_seconds / 3600.0)
    state["heading"] = (state["heading"] + random.uniform(-15, 15)) % 360.0
    rad = math.radians(state["heading"])
    dlat = (distance_km * math.cos(rad)) / 111.0
    dlon = (distance_km * math.sin(rad)) / (111.0 * max(math.cos(math.radians(state["lat"])), 0.1))
    state["lat"] += dlat
    state["lon"] += dlon


def step_courier(state, dt_seconds, speed_multiplier):
    # Advance a courier's state machine one tick.
    now = time.time()

    # Resume from forced-offline
    if state["status"] == "OFFLINE" and now >= state["offline_until"]:
        state["status"] = "ONLINE_IDLE"
        state["order_id"] = None

    # 3% chance of going offline mid-delivery
    if state["status"] == "ONLINE_DELIVERING" and random.random() < 0.03:
        state["status"] = "OFFLINE"
        state["order_id"] = None
        state["offline_until"] = now + random.uniform(30, 180) / speed_multiplier
        return

    # Coarse status transitions
    if state["status"] == "ONLINE_IDLE" and random.random() < 0.05:
        state["status"] = "ONLINE_PICKUP"
        state["order_id"] = "ord_" + uuid.uuid4().hex[:10]
    elif state["status"] == "ONLINE_PICKUP" and random.random() < 0.10:
        state["status"] = "ONLINE_DELIVERING"
    elif state["status"] == "ONLINE_DELIVERING" and random.random() < 0.08:
        state["status"] = "ONLINE_IDLE"
        state["order_id"] = None

    # Position drift
    drift(state, dt_seconds)

    # Battery: drop 1% with 30% probability per tick, floor at 5%
    if random.random() < 0.30:
        state["battery_pct"] = max(5, state["battery_pct"] - 1)


def build_ping(state):
    # Assemble the Avro record. Applies 2% anomalous speed, 5% late arrival,
    # 3% duplicate edge cases.
    speed = realistic_speed(state["status"], state["vehicle_type"])
    if random.random() < 0.02:                        # anomalous speed > 200 km/h
        speed = random.uniform(205, 320)

    event_time = now_ms()
    if random.random() < 0.05:                        # late arrival
        ingestion_time = event_time + random.randint(60000, 600000)
    else:
        ingestion_time = event_time

    if state["battery_pct"] < 15:                     # weak signal with low battery
        network = random.choice(["3G", "NO_SIGNAL"])
    else:
        network = random.choices(network_types, weights=[0.50, 0.35, 0.10, 0.05])[0]

    return {
        "event_id":         "loc_" + uuid.uuid4().hex,
        "courier_id":       state["courier_id"],
        "order_id":         state["order_id"],
        "zone_id":          state["zone_id"],
        "event_time":       event_time,
        "ingestion_time":   ingestion_time,
        "latitude":         state["lat"],
        "longitude":        state["lon"],
        "speed_kmh":        speed,
        "heading_degrees":  state["heading"],
        "courier_status":   state["status"],
        "vehicle_type":     state["vehicle_type"],
        "battery_pct":      state["battery_pct"],
        "network_type":     network,
        "is_duplicate":     random.random() < 0.03,
    }


# ---------------------------------------------------------------------------
# Delivery report callback
# ---------------------------------------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        print("[delivery] FAILED:", err, flush=True)


def avro_encode(record, schema):
    buf = BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Courier GPS Avro producer")
    parser.add_argument("event_hub_namespace")
    parser.add_argument("eventhub_name")
    parser.add_argument("eventhub_connection_string")
    parser.add_argument("--num-couriers", type=int, default=20)
    parser.add_argument("--speed-multiplier", type=float, default=1.0)
    args = parser.parse_args()

    # Kafka config identical to Notebook 1, Cell 3
    conf = {
        "bootstrap.servers": args.event_hub_namespace + ".servicebus.windows.net:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "sasl.username":     "$ConnectionString",
        "sasl.password":     args.eventhub_connection_string,
        "client.id":         "courier-producer",
    }
    producer = Producer(conf)
    topic = args.eventhub_name

    couriers = [init_courier(i + 1) for i in range(args.num_couriers)]

    print("[courier_producer] connected to", args.event_hub_namespace, "topic=" + topic)
    print("[courier_producer] simulating {} couriers at {}x".format(
        len(couriers), args.speed_multiplier))

    # One ping per active courier every 15 wall-clock seconds
    ping_interval = 15.0 / args.speed_multiplier
    sent_total = 0
    window_count = 0
    last_report = time.time()
    last_tick = time.time()

    while True:
        now = time.time()
        dt = now - last_tick
        last_tick = now

        # Advance every courier's state
        for state in couriers:
            step_courier(state, dt, args.speed_multiplier)

        # Emit one ping per courier (sleep at end paces the loop)
        for state in couriers:
            record = build_ping(state)
            payload = avro_encode(record, courier_schema)
            producer.produce(
                topic=topic,
                key=state["zone_id"].encode("utf-8"),  # partition routing by zone
                value=payload,
                on_delivery=delivery_report,
            )
            sent_total += 1
            window_count += 1
            if record["is_duplicate"]:
                producer.produce(
                    topic=topic,
                    key=state["zone_id"].encode("utf-8"),
                    value=payload,
                    on_delivery=delivery_report,
                )
                sent_total += 1
                window_count += 1

        producer.poll(0)

        # Summary every 10 seconds
        if now - last_report >= 10.0:
            online = sum(1 for c in couriers if c["status"] != "OFFLINE")
            print("[courier_producer] sent_total={} last_10s={} online={}/{}".format(
                sent_total, window_count, online, len(couriers)), flush=True)
            window_count = 0
            last_report = now

        time.sleep(ping_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[courier_producer] interrupted")
        sys.exit(0)
