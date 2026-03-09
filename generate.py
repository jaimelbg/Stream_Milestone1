"""
generate.py — Food Delivery Streaming Data Generator
=====================================================
Generates two feeds:
  1. order_events      — order lifecycle (placed → delivered/cancelled)
  2. courier_locations — GPS pings from couriers

Outputs:
  output/order_events_sample.json
  output/courier_locations_sample.json
  output/order_events_sample.avro
  output/courier_locations_sample.avro

Run:
  python generate.py
  python generate.py --orders 200 --couriers 20 --seed Random
"""

import argparse
import io
import json
import math
import os
import random
import struct
import uuid
from datetime import datetime, timedelta, timezone

# ── Config ────────────────────────────────────────────────────────────────────

ZONES = {
    "downtown":  {"lat": 40.7128, "lon": -74.0060, "weight": 3.0},
    "midtown":   {"lat": 40.7549, "lon": -73.9840, "weight": 2.5},
    "brooklyn":  {"lat": 40.6782, "lon": -73.9442, "weight": 2.0},
    "queens":    {"lat": 40.7282, "lon": -73.7949, "weight": 1.5},
    "suburbs":   {"lat": 40.8448, "lon": -73.8648, "weight": 0.8},
}

# Hourly demand multipliers (0=midnight … 23=11pm)
HOURLY_DEMAND = [
    0.10, 0.05, 0.03, 0.02, 0.02, 0.05,  # 00–05
    0.15, 0.30, 0.45, 0.55, 0.65, 0.85,  # 06–11
    1.00, 0.90, 0.70, 0.60, 0.65, 0.80,  # 12–17  ← lunch peak
    1.00, 0.95, 0.80, 0.60, 0.40, 0.20,  # 18–23  ← dinner peak
]

ORDER_STATUSES = [
    "ORDER_PLACED", "ORDER_CONFIRMED", "ORDER_PREPARING",
    "COURIER_ASSIGNED", "COURIER_PICKED_UP", "ORDER_DELIVERED",
    "ORDER_CANCELLED",
]

COURIER_STATUSES = ["ONLINE_IDLE", "ONLINE_PICKUP", "ONLINE_DELIVERING", "OFFLINE"]
VEHICLE_TYPES    = ["BICYCLE", "MOTORCYCLE", "CAR", "SCOOTER"]
PAYMENT_METHODS  = ["CREDIT_CARD", "DEBIT_CARD", "DIGITAL_WALLET", "CASH"]
PLATFORMS        = ["IOS", "ANDROID", "WEB"]

# Edge-case probabilities
PROB_LATE_ARRIVAL    = 0.08   # ingestion delayed > 60s after event_time
PROB_DUPLICATE       = 0.03   # event re-emitted with is_duplicate=True
PROB_MISSING_PICKUP  = 0.02   # skips COURIER_PICKED_UP step
PROB_IMPOSSIBLE_DUR  = 0.01   # negative or absurd durations
PROB_CANCEL          = 0.06
PROB_COURIER_OFFLINE = 0.02   # courier goes OFFLINE mid-delivery


# ── Helpers ───────────────────────────────────────────────────────────────────

def ts(dt): return int(dt.timestamp() * 1000)

def jitter(base, spread, rng): return round(base + rng.uniform(-spread, spread), 6)

def pick_zone(rng):
    keys = list(ZONES); weights = [ZONES[z]["weight"] for z in keys]
    return rng.choices(keys, weights=weights, k=1)[0]

def demand_scale(dt, rng):
    scale = HOURLY_DEMAND[dt.hour]
    if dt.weekday() >= 5: scale *= 1.3   # weekend boost
    return scale


# ── Reference data ────────────────────────────────────────────────────────────

def make_restaurants(n, rng):
    out = []
    for i in range(n):
        z = pick_zone(rng); zone = ZONES[z]
        out.append({"restaurant_id": f"rest_{i:03d}", "zone_id": z,
                    "lat": jitter(zone["lat"], 0.015, rng),
                    "lon": jitter(zone["lon"], 0.015, rng)})
    return out

def make_couriers(n, rng):
    out = []
    for i in range(n):
        z = pick_zone(rng); zone = ZONES[z]
        out.append({"courier_id": f"courier_{i:03d}", "zone_id": z,
                    "vehicle": rng.choice(VEHICLE_TYPES),
                    "lat": jitter(zone["lat"], 0.020, rng),
                    "lon": jitter(zone["lon"], 0.020, rng)})
    return out


# ── Feed 1: Order Lifecycle Events ────────────────────────────────────────────

def generate_order_events(n_orders, restaurants, couriers, rng):
    events = []
    start = datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)

    for _ in range(n_orders):
        # Spread orders across 24h, weighted by hourly demand
        hour    = rng.choices(range(24), weights=HOURLY_DEMAND, k=1)[0]
        placed  = start + timedelta(hours=hour, seconds=rng.randint(0, 3599))
        rest    = rng.choice(restaurants)
        courier = rng.choice(couriers)
        oid     = f"ord_{uuid.uuid4().hex[:10]}"
        zone    = rest["zone_id"]
        val     = rng.randint(800, 5500)
        fee     = rng.randint(99, 499)

        base = dict(order_id=oid, restaurant_id=rest["restaurant_id"],
                    courier_id=None, zone_id=zone,
                    order_value_cents=val, delivery_fee_cents=fee,
                    item_count=rng.randint(1, 7),
                    payment_method=rng.choice(PAYMENT_METHODS),
                    platform=rng.choice(PLATFORMS),
                    is_duplicate=False)

        def make_evt(event_type, event_dt, extra=None):
            late_s = rng.randint(60, 600) if rng.random() < PROB_LATE_ARRIVAL else rng.randint(0, 4)
            evt = {**base, **(extra or {}),
                   "event_id": f"evt_{uuid.uuid4().hex}",
                   "event_type": event_type,
                   "event_time": ts(event_dt),
                   "ingestion_time": ts(event_dt + timedelta(seconds=late_s))}
            return evt

        cancelled = rng.random() < PROB_CANCEL

        # ORDER_PLACED
        e = make_evt("ORDER_PLACED", placed)
        events.append(e)
        if rng.random() < PROB_DUPLICATE:
            events.append({**e, "is_duplicate": True,
                           "ingestion_time": e["ingestion_time"] + rng.randint(200, 3000)})

        # ORDER_CONFIRMED (+1–3 min)
        t1 = placed + timedelta(seconds=rng.randint(60, 180))
        eta = t1 + timedelta(seconds=rng.randint(1800, 3600))
        events.append(make_evt("ORDER_CONFIRMED", t1,
                               {"estimated_delivery_time": ts(eta)}))

        if cancelled and rng.random() < 0.5:
            events.append(make_evt("ORDER_CANCELLED",
                                   t1 + timedelta(seconds=rng.randint(10, 120)),
                                   {"cancellation_reason": "CUSTOMER_CANCELLED"}))
            continue

        # ORDER_PREPARING (+1–2 min)
        t2 = t1 + timedelta(seconds=rng.randint(60, 120))
        events.append(make_evt("ORDER_PREPARING", t2))

        # COURIER_ASSIGNED (+2–8 min)
        t3 = t2 + timedelta(seconds=rng.randint(120, 480))
        events.append(make_evt("COURIER_ASSIGNED", t3,
                               {"courier_id": courier["courier_id"]}))
        base["courier_id"] = courier["courier_id"]

        # COURIER_PICKED_UP (+10–25 min prep)
        prep_s = rng.randint(600, 1500)
        if rng.random() < PROB_IMPOSSIBLE_DUR:
            prep_s = rng.choice([-60, 9000])   # anomaly
        t4 = t3 + timedelta(seconds=abs(prep_s))

        skip_pickup = rng.random() < PROB_MISSING_PICKUP
        if not skip_pickup:
            events.append(make_evt("COURIER_PICKED_UP", t4,
                                   {"prep_duration_seconds": prep_s}))

        # ORDER_DELIVERED (+10–40 min transit)
        del_s = rng.randint(600, 2400)
        if rng.random() < PROB_IMPOSSIBLE_DUR:
            del_s = rng.choice([-30, 14400])   # anomaly
        t5 = t4 + timedelta(seconds=abs(del_s))
        events.append(make_evt("ORDER_DELIVERED", t5,
                               {"prep_duration_seconds": prep_s,
                                "delivery_duration_seconds": del_s,
                                "actual_delivery_time": ts(t5),
                                "estimated_delivery_time": ts(eta)}))

    events.sort(key=lambda e: e["event_time"])
    return events


# ── Feed 2: Courier Location Events ───────────────────────────────────────────

def generate_location_events(order_events, couriers, rng, ping_interval=15):
    """Emit GPS pings for every courier that appears in order_events."""
    from collections import defaultdict

    # Build per-courier active windows from order events
    courier_map = {c["courier_id"]: c for c in couriers}
    windows = defaultdict(list)  # courier_id → [(start_ms, end_ms, order_id, zone_id)]

    order_by_id = defaultdict(dict)
    for e in order_events:
        order_by_id[e["order_id"]][e["event_type"]] = e

    for oid, phases in order_by_id.items():
        if "COURIER_ASSIGNED" not in phases: continue
        cid    = phases["COURIER_ASSIGNED"].get("courier_id")
        if not cid: continue
        start  = phases["COURIER_ASSIGNED"]["event_time"]
        end    = phases.get("ORDER_DELIVERED", phases.get("ORDER_CANCELLED",
                 phases["COURIER_ASSIGNED"]))["event_time"] + 60_000
        zone   = phases["COURIER_ASSIGNED"]["zone_id"]
        windows[cid].append((start, end, oid, zone))

    pings = []
    for cid, spans in windows.items():
        courier = courier_map.get(cid, {})
        lat = courier.get("lat", 40.71)
        lon = courier.get("lon", -74.00)
        vehicle = courier.get("vehicle", "BICYCLE")
        max_spd = {"BICYCLE": 22, "SCOOTER": 40, "MOTORCYCLE": 55, "CAR": 65}.get(vehicle, 30)

        went_offline = False
        offline_until = 0

        for (start_ms, end_ms, order_id, zone_id) in sorted(spans):
            t = start_ms
            while t <= end_ms:
                # random small drift
                lat += rng.uniform(-0.0002, 0.0002)
                lon += rng.uniform(-0.0002, 0.0002)
                speed = round(rng.uniform(max_spd * 0.4, max_spd * 0.9), 1)

                # anomalous speed
                if rng.random() < PROB_IMPOSSIBLE_DUR:
                    speed = round(rng.uniform(200, 320), 1)

                # courier offline mid-delivery
                if not went_offline and rng.random() < PROB_COURIER_OFFLINE:
                    went_offline = True
                    offline_until = t + rng.randint(3, 7) * ping_interval * 1000

                status = "OFFLINE" if t < offline_until else "ONLINE_DELIVERING"
                if t >= offline_until: went_offline = False

                late_ms = rng.randint(60_000, 600_000) if rng.random() < PROB_LATE_ARRIVAL \
                          else rng.randint(0, 2000)

                ping = {
                    "event_id": f"loc_{uuid.uuid4().hex}",
                    "courier_id": cid,
                    "order_id": order_id,
                    "zone_id": zone_id,
                    "event_time": t,
                    "ingestion_time": t + late_ms,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "speed_kmh": speed,
                    "heading_degrees": round(rng.uniform(0, 360), 1),
                    "courier_status": status,
                    "vehicle_type": vehicle,
                    "battery_pct": rng.randint(15, 100),
                    "network_type": rng.choices(["5G","4G","3G","NO_SIGNAL"],
                                                weights=[0.30,0.45,0.20,0.05])[0],
                    "is_duplicate": False,
                }
                pings.append(ping)

                # duplicate injection
                if rng.random() < PROB_DUPLICATE:
                    dup = {**ping, "is_duplicate": True,
                           "ingestion_time": ping["ingestion_time"] + rng.randint(200, 4000)}
                    pings.append(dup)

                t += ping_interval * 1000

    pings.sort(key=lambda p: p["event_time"])
    return pings


# ── Avro Writer (pure stdlib) ─────────────────────────────────────────────────

def _zz(n):
    """Zigzag-encode integer to variable-length bytes."""
    n = (n << 1) ^ (n >> 63)
    buf = []
    while True:
        b = n & 0x7F; n >>= 7
        buf.append(b if n == 0 else b | 0x80)
        if n == 0: break
    return bytes(buf)

def _enc(schema, value):
    """Encode value according to schema (subset of Avro types)."""
    if isinstance(schema, list):  # union
        t = None
    elif isinstance(schema, str):
        t = schema
    else:
        t = schema.get("type")

    if isinstance(schema, list):
        for i, branch in enumerate(schema):
            bt = branch if isinstance(branch, str) else branch.get("type")
            if branch == "null" and value is None: return _zz(i)
            if bt in ("string","long","int","boolean","double","float","enum","map") and value is not None:
                return _zz(i) + _enc(branch, value)
        return _zz(0)  # fallback null

    if t == "null":    return b""
    if t == "boolean": return b"\x01" if value else b"\x00"
    if t in ("int","long"):
        v = int(value); v = (v << 1) ^ (v >> 63)
        buf = []
        while True:
            b = v & 0x7F; v >>= 7
            buf.append(b if v == 0 else b | 0x80)
            if v == 0: break
        return bytes(buf)
    if t == "float":   return struct.pack("<f", float(value))
    if t == "double":  return struct.pack("<d", float(value))
    if t == "string":
        s = str(value).encode(); return _zz(len(s)) + s
    if t == "enum":
        return _zz(schema["symbols"].index(value))
    if t == "map":
        if not value: return _zz(0)
        out = _zz(len(value))
        for k, v in value.items():
            out += _enc("string", k) + _enc(schema["values"], v)
        return out + _zz(0)
    if t == "record":
        return b"".join(_enc(f["type"], value.get(f["name"], f.get("default")))
                        for f in schema["fields"])
    raise ValueError(f"Unsupported type: {t}")

def write_avro(records, schema, path):
    sync = os.urandom(16)
    meta = {"avro.schema": json.dumps(schema).encode(), "avro.codec": b"null"}
    # header
    hdr = b"Obj\x01"
    hdr += _zz(len(meta))
    for k, v in meta.items():
        k_b = k.encode(); hdr += _zz(len(k_b)) + k_b + _zz(len(v)) + v
    hdr += _zz(0) + sync

    encoded = [_enc(schema, r) for r in records]
    payload = b"".join(encoded)
    block = _zz(len(encoded)) + _zz(len(payload)) + payload + sync

    with open(path, "wb") as f:
        f.write(hdr + block)


# ── Schemas (inline) ──────────────────────────────────────────────────────────

ORDER_SCHEMA = {
    "type": "record", "name": "OrderLifecycleEvent",
    "namespace": "com.fooddelivery.v1",
    "fields": [
        {"name": "event_id",              "type": "string"},
        {"name": "order_id",              "type": "string"},
        {"name": "restaurant_id",         "type": "string"},
        {"name": "courier_id",            "type": ["null", "string"], "default": None},
        {"name": "zone_id",               "type": "string"},
        {"name": "event_type",            "type": {"type":"enum","name":"OET",
            "symbols": ORDER_STATUSES}},
        {"name": "event_time",            "type": {"type":"long","logicalType":"timestamp-millis"}},
        {"name": "ingestion_time",        "type": {"type":"long","logicalType":"timestamp-millis"}},
        {"name": "order_value_cents",     "type": "int"},
        {"name": "delivery_fee_cents",    "type": "int"},
        {"name": "item_count",            "type": "int"},
        {"name": "payment_method",        "type": "string"},
        {"name": "platform",              "type": "string"},
        {"name": "is_duplicate",          "type": "boolean", "default": False},
        {"name": "estimated_delivery_time","type": ["null",{"type":"long","logicalType":"timestamp-millis"}], "default": None},
        {"name": "actual_delivery_time",  "type": ["null",{"type":"long","logicalType":"timestamp-millis"}], "default": None},
        {"name": "prep_duration_seconds", "type": ["null","int"], "default": None},
        {"name": "delivery_duration_seconds","type": ["null","int"], "default": None},
        {"name": "cancellation_reason",   "type": ["null", {"type": "enum", "name": "CancellationReason",
            "symbols": ["CUSTOMER_CANCELLED","RESTAURANT_REJECTED","NO_COURIER_AVAILABLE","PAYMENT_FAILED","TIMEOUT"]}], "default": None},
    ]
}

LOCATION_SCHEMA = {
    "type": "record", "name": "CourierLocationEvent",
    "namespace": "com.fooddelivery.v1",
    "fields": [
        {"name": "event_id",       "type": "string"},
        {"name": "courier_id",     "type": "string"},
        {"name": "order_id",       "type": ["null","string"], "default": None},
        {"name": "zone_id",        "type": "string"},
        {"name": "event_time",     "type": {"type":"long","logicalType":"timestamp-millis"}},
        {"name": "ingestion_time", "type": {"type":"long","logicalType":"timestamp-millis"}},
        {"name": "latitude",       "type": "double"},
        {"name": "longitude",      "type": "double"},
        {"name": "speed_kmh",      "type": "double"},
        {"name": "heading_degrees","type": "double"},
        {"name": "courier_status", "type": {"type": "enum", "name": "CourierStatus",
            "symbols": ["ONLINE_IDLE","ONLINE_PICKUP","ONLINE_DELIVERING","OFFLINE"]}},
        {"name": "vehicle_type",   "type": {"type": "enum", "name": "VehicleType",
            "symbols": ["BICYCLE","MOTORCYCLE","CAR","SCOOTER"]}},
        {"name": "battery_pct",    "type": "int"},
        {"name": "network_type",   "type": "string"},
        {"name": "is_duplicate",   "type": "boolean", "default": False},
    ]
}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Food Delivery Feed Generator")
    parser.add_argument("--orders",   type=int, default=150, help="Number of orders (default 150)")
    parser.add_argument("--couriers", type=int, default=20,  help="Number of couriers (default 20)")
    parser.add_argument("--seed",     type=int, default= None,  help="Random seed")
    parser.add_argument("--out",      default="output",      help="Output directory")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)

    print(f"Generating {args.orders} orders, {args.couriers} couriers (seed={args.seed})...")

    restaurants = make_restaurants(max(args.orders // 5, 10), rng)
    couriers    = make_couriers(args.couriers, rng)

    # Feed 1
    order_events = generate_order_events(args.orders, restaurants, couriers, rng)
    print(f"  Order events:    {len(order_events):,}")

    # Feed 2
    loc_events = generate_location_events(order_events, couriers, rng)
    print(f"  Location pings:  {len(loc_events):,}")

    # Write JSON
    json_order = f"{args.out}/order_events_sample.json"
    json_loc   = f"{args.out}/courier_locations_sample.json"
    with open(json_order, "w") as f: json.dump(order_events[:200], f, indent=2)
    with open(json_loc,   "w") as f: json.dump(loc_events[:200],   f, indent=2)

    # Write Avro
    avro_order = f"{args.out}/order_events_sample.avro"
    avro_loc   = f"{args.out}/courier_locations_sample.avro"

    # Align records to schema fields for Avro
    def align_order(e):
        return {f["name"]: e.get(f["name"], f.get("default")) for f in ORDER_SCHEMA["fields"]}
    def align_loc(e):
        return {f["name"]: e.get(f["name"], f.get("default")) for f in LOCATION_SCHEMA["fields"]}

    write_avro([align_order(e) for e in order_events[:200]], ORDER_SCHEMA,    avro_order)
    write_avro([align_loc(e)   for e in loc_events[:200]],   LOCATION_SCHEMA, avro_loc)

    print(f"\nOutput written to '{args.out}/':")
    for fname in [json_order, json_loc, avro_order, avro_loc]:
        size = os.path.getsize(fname)
        print(f"  {fname}  ({size:,} bytes)")

    # Quick edge-case summary
    dups  = sum(1 for e in order_events if e["is_duplicate"])
    late  = sum(1 for e in order_events if e["ingestion_time"] - e["event_time"] > 60_000)
    canc  = sum(1 for e in order_events if e["event_type"] == "ORDER_CANCELLED")
    print(f"\nEdge cases injected:")
    print(f"  Duplicates:     {dups}")
    print(f"  Late arrivals:  {late}")
    print(f"  Cancellations:  {canc}")

if __name__ == "__main__":
    import sys
    from datetime import datetime

    class Tee:
        def __init__(self, *streams): self.streams = streams
        def write(self, data):
            for s in self.streams: s.write(data)
        def flush(self):
            for s in self.streams: s.flush()

    os.makedirs("output", exist_ok=True)
    log_path = f"output/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(log_path, "w") as log_file:
        sys.stdout = Tee(sys.__stdout__, log_file)
        main()
        sys.stdout = sys.__stdout__
        print("\nTerminal output saved to: " + log_path)
