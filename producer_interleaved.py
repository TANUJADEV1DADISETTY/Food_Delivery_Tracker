"""
Interleaved producer - publishes status updates for ALL orders simultaneously
so the dashboard shows multiple orders progressing at once.
"""
import argparse
import json
import random
import time
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

STATUSES = ["PLACED", "CONFIRMED", "PREPARING", "OUT_FOR_DELIVERY", "DELIVERED"]

def load_fixtures(file_path="fixtures.json"):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def create_producer(bootstrap_servers="localhost:9092"):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3, acks="all"
    )
    return producer

def simulate_interleaved(num_orders, delay_sec=3, bootstrap_servers="localhost:9092", topic="order-events"):
    fixtures = load_fixtures()
    restaurants = fixtures["restaurants"]
    customers = fixtures["customers"]
    items_pool = fixtures["items"]

    producer = create_producer(bootstrap_servers)

    # Pre-generate all order metadata
    orders = []
    for i in range(1, num_orders + 1):
        orders.append({
            "order_id": f"ORD-{i:03d}",
            "customer_name": random.choice(customers),
            "restaurant": random.choice(restaurants),
            "items": random.sample(items_pool, k=random.randint(1, 4)),
            "estimated_delivery_minutes": random.randint(15, 45)
        })

    print(f"Starting INTERLEAVED simulation for {num_orders} orders...")

    # Publish all orders at same status stage before moving to next
    for status in STATUSES:
        for order in orders:
            msg = {
                **order,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            producer.send(topic, key=order["order_id"], value=msg)
            print(f"[SENT] {order['order_id']} -> {status}", flush=True)
        producer.flush()
        if status != "DELIVERED":
            print(f"--- All orders now at: {status}. Waiting {delay_sec}s... ---", flush=True)
            time.sleep(delay_sec)

    producer.close()
    print("Interleaved simulation complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=8)
    parser.add_argument("--delay", type=float, default=4)
    parser.add_argument("--bootstrap-server", type=str, default="localhost:9092")
    parser.add_argument("--topic", type=str, default="order-events")
    args = parser.parse_args()
    simulate_interleaved(args.orders, args.delay, args.bootstrap_server, args.topic)
