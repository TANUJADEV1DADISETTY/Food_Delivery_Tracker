import argparse
import json
import random
import time
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Status lifecycle
STATUSES = ["PLACED", "CONFIRMED", "PREPARING", "OUT_FOR_DELIVERY", "DELIVERED"]

def load_fixtures(file_path="fixtures.json"):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading fixtures file '{file_path}': {e}")
        sys.exit(1)

def create_producer(bootstrap_servers="localhost:9092", retries=3):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
                acks="all"
            )
            return producer
        except Exception as e:
            print(f"[RETRY {attempt}/{retries}] Failed to connect to Kafka broker at {bootstrap_servers}: {e}")
            if attempt < retries:
                time.sleep(3)
            else:
                print("Could not connect to Kafka after maximum retries.")
                sys.exit(1)

def send_with_retry(producer, topic, key, value, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            future = producer.send(topic, key=key, value=value)
            record_metadata = future.get(timeout=10)
            return record_metadata
        except KafkaError as ke:
            print(f"[RETRY {attempt}/{max_retries}] Error sending message for key {key}: {ke}")
            if attempt < max_retries:
                time.sleep(2)
            else:
                raise ke

def simulate_orders(num_orders, delay_sec=1.5, bootstrap_servers="localhost:9092", topic="order-events"):
    fixtures = load_fixtures()
    restaurants = fixtures.get("restaurants", ["Pizza Place"])
    customers = fixtures.get("customers", ["John Doe"])
    items_pool = fixtures.get("items", ["Pizza", "Fries"])

    producer = create_producer(bootstrap_servers=bootstrap_servers)

    print(f"Starting simulation for {num_orders} orders to topic '{topic}'...")

    for i in range(1, num_orders + 1):
        order_id = f"ORD-{i:03d}"
        customer_name = random.choice(customers)
        restaurant = random.choice(restaurants)
        num_items = random.randint(1, 4)
        order_items = random.sample(items_pool, k=min(num_items, len(items_pool)))
        estimated_delivery_minutes = random.randint(15, 45)

        for status in STATUSES:
            timestamp = datetime.now(timezone.utc).isoformat()
            message = {
                "order_id": order_id,
                "customer_name": customer_name,
                "restaurant": restaurant,
                "items": order_items,
                "status": status,
                "timestamp": timestamp,
                "estimated_delivery_minutes": estimated_delivery_minutes
            }

            try:
                send_with_retry(producer, topic, key=order_id, value=message)
                print(f"[SENT] {order_id} -> {status}", flush=True)
            except Exception as e:
                print(f"[FAILED] {order_id} -> {status}: {e}", flush=True)

            if delay_sec > 0:
                time.sleep(delay_sec)

    producer.flush()
    producer.close()
    print("Order simulation completed.")

def main():
    parser = argparse.ArgumentParser(description="Food Delivery Order Kafka Producer")
    parser.add_argument("--orders", type=int, default=10, help="Number of orders to simulate (default: 10)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay in seconds between status updates (default: 1.5)")
    parser.add_argument("--bootstrap-server", type=str, default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", type=str, default="order-events", help="Kafka topic name")
    args = parser.parse_args()

    simulate_orders(
        num_orders=args.orders,
        delay_sec=args.delay,
        bootstrap_servers=args.bootstrap_server,
        topic=args.topic
    )

if __name__ == "__main__":
    main()
