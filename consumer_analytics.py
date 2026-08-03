import json
import time
import threading
import argparse
from collections import defaultdict
from kafka import KafkaConsumer

restaurant_counts = defaultdict(int)
status_counts = defaultdict(int)
total_messages = 0
counters_lock = threading.Lock()

def print_analytics_snapshot(interval_sec=15):
    """Periodically print snapshot of analytics counters to stdout"""
    while True:
        time.sleep(interval_sec)
        with counters_lock:
            r_dict = dict(restaurant_counts)
            s_dict = dict(status_counts)
            tot = total_messages

        print("\n=================== ANALYTICS SNAPSHOT ===================", flush=True)
        print(f"Total Messages Processed: {tot}", flush=True)
        print("Messages per Restaurant:", flush=True)
        for rest, count in sorted(r_dict.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {rest}: {count}", flush=True)
        print("Messages per Status:", flush=True)
        for st, count in sorted(s_dict.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {st}: {count}", flush=True)
        print("=========================================================\n", flush=True)

def run_kafka_consumer(bootstrap_servers, topic, group_id):
    global total_messages
    print(f"Starting Consumer B (Analytics Engine) on group '{group_id}'...")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=-1,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None
    )

    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
        except Exception as e:
            print(f"[WARN] Poll error: {e}", flush=True)
            time.sleep(1)
            continue
        for tp, messages in records.items():
          for message in messages:
            event = message.value
            if not event or not isinstance(event, dict):
                continue

            restaurant = event.get("restaurant", "Unknown")
            status = event.get("status", "Unknown")

            with counters_lock:
                restaurant_counts[restaurant] += 1
                status_counts[status] += 1
                total_messages += 1

def main():
    parser = argparse.ArgumentParser(description="Consumer B: Analytics Engine")
    parser.add_argument("--bootstrap-server", type=str, default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", type=str, default="order-events", help="Kafka topic name")
    parser.add_argument("--group-id", type=str, default="analytics", help="Consumer group ID")
    parser.add_argument("--interval", type=int, default=15, help="Snapshot print interval in seconds (default: 15)")
    args = parser.parse_args()

    # Start snapshot reporting thread
    snapshot_thread = threading.Thread(target=print_analytics_snapshot, args=(args.interval,), daemon=True)
    snapshot_thread.start()

    # Run Kafka consumer in main thread
    run_kafka_consumer(args.bootstrap_server, args.topic, args.group_id)

if __name__ == "__main__":
    main()
