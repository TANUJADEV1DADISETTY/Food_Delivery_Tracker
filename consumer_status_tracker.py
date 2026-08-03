import json
import os
import time
import threading
import argparse
from flask import Flask, jsonify
from flask_cors import CORS
from kafka import KafkaConsumer

app = Flask(__name__)
CORS(app)

# In-memory active order state and lock
active_orders = {}
state_lock = threading.Lock()
STATE_FILE = "state.json"

@app.route("/state", methods=["GET"])
def get_state():
    with state_lock:
        # Return a copy of active orders
        return jsonify(dict(active_orders)), 200

def persist_state_loop(interval_sec=10):
    """Periodically write in-memory state to state.json"""
    while True:
        time.sleep(interval_sec)
        try:
            with state_lock:
                state_copy = dict(active_orders)
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(state_copy, f, indent=2)
            # print(f"[PERSIST] Saved state with {len(state_copy)} active orders to {STATE_FILE}")
        except Exception as e:
            print(f"[ERROR] Failed to save state file: {e}")

def run_kafka_consumer(bootstrap_servers, topic, group_id):
    global active_orders
    print(f"Starting Consumer A (Status Tracker) on group '{group_id}'...")

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

            order_id = event.get("order_id")
            new_status = event.get("status")
            restaurant = event.get("restaurant", "Unknown")
            est_mins = event.get("estimated_delivery_minutes", "~")

            if not order_id or not new_status:
                continue

            with state_lock:
                old_order = active_orders.get(order_id)
                old_status = old_order.get("status") if old_order else None

                if old_status and old_status != new_status:
                    print(f"[UPDATE] {order_id}: {old_status} -> {new_status}", flush=True)
                elif not old_status:
                    print(f"[UPDATE] {order_id}: -> {new_status}", flush=True)

                if new_status == "DELIVERED":
                    print(f"[COMPLETE] {order_id} | {restaurant} | ~{est_mins} min", flush=True)
                    if order_id in active_orders:
                        del active_orders[order_id]
                else:
                    active_orders[order_id] = event

def main():
    parser = argparse.ArgumentParser(description="Consumer A: Status Tracker")
    parser.add_argument("--port", type=int, default=5000, help="Port for GET /state endpoint (default: 5000)")
    parser.add_argument("--bootstrap-server", type=str, default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", type=str, default="order-events", help="Kafka topic name")
    parser.add_argument("--group-id", type=str, default="status-tracker", help="Consumer group ID")
    args = parser.parse_args()

    # Start persistence thread
    persist_thread = threading.Thread(target=persist_state_loop, args=(10,), daemon=True)
    persist_thread.start()

    # Start Kafka consumer thread
    consumer_thread = threading.Thread(
        target=run_kafka_consumer,
        args=(args.bootstrap_server, args.topic, args.group_id),
        daemon=True
    )
    consumer_thread.start()

    # Run HTTP server
    print(f"Server listening on http://0.0.0.0:{args.port}/state")
    app.run(host="0.0.0.0", port=args.port, debug=False)

if __name__ == "__main__":
    main()
