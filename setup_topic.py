import sys
import time
import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_order_events_topic(bootstrap_servers="localhost:9092", topic_name="order-events"):
    print(f"Connecting to Kafka admin client at {bootstrap_servers}...")
    admin_client = None
    for attempt in range(1, 6):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="setup-topic-admin"
            )
            break
        except Exception as e:
            print(f"[RETRY {attempt}/5] Could not connect to Kafka: {e}")
            time.sleep(3)

    if not admin_client:
        print("Failed to connect to Kafka admin client.")
        sys.exit(1)

    topic = NewTopic(
        name=topic_name,
        num_partitions=3,
        replication_factor=1,
        topic_configs={"retention.ms": "3600000"}
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully with 3 partitions and retention.ms=3600000.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Error creating topic '{topic_name}': {e}")

    admin_client.close()

def main():
    parser = argparse.ArgumentParser(description="Kafka Topic Creator")
    parser.add_argument("--bootstrap-server", type=str, default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", type=str, default="order-events", help="Kafka topic name")
    args = parser.parse_args()

    create_order_events_topic(args.bootstrap_server, args.topic)

if __name__ == "__main__":
    main()
