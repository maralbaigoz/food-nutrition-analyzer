import json
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "food-nutrition-data"
DATA_FILE = Path("data/raw_products.json")
STATS_FILE = Path("data/kafka_stats.json")

# Counters updated by the delivery callbacks
delivery_results = {"published": 0, "failed": 0}


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            # JSON serialization: Python dict -> JSON string -> UTF-8 bytes
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False
            ).encode("utf-8"),
            # Product code as message key (same product -> same partition)
            key_serializer=lambda key: str(key).encode("utf-8"),
            acks="all",
            retries=3
        )

        print("Connected to Kafka successfully!")
        return producer

    except KafkaError as error:
        print(f"Kafka connection failed: {error}")
        return None


def on_send_success(record_metadata):
    delivery_results["published"] += 1


def on_send_error(error):
    delivery_results["failed"] += 1
    print(f"Message delivery failed: {error}")


def save_stats(total_records):
    stats = {}
    if STATS_FILE.exists():
        try:
            stats = json.loads(STATS_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            stats = {}

    stats["producer"] = {
        "topic": TOPIC_NAME,
        "records_in_file": total_records,
        "published": delivery_results["published"],
        "failed": delivery_results["failed"],
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    STATS_FILE.write_text(json.dumps(stats, indent=4), encoding="utf-8")


def send_products(producer):
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as file:
            products = json.load(file)

    except FileNotFoundError:
        print(f"Data file not found: {DATA_FILE}")
        return False

    except json.JSONDecodeError as error:
        print(f"Data file is not valid JSON: {error}")
        return False

    for product in products:
        try:
            future = producer.send(
                TOPIC_NAME, key=product.get("code"), value=product
            )
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

        except (KafkaError, TypeError, ValueError) as error:
            # e.g. a record that cannot be serialized to JSON
            delivery_results["failed"] += 1
            print(f"Could not send product {product.get('code')}: {error}")

    # Wait until all messages are acknowledged by the broker
    producer.flush()

    print(f"Records in file: {len(products)}")
    print(f"Successfully published to Kafka: {delivery_results['published']}")
    print(f"Failed messages: {delivery_results['failed']}")

    save_stats(len(products))
    return delivery_results["failed"] == 0


def main():
    producer = create_producer()

    if producer is None:
        raise SystemExit(1)

    try:
        success = send_products(producer)
    finally:
        producer.close()
        print("Producer closed.")

    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
