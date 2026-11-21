
import json
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "food-nutrition-data"
OUTPUT_FILE = Path("data/consumed_products.json")
STATS_FILE = Path("data/kafka_stats.json")

# Termination condition: stop when no new message arrives for 10 seconds
CONSUMER_TIMEOUT_MS = 10000


def deserialize_message(value):
    """JSON deserialization: UTF-8 bytes -> JSON string -> Python dict.
    A broken message returns None instead of stopping the consumer."""
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def create_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="food-validation-group",
            value_deserializer=deserialize_message,
            consumer_timeout_ms=CONSUMER_TIMEOUT_MS
        )

        print("Connected to Kafka successfully!")
        return consumer

    except KafkaError as error:
        print(f"Consumer connection error: {error}")
        return None


def save_stats(received, skipped):
    stats = {}
    if STATS_FILE.exists():
        try:
            stats = json.loads(STATS_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            stats = {}

    stats["consumer"] = {
        "topic": TOPIC_NAME,
        "consumed": received,
        "skipped_invalid_json": skipped,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    STATS_FILE.parent.mkdir(exist_ok=True)
    STATS_FILE.write_text(json.dumps(stats, indent=4), encoding="utf-8")


def consume_products(consumer):
    products = []
    skipped = 0

    try:
        # The loop ends automatically after CONSUMER_TIMEOUT_MS without messages
        for message in consumer:
            if message.value is None:
                skipped += 1
                print(f"Skipped message at offset {message.offset}: invalid JSON")
                continue

            products.append(message.value)

            if len(products) % 50 == 0:
                print(f"Received {len(products)} messages...")

    except KafkaError as error:
        print(f"Error while consuming messages: {error}")

    finally:
        consumer.close()

    print("\nConsumer Summary")
    print(f"Total messages received: {len(products)}")
    print(f"Messages skipped (invalid JSON): {skipped}")

    if not products:
        # Do not overwrite the previous file with an empty list
        print(
            "No new messages. The consumer group may have already read the "
            "topic - recreate the topic and run producer.py again "
            "(python main.py does this automatically)."
        )
        return False

    OUTPUT_FILE.parent.mkdir(exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(products, file, ensure_ascii=False, indent=4)

    save_stats(len(products), skipped)
    print(f"Records saved to: {OUTPUT_FILE}")
    return True


def main():
    consumer = create_consumer()

    if consumer is None:
        raise SystemExit(1)

    if not consume_products(consumer):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

