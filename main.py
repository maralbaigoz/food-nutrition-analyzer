"""
Food Nutrition Analyzer - complete pipeline runner.

Runs every stage in order as a separate Python process:

    1. collect_data.py    Open Food Facts API  -> data/raw_products.json
    2. producer.py        raw_products.json    -> Kafka topic
    3. consumer.py        Kafka topic          -> data/consumed_products.json
    4. validation.py      Pandera validation   -> valid/invalid_products.csv
    5. cleaning.py        pandas + NumPy       -> data/cleaned_products.csv
    6. database.py        SQLite3              -> data/food_nutrition.db
    7. database.py        (second run, idempotency demonstration)
    8. analysis.py        EDA + figures        -> figures/, reports/eda_summary.txt
    9. verify_pipeline.py checks + counts      -> reports/pipeline_verification.*

Usage (from the project root, Kafka broker running):
    python main.py                     # full pipeline
    python main.py --skip-collect      # reuse existing data/raw_products.json
    python main.py --from analysis     # start from a given stage
    python main.py --no-reset-topic    # do not recreate the Kafka topic
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "food-nutrition-data"

STAGES = [
    ("collect", "collect_data.py", "Data collection from Open Food Facts API"),
    ("produce", "producer.py", "Kafka Producer"),
    ("consume", "consumer.py", "Kafka Consumer"),
    ("validate", "validation.py", "Pandera validation"),
    ("clean", "cleaning.py", "Cleaning and transformation"),
    ("load", "database.py", "SQLite3 loading (run 1)"),
    ("reload", "database.py", "SQLite3 loading (run 2 - idempotency)"),
    ("analysis", "analysis.py", "Exploratory data analysis"),
    ("verify", "verify_pipeline.py", "Pipeline verification"),
]


def reset_kafka_topic():
    """
    Recreate the topic so that each full run publishes and consumes exactly
    the records of this run (no copies left from earlier runs).
    """
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
    except ImportError:
        print("kafka-python is not installed: pip install -r requirements.txt")
        return False

    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
    except Exception as error:
        print(f"Kafka broker not reachable at {KAFKA_SERVER}: {error}")
        return False

    try:
        admin.delete_topics([TOPIC_NAME])
        print(f"Deleted old topic '{TOPIC_NAME}'")
    except UnknownTopicOrPartitionError:
        pass

    # Deletion is asynchronous - retry creation for a few seconds
    for _ in range(10):
        try:
            admin.create_topics([NewTopic(TOPIC_NAME, num_partitions=1,
                                          replication_factor=1)])
            print(f"Created topic '{TOPIC_NAME}' (1 partition, replication 1)")
            break
        except TopicAlreadyExistsError:
            time.sleep(1)
    else:
        print("Could not recreate the topic; using the existing one.")
    admin.close()
    return True


def run_stage(script_name, title):
    print("\n" + "=" * 70)
    print(f"STAGE: {title}  ({script_name})")
    print("=" * 70, flush=True)
    started = time.time()
    completed = subprocess.run(
        [sys.executable, str(SRC_DIR / script_name)], cwd=PROJECT_ROOT
    )
    duration = time.time() - started
    if completed.returncode != 0:
        print(f"\nStage '{title}' failed (exit code {completed.returncode}).")
        return False
    print(f"--- finished in {duration:.1f} s")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run the Food Nutrition pipeline")
    parser.add_argument("--from", dest="start", default="collect",
                        choices=[name for name, _, _ in STAGES],
                        help="stage to start from")
    parser.add_argument("--skip-collect", action="store_true",
                        help="reuse data/raw_products.json instead of calling the API")
    parser.add_argument("--no-reset-topic", action="store_true",
                        help="keep the existing Kafka topic")
    args = parser.parse_args()

    stage_names = [name for name, _, _ in STAGES]
    start_index = stage_names.index(args.start)
    if args.skip_collect and start_index == 0:
        start_index = 1

    for name, script, title in STAGES[start_index:]:
        if name == "produce" and not args.no_reset_topic:
            if not reset_kafka_topic():
                print("Start Kafka first (see README.md, 'Kafka setup').")
                sys.exit(1)
        if not run_stage(script, title):
            sys.exit(1)

    print("\nPipeline completed. See reports/ and figures/ for the results.")


if __name__ == "__main__":
    main()
