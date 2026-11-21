"""
Food Nutrition Analyzer - Part 7: Pipeline Verification and Data Quality

Checks that all pipeline stages are connected and consistent:
    1. Counts records at every stage (API -> Kafka -> validation ->
       cleaning -> SQLite3) and explains the differences.
    2. Reads the number of published / consumed messages directly from the
       Kafka broker (topic offsets and consumer-group offsets).
    3. Verifies that the database contains exactly the cleaned data.
    4. Runs the database loading stage again and proves that it does not
       create duplicates or change the stored data (idempotency).
    5. Runs additional data-quality checks.

Results are printed and saved to reports/pipeline_verification.txt and
reports/pipeline_verification.json.

Run from the project root (Kafka broker should be running for step 2):
    python src/verify_pipeline.py
"""

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

RAW_FILE = Path("data/raw_products.json")
CONSUMED_FILE = Path("data/consumed_products.json")
VALID_FILE = Path("data/valid_products.csv")
INVALID_FILE = Path("data/invalid_products.csv")
CLEANED_FILE = Path("data/cleaned_products.csv")
REMOVED_FILE = Path("data/removed_products.csv")
KAFKA_STATS_FILE = Path("data/kafka_stats.json")
DATABASE_FILE = Path("data/food_nutrition.db")
REPORT_TEXT = Path("reports/pipeline_verification.txt")
REPORT_JSON = Path("reports/pipeline_verification.json")

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "food-nutrition-data"
CONSUMER_GROUP = "food-validation-group"

NUTRIENT_COLUMNS = [
    "energy_kcal_100g", "sugars_100g", "proteins_100g",
    "fat_100g", "carbohydrates_100g", "salt_100g",
]

report_lines = []
results = {}


def log(text=""):
    print(text)
    report_lines.append(text)


def check(name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    log(f"  [{status}] {name}" + (f" - {details}" if details else ""))
    results.setdefault("checks", []).append(
        {"check": name, "passed": bool(passed), "details": details}
    )
    return passed


# ----------------------------------------------------------------------
# 1. Kafka counts from the broker
# ----------------------------------------------------------------------
def get_kafka_counts():
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        return None, "kafka-python is not installed"

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVER,
            group_id=CONSUMER_GROUP,
            enable_auto_commit=False,
            request_timeout_ms=10000,
        )
        partitions = consumer.partitions_for_topic(TOPIC_NAME)
        if not partitions:
            consumer.close()
            return None, f"topic '{TOPIC_NAME}' not found on the broker"

        topic_partitions = [TopicPartition(TOPIC_NAME, p) for p in partitions]
        begin = consumer.beginning_offsets(topic_partitions)
        end = consumer.end_offsets(topic_partitions)
        published = sum(end[tp] - begin[tp] for tp in topic_partitions)
        committed = sum(
            (consumer.committed(tp) or begin[tp]) - begin[tp]
            for tp in topic_partitions
        )
        consumer.close()
        return {
            "partitions": len(topic_partitions),
            "messages_in_topic": published,
            "committed_by_consumer_group": committed,
        }, None

    except Exception as error:  # broker not running, network error, ...
        return None, f"Kafka broker not reachable at {KAFKA_SERVER}: {error}"


# ----------------------------------------------------------------------
# 2. Stage counts
# ----------------------------------------------------------------------
def load_stage_data():
    missing = [str(p) for p in
               [RAW_FILE, CONSUMED_FILE, VALID_FILE, INVALID_FILE,
                CLEANED_FILE, DATABASE_FILE] if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing pipeline outputs: " + ", ".join(missing) +
            ". Run the previous stages first (python main.py)."
        )

    with open(RAW_FILE, encoding="utf-8") as file:
        raw = json.load(file)
    with open(CONSUMED_FILE, encoding="utf-8") as file:
        consumed = json.load(file)

    valid = pd.read_csv(VALID_FILE, dtype={"code": str})
    invalid = pd.read_csv(INVALID_FILE, dtype={"code": str})
    cleaned = pd.read_csv(CLEANED_FILE, dtype={"code": str})

    with sqlite3.connect(DATABASE_FILE) as connection:
        stored = pd.read_sql_query("SELECT * FROM products", connection)

    return raw, consumed, valid, invalid, cleaned, stored


def report_stage_counts(raw, consumed, valid, invalid, cleaned, stored, kafka):
    log("=" * 70)
    log("FOOD NUTRITION ANALYZER - PIPELINE VERIFICATION")
    log("=" * 70)
    log("\n1. RECORD COUNTS PER STAGE")

    kafka_stats = {}
    if KAFKA_STATS_FILE.exists():
        kafka_stats = json.loads(KAFKA_STATS_FILE.read_text(encoding="utf-8"))
    producer_published = kafka_stats.get("producer", {}).get("published")
    results["kafka_stats_file"] = kafka_stats

    published = kafka["messages_in_topic"] if kafka else producer_published
    counts = {
        "collected_from_api": len(raw),
        "published_to_kafka": published,
        "consumed_from_kafka": len(consumed),
        "valid_records": len(valid),
        "invalid_records": len(invalid),
        "records_after_cleaning": len(cleaned),
        "records_in_sqlite": len(stored),
    }
    results["counts"] = counts

    labels = {
        "collected_from_api": "Collected from API (raw_products.json)",
        "published_to_kafka": ("Published to Kafka (broker offsets)" if kafka
                               else "Published to Kafka (producer log)"),
        "consumed_from_kafka": "Consumed from Kafka (consumed_products.json)",
        "valid_records": "Valid after Pandera (valid_products.csv)",
        "invalid_records": "Invalid after Pandera (invalid_products.csv)",
        "records_after_cleaning": "After cleaning (cleaned_products.csv)",
        "records_in_sqlite": "Stored in SQLite3 (products table)",
    }
    for key, label in labels.items():
        value = counts[key]
        log(f"  {label:<48} {value if value is not None else 'n/a'}")
    if kafka:
        log(f"  {'Offsets committed by ' + CONSUMER_GROUP:<48} "
            f"{kafka['committed_by_consumer_group']}")
    if producer_published is not None:
        failed = kafka_stats["producer"].get("failed", 0)
        log(f"  {'Producer log: acknowledged / failed':<48} "
            f"{producer_published} / {failed}")
    return counts


def explain_differences(raw, consumed, valid, invalid, cleaned, stored, kafka):
    log("\n2. EXPLANATION OF DIFFERENCES")

    # API -> Kafka
    if kafka:
        published = kafka["messages_in_topic"]
        if published == len(raw):
            check("API records == messages in Kafka topic", True,
                  f"{published} = {len(raw)}")
        else:
            check("API records == messages in Kafka topic", False,
                  f"{published} vs {len(raw)}. If the producer was run more "
                  f"than once the topic holds several copies - recreate the "
                  f"topic (python main.py does this) and run again.")
    else:
        log("  [INFO] Kafka broker not reachable - published count taken from "
            "data/kafka_stats.json (producer delivery callbacks). Start Kafka "
            "to verify it against the topic offsets.")

    # Kafka -> consumer
    raw_codes = [p.get("code") for p in raw]
    consumed_codes = [p.get("code") for p in consumed]
    check("Consumed messages == collected records",
          len(consumed) == len(raw), f"{len(consumed)} vs {len(raw)}")
    check("Consumed messages contain the same products as the API data",
          sorted(map(str, raw_codes)) == sorted(map(str, consumed_codes)))
    check("Consumed messages are identical to the API records (JSON round-trip)",
          raw == consumed)

    # Consumer -> validation
    total_validated = len(valid) + len(invalid)
    check("valid + invalid == consumed", total_validated == len(consumed),
          f"{len(valid)} + {len(invalid)} = {total_validated}")

    reasons = []
    for _, row in invalid.iterrows():
        row_reasons = []
        if pd.isna(row["product_name"]):
            row_reasons.append("missing product_name")
        if pd.notna(row["energy_kcal_100g"]) and row["energy_kcal_100g"] > 1000:
            row_reasons.append(f"energy {row['energy_kcal_100g']:.0f} kcal > 1000")
        for column in NUTRIENT_COLUMNS[1:]:
            if pd.notna(row[column]) and row[column] > 100:
                row_reasons.append(f"{column} {row[column]:.1f} > 100")
        reasons.append(f"    {row['code']}: {', '.join(row_reasons) or 'see validation log'}")
    log(f"  Invalid records ({len(invalid)}) and the rules they broke:")
    for line in reasons:
        log(line)

    # Validation -> cleaning (cleaning.py logs every removed record)
    removed = (pd.read_csv(REMOVED_FILE, dtype={"code": str})
               if REMOVED_FILE.exists() else pd.DataFrame(columns=["code", "reason"]))
    log(f"\n  Cleaning stage: {len(valid)} -> {len(cleaned)} "
        f"({len(valid) - len(cleaned)} removed)")
    if not removed.empty:
        for reason, count in removed["reason"].value_counts().items():
            log(f"    {reason:<40} {count}")
    accounted = set(cleaned["code"]) | set(removed["code"])
    check("Every validated record is either cleaned or logged as removed",
          len(cleaned) + len(removed) == len(valid) and
          accounted == set(valid["code"]),
          f"{len(cleaned)} + {len(removed)} = {len(cleaned) + len(removed)}")

    # Cleaning -> database
    check("Records after cleaning == records in SQLite3",
          len(cleaned) == len(stored), f"{len(cleaned)} vs {len(stored)}")


# ----------------------------------------------------------------------
# 3. Database content
# ----------------------------------------------------------------------
def verify_database(cleaned, stored):
    log("\n3. DATABASE VERIFICATION (SQL QUERIES)")

    with sqlite3.connect(DATABASE_FILE) as connection:
        cursor = connection.cursor()

        queries = {
            "Total rows":
                "SELECT COUNT(*) FROM products",
            "Distinct product codes":
                "SELECT COUNT(DISTINCT code) FROM products",
            "Codes that appear more than once":
                "SELECT COUNT(*) FROM (SELECT code FROM products "
                "GROUP BY code HAVING COUNT(*) > 1)",
            "Rows with NULL product_name":
                "SELECT COUNT(*) FROM products WHERE product_name IS NULL",
            "Rows with negative nutrient values":
                "SELECT COUNT(*) FROM products WHERE energy_kcal_100g < 0 "
                "OR sugars_100g < 0 OR proteins_100g < 0 OR fat_100g < 0 "
                "OR carbohydrates_100g < 0 OR salt_100g < 0",
            "Rows with sugars > carbohydrates (should be filtered)":
                "SELECT COUNT(*) FROM products "
                "WHERE sugars_100g > carbohydrates_100g + 0.5",
            "Barcodes with a leading zero (must be kept)":
                "SELECT COUNT(*) FROM products WHERE code LIKE '0%'",
        }
        query_results = {}
        for title, sql in queries.items():
            value = cursor.execute(sql).fetchone()[0]
            query_results[title] = value
            log(f"  {title:<45} {value}")
            log(f"      SQL: {sql}")

        primary_key = [row for row in cursor.execute("PRAGMA table_info(products)")
                       if row[5] == 1]
        log("\n  Average nutrients per 100 g (SQL AVG):")
        averages = cursor.execute(
            "SELECT ROUND(AVG(energy_kcal_100g),1), ROUND(AVG(sugars_100g),1), "
            "ROUND(AVG(proteins_100g),1), ROUND(AVG(fat_100g),1) FROM products"
        ).fetchone()
        log(f"      kcal={averages[0]}, sugar={averages[1]}, "
            f"protein={averages[2]}, fat={averages[3]}")

        log("\n  Top 5 brands by product count (SQL GROUP BY):")
        for brand, count in cursor.execute(
            "SELECT brands, COUNT(*) AS n FROM products GROUP BY brands "
            "ORDER BY n DESC LIMIT 5"
        ):
            log(f"      {brand:<20} {count}")

    log("")
    check("Table has a PRIMARY KEY on code",
          len(primary_key) == 1 and primary_key[0][1] == "code")
    check("No duplicate product codes in the database",
          query_results["Codes that appear more than once"] == 0)
    check("No NULL product names",
          query_results["Rows with NULL product_name"] == 0)
    check("No negative nutrient values",
          query_results["Rows with negative nutrient values"] == 0)
    check("No products with sugars > carbohydrates",
          query_results["Rows with sugars > carbohydrates (should be filtered)"] == 0)

    # Same products and same values as the cleaned CSV
    check("Database contains exactly the cleaned products",
          set(stored["code"]) == set(cleaned["code"]))

    merged = cleaned.merge(stored, on="code", suffixes=("_csv", "_db"))
    value_columns = NUTRIENT_COLUMNS + ["total_macros"]
    values_match = all(
        np.allclose(merged[f"{c}_csv"], merged[f"{c}_db"], equal_nan=True)
        for c in value_columns
    )
    names_match = (merged["product_name_csv"] == merged["product_name_db"]).all()
    check("Stored nutrient values equal the cleaned values", values_match)
    check("Stored product names equal the cleaned names", bool(names_match))
    results["sql_queries"] = query_results


# ----------------------------------------------------------------------
# 4. Idempotency
# ----------------------------------------------------------------------
def table_fingerprint(connection):
    rows = connection.execute("SELECT * FROM products ORDER BY code").fetchall()
    digest = hashlib.sha256(repr(rows).encode("utf-8")).hexdigest()
    return len(rows), digest


def verify_idempotency():
    log("\n4. IDEMPOTENCY TEST (repeated loading)")
    try:
        import database
    except ImportError as error:
        log(f"  Could not import database.py: {error}")
        return

    connection = sqlite3.connect(DATABASE_FILE)
    try:
        count_before, hash_before = table_fingerprint(connection)
        log(f"  Before repeated load: {count_before} rows, "
            f"SHA-256 {hash_before[:16]}...")

        for run in (1, 2):
            log(f"  Running database.load_data() again (run {run})...")
            database.load_data(connection)

        count_after, hash_after = table_fingerprint(connection)
        log(f"  After 2 repeated loads: {count_after} rows, "
            f"SHA-256 {hash_after[:16]}...")
    finally:
        connection.close()

    check("Row count unchanged after repeated loading",
          count_before == count_after, f"{count_before} -> {count_after}")
    check("Stored data unchanged after repeated loading (same hash)",
          hash_before == hash_after)
    results["idempotency"] = {
        "rows_before": count_before, "rows_after": count_after,
        "hash_before": hash_before, "hash_after": hash_after,
    }


# ----------------------------------------------------------------------
# 5. Data quality
# ----------------------------------------------------------------------
def data_quality_checks(valid, cleaned, stored):
    log("\n5. DATA QUALITY")

    stored_codes = set(stored["code"])
    partly_missing = valid[valid[NUTRIENT_COLUMNS].isna().any(axis=1)
                           & valid["energy_kcal_100g"].notna()]
    filled_in_db = stored_codes & set(partly_missing["code"])
    log(f"  Stored products that had some missing nutrients "
        f"(filled during cleaning): {len(filled_in_db)}")
    no_energy_in_db = stored_codes & set(
        valid.loc[valid["energy_kcal_100g"].isna(), "code"])
    check("No product without a real energy value is stored",
          len(no_energy_in_db) == 0, f"{len(no_energy_in_db)} found")

    leading_zero_codes = set(valid.loc[valid["code"].str.startswith("0"), "code"])
    check("Barcodes with leading zeros are preserved",
          leading_zero_codes <= stored_codes | set(
              pd.read_csv(REMOVED_FILE, dtype={"code": str})["code"]
              if REMOVED_FILE.exists() else []),
          ", ".join(sorted(leading_zero_codes)))

    brand_variants = stored["brands"].nunique() - \
        stored["brands"].str.lower().str.strip().nunique()
    check("Brand spellings normalized (no upper/lower-case duplicates)",
          brand_variants == 0, f"{brand_variants} variants")

    macros_over_100 = stored[stored["total_macros"] > 100.5]
    check("protein + fat + carbs <= 100 g for every product",
          len(macros_over_100) == 0, f"{len(macros_over_100)} found")

    calculated = (4 * stored["proteins_100g"] + 4 * stored["carbohydrates_100g"]
                  + 9 * stored["fat_100g"]).to_numpy()
    declared = stored["energy_kcal_100g"].to_numpy()
    mismatch = (np.abs(declared - calculated) > 30) & (
        np.abs(declared - calculated) / np.maximum(declared, 1) > 0.25)
    log(f"  [INFO] Declared kcal differs from 4P+4C+9F by >25%: "
        f"{int(mismatch.sum())} products (source data errors in Open Food "
        f"Facts, kept and reported in the EDA)")

    results["data_quality"] = {
        "products_with_filled_nutrients": len(filled_in_db),
        "energy_mismatch": int(mismatch.sum()),
        "brand_case_variants": int(brand_variants),
    }


# ----------------------------------------------------------------------
def main():
    import os
    os.chdir(PROJECT_ROOT)  # all stages use paths relative to the project root

    try:
        raw, consumed, valid, invalid, cleaned, stored = load_stage_data()
    except FileNotFoundError as error:
        print(error)
        sys.exit(1)

    kafka, kafka_error = get_kafka_counts()
    if kafka_error:
        print(f"Note: {kafka_error}")
    results["kafka"] = kafka or {"error": kafka_error}

    report_stage_counts(raw, consumed, valid, invalid, cleaned, stored, kafka)
    explain_differences(raw, consumed, valid, invalid, cleaned, stored, kafka)
    verify_database(cleaned, stored)
    verify_idempotency()
    data_quality_checks(valid, cleaned, stored)

    failed = [c for c in results["checks"] if not c["passed"]]
    log("\n" + "=" * 70)
    log(f"SUMMARY: {len(results['checks']) - len(failed)} of "
        f"{len(results['checks'])} checks passed")
    for item in failed:
        log(f"  FAILED: {item['check']}")
    log("=" * 70)

    REPORT_TEXT.parent.mkdir(exist_ok=True)
    REPORT_TEXT.write_text("\n".join(report_lines), encoding="utf-8")
    REPORT_JSON.write_text(json.dumps(results, indent=2, ensure_ascii=False),
                           encoding="utf-8")
    print(f"\nReport saved to: {REPORT_TEXT} and {REPORT_JSON}")


if __name__ == "__main__":
    main()
