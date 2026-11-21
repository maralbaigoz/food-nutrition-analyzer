
import sqlite3
import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/cleaned_products.csv")
DATABASE_FILE = Path("data/food_nutrition.db")

TABLE_COLUMNS = [
    "code",
    "product_name",
    "brands",
    "categories",
    "energy_kcal_100g",
    "sugars_100g",
    "proteins_100g",
    "fat_100g",
    "carbohydrates_100g",
    "salt_100g",
    "last_modified_t",
    "total_macros"
]


def create_database():
    DATABASE_FILE.parent.mkdir(exist_ok=True)

    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            code TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            brands TEXT,
            categories TEXT,
            energy_kcal_100g REAL,
            sugars_100g REAL,
            proteins_100g REAL,
            fat_100g REAL,
            carbohydrates_100g REAL,
            salt_100g REAL,
            last_modified_t TEXT,
            total_macros REAL
        )
    """)

    connection.commit()
    return connection


def load_data(connection):
    # code is read as text so that barcodes keep their leading zeros
    df = pd.read_csv(INPUT_FILE, dtype={"code": str})

    df["last_modified_t"] = pd.to_datetime(
        df["last_modified_t"], errors="coerce"
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Columns are selected explicitly so the insert does not depend on
    # the column order of the CSV file
    df = df[TABLE_COLUMNS].astype(object)
    df = df.where(pd.notna(df), None)

    records = list(df.itertuples(index=False, name=None))

    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM products")
    count_before = cursor.fetchone()[0]

    # INSERT OR IGNORE + PRIMARY KEY(code): a product that is already
    # stored is skipped, so repeated runs never create duplicates
    placeholders = ", ".join("?" for _ in TABLE_COLUMNS)
    cursor.executemany(f"""
        INSERT OR IGNORE INTO products ({", ".join(TABLE_COLUMNS)})
        VALUES ({placeholders})
    """, records)

    connection.commit()

    cursor.execute("SELECT COUNT(*) FROM products")
    stored_count = cursor.fetchone()[0]

    print(f"Processed records: {len(records)}")
    print(f"Records stored before this run: {count_before}")
    print(f"New records inserted: {stored_count - count_before}")
    print(f"Records stored in database: {stored_count}")
    print(f"Database file: {DATABASE_FILE}")


def main():
    print("Food Nutrition Analyzer - SQLite3 Database")

    if not INPUT_FILE.exists():
        print(f"Input file not found: {INPUT_FILE}. Run cleaning.py first.")
        raise SystemExit(1)

    try:
        connection = create_database()
    except sqlite3.Error as error:
        print(f"Database error: {error}")
        raise SystemExit(1)

    try:
        load_data(connection)
        print("\nDatabase operation completed successfully!")

    except sqlite3.Error as error:
        print(f"Database error: {error}")
        raise SystemExit(1)

    finally:
        connection.close()


if __name__ == "__main__":
    main()

