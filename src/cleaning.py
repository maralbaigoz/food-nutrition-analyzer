
import pandas as pd
import numpy as np
from pathlib import Path

INPUT_FILE = Path("data/valid_products.csv")
OUTPUT_FILE = Path("data/cleaned_products.csv")
REMOVED_FILE = Path("data/removed_products.csv")

NUMERIC_COLUMNS = [
    "energy_kcal_100g",
    "sugars_100g",
    "proteins_100g",
    "fat_100g",
    "carbohydrates_100g",
    "salt_100g"
]

# Every record removed during cleaning is logged here with its reason,
# so the difference between validated and cleaned counts can be explained.
removed_records = []


def log_removed(rows, reason):
    for code, name in zip(rows["code"], rows["product_name"]):
        removed_records.append(
            {"code": code, "product_name": name, "reason": reason}
        )


def load_data():
    # Barcodes are read as text: reading them as numbers drops leading zeros
    # (e.g. 06175700 -> 6175700)
    df = pd.read_csv(INPUT_FILE, dtype={"code": str})
    print(f"Loaded records: {len(df)}")
    return df


def clean_data(df):
    print("\nStarting data cleaning...")

    # Remove duplicate products
    duplicate_mask = df.duplicated(subset=["code"])
    log_removed(df[duplicate_mask], "duplicate code")
    df = df[~duplicate_mask].copy()
    print(f"Duplicates removed: {int(duplicate_mask.sum())}")

    # Convert numerical columns
    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Missing-value analysis
    print("\nMissing values per nutrient column:")
    print(df[NUMERIC_COLUMNS].isna().sum().to_string())

    # Energy is the main variable of the project: without it a product
    # cannot be compared, and guessing it (e.g. 263 kcal for mineral
    # water) would create false data. Such products are removed.
    no_energy = df["energy_kcal_100g"].isna()
    log_removed(df[no_energy], "energy value missing")
    df = df[~no_energy].copy()
    print(f"\nRemoved products without energy value: {int(no_energy.sum())}")

    # Products with 0 kcal (water, salt, zero drinks) cannot contain
    # protein, fat or carbohydrates, so their missing values are 0.
    zero_energy = df["energy_kcal_100g"] == 0
    df.loc[zero_energy, NUMERIC_COLUMNS] = (
        df.loc[zero_energy, NUMERIC_COLUMNS].fillna(0)
    )

    # Remaining gaps (a single missing nutrient) are filled with the median
    # of products that have energy > 0 (robust to outliers)
    # Sugar is part of carbohydrates, so a filled carbohydrate value must
    # not be lower than the known sugar value, and a filled sugar value
    # must not be higher than the known carbohydrate value (NumPy max/min).
    medians = df.loc[~zero_energy, NUMERIC_COLUMNS].median()
    filled_counts = {
        column: int(df[column].isna().sum()) for column in NUMERIC_COLUMNS
    }

    df["carbohydrates_100g"] = df["carbohydrates_100g"].fillna(
        pd.Series(
            np.fmax(medians["carbohydrates_100g"], df["sugars_100g"]),
            index=df.index,
        )
    )
    df["sugars_100g"] = df["sugars_100g"].fillna(
        pd.Series(
            np.fmin(medians["sugars_100g"], df["carbohydrates_100g"]),
            index=df.index,
        )
    )
    for column in NUMERIC_COLUMNS:
        df[column] = df[column].fillna(medians[column])
    print(f"Values filled with the median: {filled_counts}")

    # Handle missing product names
    df["product_name"] = df["product_name"].fillna("Unknown product")

    df["product_name"] = df["product_name"].replace(
        ["nan", "None", ""], "Unknown product"
    )

    # Handle missing brands and categories
    for column in ["brands", "categories"]:
        df[column] = df[column].fillna("Unknown")

    # Standardize text values: trim and collapse repeated spaces
    for column in ["product_name", "brands", "categories"]:
        df[column] = (
            df[column].astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )

    # Normalize brand spelling: "Jaouda" / "jaouda", "alpro" / "Alpro"...
    # All spellings of a brand get its most frequent spelling.
    brand_key = df["brands"].str.lower()
    canonical_brand = (
        df.groupby(brand_key)["brands"]
        .agg(lambda names: names.value_counts().index[0])
    )
    brands_before = df["brands"].nunique()
    df["brands"] = brand_key.map(canonical_brand)
    print(f"Brand names normalized: {brands_before} -> {df['brands'].nunique()}")

    # Convert timestamp to datetime
    df["last_modified_t"] = pd.to_numeric(
        df["last_modified_t"], errors="coerce"
    )

    df["last_modified_t"] = pd.to_datetime(
        df["last_modified_t"], unit="s", errors="coerce"
    )

    # Remove records with invalid dates
    invalid_dates = df["last_modified_t"].isna()
    log_removed(df[invalid_dates], "invalid date")
    df = df[~invalid_dates].copy()

    print(f"Records after cleaning: {len(df)}")

    return df


def transform_data(df):
    print("\nStarting data transformation...")

    # Calculate total macronutrients using NumPy
    df["total_macros"] = np.round(np.sum(
        df[
            [
                "proteins_100g",
                "fat_100g",
                "carbohydrates_100g"
            ]
        ].values,
        axis=1
    ), 2)

    # Apply multiple filtering conditions: keep only physically possible
    # products (sugar is part of carbohydrates; 100 g of food cannot hold
    # more than 100 g of macronutrients). 0.5 g tolerance for rounding.
    valid_mask = (
        (df["energy_kcal_100g"] >= 0) &
        (df["proteins_100g"] >= 0) &
        (df["sugars_100g"] <= df["carbohydrates_100g"] + 0.5) &
        (df["total_macros"] <= 100.5)
    )
    log_removed(
        df[df["sugars_100g"] > df["carbohydrates_100g"] + 0.5],
        "sugars greater than carbohydrates"
    )
    log_removed(
        df[(df["total_macros"] > 100.5) &
           (df["sugars_100g"] <= df["carbohydrates_100g"] + 0.5)],
        "protein + fat + carbs > 100 g"
    )
    filtered_df = df[valid_mask].copy()

    print(f"Records after filtering: {len(filtered_df)}")

    # Group products by brand
    brand_summary = filtered_df.groupby("brands").agg(
        product_count=("code", "count"),
        average_calories=("energy_kcal_100g", "mean"),
        average_protein=("proteins_100g", "mean"),
        average_sugar=("sugars_100g", "mean")
    ).reset_index().sort_values("product_count", ascending=False)

    brand_summary.to_csv(
        "data/brand_summary.csv",
        index=False
    )

    # Reshape data using melt()
    nutrition_long = filtered_df.melt(
        id_vars=["code", "product_name"],
        value_vars=NUMERIC_COLUMNS,
        var_name="nutrient",
        value_name="value"
    )

    nutrition_long.to_csv(
        "data/nutrition_long.csv",
        index=False
    )

    return filtered_df


def main():
    print("Food Nutrition Analyzer - Data Cleaning")

    try:
        df = load_data()
    except FileNotFoundError:
        print(f"Input file not found: {INPUT_FILE}. Run validation.py first.")
        raise SystemExit(1)

    loaded_count = len(df)
    df = clean_data(df)
    df = transform_data(df)

    df.to_csv(OUTPUT_FILE, index=False)

    removed_df = pd.DataFrame(
        removed_records, columns=["code", "product_name", "reason"]
    )
    removed_df.to_csv(REMOVED_FILE, index=False)

    print("\nCleaning Summary")
    print(f"Input records: {loaded_count}")
    print(f"Removed records: {len(removed_df)}")
    if not removed_df.empty:
        print(removed_df["reason"].value_counts().to_string())
    print(f"Final records: {len(df)}")
    print(f"Output saved to: {OUTPUT_FILE}")
    print(f"Removed records saved to: {REMOVED_FILE}")
    print("Brand summary saved to: data/brand_summary.csv")
    print("Nutrition data saved to: data/nutrition_long.csv")


if __name__ == "__main__":
    main()

