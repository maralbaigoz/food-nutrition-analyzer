import json
import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pathlib import Path


DATA_FILE = Path("data/consumed_products.json")

def load_data():
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        products = json.load(file)

    records = []

    for product in products:
        nutrition = product.get("nutriments", {})

        records.append({
            "code": product.get("code"),
            "product_name": product.get("product_name"),
            "brands": product.get("brands"),
            "categories": product.get("categories"),
            "energy_kcal_100g": nutrition.get("energy-kcal_100g"),
            "sugars_100g": nutrition.get("sugars_100g"),
            "proteins_100g": nutrition.get("proteins_100g"),
            "fat_100g": nutrition.get("fat_100g"),
            "carbohydrates_100g": nutrition.get("carbohydrates_100g"),
            "salt_100g": nutrition.get("salt_100g"),
            "last_modified_t": product.get("last_modified_t")
        })

    return pd.DataFrame(records)
schema = pa.DataFrameSchema({
    "code": Column(str, nullable=False, unique=True),
    "product_name": Column(str, nullable=False),
    "brands": Column(str, nullable=True),
    "categories": Column(str, nullable=True),
    "energy_kcal_100g": Column(
        float, Check.in_range(0, 1000), nullable=True, coerce=True
    ),
    "sugars_100g": Column(
        float, Check.in_range(0, 100), nullable=True, coerce=True
    ),
    "proteins_100g": Column(
        float, Check.in_range(0, 100), nullable=True, coerce=True
    ),
    "fat_100g": Column(
        float, Check.in_range(0, 100), nullable=True, coerce=True
    ),
    "carbohydrates_100g": Column(
        float, Check.in_range(0, 100), nullable=True, coerce=True
    ),
    "salt_100g": Column(
        float, Check.in_range(0, 100), nullable=True, coerce=True
    ),
    "last_modified_t": Column(int, nullable=False, coerce=True)
}, strict=True)
def validate_data(df):
    try:
        validated_df = schema.validate(df, lazy=True)

        print("All records passed validation.")
        return validated_df, pd.DataFrame()

    except pa.errors.SchemaErrors as error:
        print("Validation errors detected!")

        failure_cases = error.failure_cases
        print("\nValidation error details:")
        print(failure_cases)

        invalid_indices = set(
            failure_cases["index"].dropna().tolist()
        )

        invalid_df = df.loc[
            df.index.isin(invalid_indices)
        ].copy()

        valid_df = df.drop(index=invalid_indices).copy()

        return valid_df, invalid_df
def main():
    print("Starting Food Nutrition Analyzer validation...")

    df = load_data()

    print(f"\nTotal records: {len(df)}")

    valid_df, invalid_df = validate_data(df)

    print("\nValidation Summary")
    print(f"Valid records: {len(valid_df)}")
    print(f"Invalid records: {len(invalid_df)}")

    Path("data").mkdir(exist_ok=True)

    valid_df.to_csv("data/valid_products.csv", index=False)
    invalid_df.to_csv("data/invalid_products.csv", index=False)

    print("\nValidation results saved successfully!")


if __name__ == "__main__":
    main()

