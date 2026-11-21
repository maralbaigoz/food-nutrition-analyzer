# Data Dictionary — Food Nutrition Analyzer

Source: Open Food Facts API v2 (`https://world.openfoodfacts.net/api/v2/search`).
All nutrient values are given **per 100 g (or 100 ml)** of product.

## 1. Raw API record (`data/raw_products.json`, Kafka messages, `data/consumed_products.json`)

| Field | Type | Description |
|---|---|---|
| `code` | string | Product barcode (EAN-8 / EAN-13) |
| `product_name` | string | Product name as entered by contributors (any language) |
| `brands` | string | Brand name(s), comma-separated |
| `categories` | string | Open Food Facts categories, comma-separated |
| `last_modified_t` | integer | Last modification time, Unix timestamp (seconds) |
| `nutriments` | object | Nested nutrient values; the pipeline uses `energy-kcal_100g`, `sugars_100g`, `proteins_100g`, `fat_100g`, `carbohydrates_100g`, `salt_100g` |
| `nutriments_estimated`, `nutrition_data`, `nutrition_data_per`, `nutrition_data_prepared_per` | various | Extra API fields, not used |

## 2. Flattened / validated record (`valid_products.csv`, `invalid_products.csv`)

| Column | Type | Kind | Validation rule (Pandera) |
|---|---|---|---|
| `code` | str | identifier | not null, unique |
| `product_name` | str | categorical (text) | not null |
| `brands` | str | categorical | nullable |
| `categories` | str | categorical | nullable |
| `energy_kcal_100g` | float | numerical | 0 – 1000 |
| `sugars_100g` | float | numerical | 0 – 100 |
| `proteins_100g` | float | numerical | 0 – 100 |
| `fat_100g` | float | numerical | 0 – 100 |
| `carbohydrates_100g` | float | numerical | 0 – 100 |
| `salt_100g` | float | numerical | 0 – 100 |
| `last_modified_t` | int | date/time (Unix) | not null |

## 3. Cleaned record / SQLite3 table `products` (`cleaned_products.csv`, `food_nutrition.db`)

| Column | SQLite type | Description |
|---|---|---|
| `code` | TEXT **PRIMARY KEY** | Barcode, prevents duplicate rows (`INSERT OR IGNORE`) |
| `product_name` | TEXT NOT NULL | Name; missing names replaced by `Unknown product` |
| `brands` | TEXT | Brand; missing replaced by `Unknown` |
| `categories` | TEXT | Categories; missing replaced by `Unknown` |
| `energy_kcal_100g` | REAL | Energy, kcal |
| `sugars_100g` | REAL | Sugars, g |
| `proteins_100g` | REAL | Protein, g |
| `fat_100g` | REAL | Fat, g |
| `carbohydrates_100g` | REAL | Carbohydrates, g |
| `salt_100g` | REAL | Salt, g |
| `last_modified_t` | TEXT | Last modification, `YYYY-MM-DD HH:MM:SS` |
| `total_macros` | REAL | protein + fat + carbohydrates (NumPy), g |

Cleaning rules: products without an energy value are removed; missing nutrients of 0 kcal
products are set to 0; other missing nutrients are filled with the median (sugar ≤ carbohydrates
kept); products with sugars > carbohydrates or protein + fat + carbs > 100 g are removed;
brand spellings are unified.

## 4. Other outputs

| File | Description |
|---|---|
| `data/brand_summary.csv` | GroupBy by brand: product count, average calories / protein / sugar |
| `data/removed_products.csv` | Records removed during cleaning: `code`, `product_name`, `reason` |
| `data/kafka_stats.json` | Producer (published / failed) and consumer (consumed / skipped) counts |
| `data/nutrition_long.csv` | `melt()` result: one row per product × nutrient (`code`, `product_name`, `nutrient`, `value`) |

## 5. Columns derived during EDA (`src/analysis.py`, not stored in the DB)

| Column | Description |
|---|---|
| `food_group` | Food group assigned by keyword rules on `categories` (12 groups) |
| `sugar_level` | Low (≤ 5 g), Medium (5–22.5 g), High (> 22.5 g) — UK FSA front-of-pack thresholds |
| `calculated_kcal` | 4·protein + 4·carbohydrates + 9·fat |
| `protein_energy_share` | % of calculated energy that comes from protein |
| `nutrients_imputed` | True if at least one nutrient was missing before cleaning (median-filled) |
