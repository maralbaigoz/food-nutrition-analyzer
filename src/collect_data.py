import requests
import json
import time
from pathlib import Path

API_URL = "https://world.openfoodfacts.net/api/v2/search"

HEADERS = {
    "User-Agent": "FoodNutritionAnalyzer/1.0 (student project)"
}

AUTH = ("off", "off")

FIELDS = (
    "code,product_name,brands,categories,"
    "nutriments,last_modified_t"
)

TOTAL_PAGES = 3
PAGE_SIZE = 100
def fetch_products(page):
    params = {
        "page": page,
        "page_size": PAGE_SIZE,
        "fields": FIELDS
    }

    try:
        response = requests.get(
            API_URL,
            params=params,
            headers=HEADERS,
            auth=AUTH,
            timeout=60
        )

        response.raise_for_status()
        data = response.json()

        return data.get("products", [])

    except requests.exceptions.Timeout:
        print(f"Page {page}: Request timed out")
    except requests.exceptions.RequestException as error:
        print(f"Page {page}: Request failed: {error}")
    except ValueError:
        print(f"Page {page}: Invalid JSON response")

    return []
def collect_all_products():
    all_products = []

    for page in range(1, TOTAL_PAGES + 1):
        print(f"Collecting page {page}/{TOTAL_PAGES}...")

        products = fetch_products(page)
        print(f"Received: {len(products)} products")

        all_products.extend(products)

        time.sleep(1)

    return all_products
def save_raw_data(products):
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "raw_products.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(products, file, ensure_ascii=False, indent=4)

    print(f"Raw data saved: {output_file}")
    print(f"Total records saved: {len(products)}")



def main():
    print("Starting Food Nutrition Analyzer data collection...")

    products = collect_all_products()

    if not products:
        print("No products were collected.")
        raise SystemExit(1)

    save_raw_data(products)

    print("Data collection completed successfully!")


if __name__ == "__main__":
    main()
