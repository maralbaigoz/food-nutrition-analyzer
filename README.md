# Food Nutrition Analyzer

SIS 2 — *Data Streaming and Validation Pipeline with Apache Kafka and Pandera*
(Data Collection & Preparation).

The project compares packaged food products by calories, sugar, protein, fat and other
nutritional values. Data is collected from the **Open Food Facts API**, streamed through
**Apache Kafka**, validated with **Pandera**, cleaned with **pandas / NumPy**, stored in
**SQLite3** and analysed with **Matplotlib**.

```
Open Food Facts API
        │  collect_data.py
        ▼
data/raw_products.json
        │  producer.py  (JSON serialization, delivery callbacks)
        ▼
Kafka topic "food-nutrition-data"
        │  consumer.py  (JSON deserialization, stops after 10 s without messages)
        ▼
data/consumed_products.json
        │  validation.py (Pandera schema)
        ▼
data/valid_products.csv  +  data/invalid_products.csv
        │  cleaning.py (missing values, duplicates, types, NumPy, filter, groupby, melt)
        ▼
data/cleaned_products.csv (+ removed_products.csv, brand_summary.csv, nutrition_long.csv)
        │  database.py (INSERT OR IGNORE, PRIMARY KEY code)
        ▼
data/food_nutrition.db
        │  analysis.py                │  verify_pipeline.py
        ▼                             ▼
figures/*.png, reports/eda_summary.txt   reports/pipeline_verification.txt/.json
```

## Project structure

```
food-nutrition-analyzer/
├── README.md
├── requirements.txt
├── docker-compose.yml        # Kafka broker
├── .gitignore
├── main.py                   # runs the whole pipeline
├── src/
│   ├── collect_data.py       # 1. API collection
│   ├── producer.py           # 2. Kafka Producer
│   ├── consumer.py           # 2. Kafka Consumer
│   ├── validation.py         # 3. Pandera validation
│   ├── cleaning.py           # 4. Cleaning & transformation
│   ├── database.py           # 5. SQLite3 storage
│   ├── analysis.py           # 6. EDA & visualization
│   └── verify_pipeline.py    # 7. Pipeline verification & data quality
├── data/                     # generated data files and database
├── figures/                  # generated charts
├── reports/                  # generated text/JSON reports
└── docs/
    ├── data_dictionary.md
    ├── eda_findings.md
    └── project_report.pdf    # project report
```

## Requirements

- Python 3.10 or newer
- Docker Desktop (to run Kafka) — or a local Apache Kafka 3.x installation
- Internet access for the Open Food Facts API

## 1. Installation

```bash
git clone https://github.com/maralbaigoz/food-nutrition-analyzer.git
cd food-nutrition-analyzer
python -m venv .venv
# Windows: .venv\Scripts\activate      macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Kafka setup

Start a single-node broker (KRaft mode) on `localhost:9092`:

```bash
docker compose up -d
docker ps                      # container "kafka" must be running
```

Create the topic (`main.py` also recreates it automatically before each full run):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic food-nutrition-data --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Check the topic and the number of messages:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe \
  --topic food-nutrition-data --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --topic food-nutrition-data --bootstrap-server localhost:9092
```

| Setting | Value |
|---|---|
| Broker | `localhost:9092` |
| Topic | `food-nutrition-data` (1 partition, replication factor 1) |
| Consumer group | `food-validation-group` |
| Serialization | JSON, UTF-8 |

## 3. API configuration

- Endpoint: `https://world.openfoodfacts.net/api/v2/search`
- Parameters: `page` (1–3), `page_size=100`,
  `fields=code,product_name,brands,categories,nutriments,last_modified_t`
- A `User-Agent` header is sent as requested by Open Food Facts.
  No private API key is needed.

## 4. Running the pipeline

**All commands are run from the project root folder.**

Whole pipeline in one command:

```bash
python main.py                  # all stages, including API collection
python main.py --skip-collect   # reuse data/raw_products.json
python main.py --from analysis  # only EDA + verification
```

Or stage by stage (execution order):

```bash
python src/collect_data.py      # -> data/raw_products.json (300 records)
python src/producer.py          # -> 300 messages to Kafka, data/kafka_stats.json
python src/consumer.py          # -> data/consumed_products.json
python src/validation.py        # -> data/valid_products.csv, data/invalid_products.csv
python src/cleaning.py          # -> data/cleaned_products.csv, removed_products.csv, brand_summary.csv, nutrition_long.csv
python src/database.py          # -> data/food_nutrition.db
python src/database.py          # run again: the row count must not change (idempotency)
python src/analysis.py          # -> figures/*.png, reports/eda_summary.txt
python src/verify_pipeline.py   # -> reports/pipeline_verification.txt / .json
```

> The consumer belongs to the group `food-validation-group` and commits its offsets.
> Running `consumer.py` a second time without new messages receives 0 messages; it then
> keeps the previous `consumed_products.json` and exits with an error message.
> To repeat the demonstration, recreate the topic (or use `python main.py`, which does it
> automatically) and run the producer again.

## 5. Database verification

```bash
sqlite3 data/food_nutrition.db
```
```sql
SELECT COUNT(*) FROM products;
SELECT code, COUNT(*) FROM products GROUP BY code HAVING COUNT(*) > 1;   -- must be empty
SELECT brands, COUNT(*) AS n FROM products GROUP BY brands ORDER BY n DESC LIMIT 5;
SELECT product_name, energy_kcal_100g FROM products ORDER BY energy_kcal_100g DESC LIMIT 5;
```

`src/verify_pipeline.py` runs these queries automatically, compares the database with
`cleaned_products.csv` and loads the data twice more to prove idempotency.

## 6. Expected outputs

| Stage | Expected result (current data) |
|---|---|
| Collection | 300 records in `data/raw_products.json` |
| Producer | `Successfully published to Kafka: 300`, `Failed messages: 0` |
| Consumer | `Total messages received: 300` |
| Validation | 292 valid, 8 invalid |
| Cleaning | 272 records (18 without energy value, 2 with sugars > carbohydrates removed) |
| Database | 272 records; a second run inserts 0 new records |
| Analysis | 6 figures in `figures/`, statistics in `reports/eda_summary.txt` |
| Verification | `SUMMARY: N of N checks passed` |

Counts can change slightly if the API returns different products on a new run.

## 7. How to check that the run was successful

1. `reports/pipeline_verification.txt` ends with *all checks passed*.
2. Counts in section 1 of that report follow 300 → 300 → 300 → 292 + 8 → 272 → 272.
3. `figures/` contains `fig1` … `fig6`.

## Figures

| File | Question |
|---|---|
| `fig1_energy_distribution.png` | How are calories distributed? |
| `fig2_energy_by_food_group.png` | Which food groups are the most energy-dense? |
| `fig3_macro_composition_by_group.png` | Protein / fat / carbs by food group |
| `fig4_sugar_vs_protein.png` | Which products are high in sugar or protein? |
| `fig5_nutrient_correlation.png` | How are nutrients related? |
| `fig6_energy_consistency.png` | Are declared calories consistent with macronutrients? |

Interpretation of every figure: `docs/eda_findings.md`.

## Submission (GitHub)

- Repository: `https://github.com/maralbaigoz/food-nutrition-analyzer.git` (public, or access granted to the instructor)
- Committed: source code (`src/`, `main.py`), `README.md`, `requirements.txt`, `docker-compose.yml`,
  `.gitignore`, `docs/`, `figures/`, `reports/` and the generated `data/` files
- Not committed: `.venv/`, `__pycache__/`, `.env` (see `.gitignore`)
- Author: Baigoz Maral
