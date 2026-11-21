# EDA Findings and Pipeline Verification Results

Numbers below were produced by `python src/analysis.py` and
`python src/verify_pipeline.py` on the current data (see `reports/`).
If the pipeline is re-run with new API data, update the numbers from those reports.

## Dataset used for the analysis

- 272 products are stored in SQLite3 (12 columns).
- 7 of them had one or two missing nutrients that the cleaning stage filled with the
  median (sugar/carbohydrate fills are kept consistent: sugar ≤ carbohydrates).
  Nutrient statistics and figures use the **265 products with fully reported values**;
  brand and date statistics use all 272.
- Food groups were derived from the `categories` text with keyword rules (12 groups).
- Dates of last modification range from 2024-12-17 to 2026-11-19; 256 products were
  updated in 2026.

## Research questions and figures

### Figure 1 — Distribution of energy (RQ1)
The distribution is **bimodal**: a large peak at 0–50 kcal (waters, zero drinks, vinegar)
and a second cluster at 450–600 kcal (chocolate, nuts, biscuits, spreads). The mean
(297 kcal) is above the median (270 kcal) and neither describes a "typical" product,
because the dataset consists of two very different kinds of food.

### Figure 2 — Energy density by food group (RQ2)
Fats & oils (median 635 kcal), nuts & nut butters (621), chocolate & sweets (566) and
sweet spreads (538.5) are the most energy-dense groups. Beverages (median 0 kcal, most
are mineral waters) and dairy (114 kcal) are the least. Sauces & condiments vary the most,
from vinegar (~1 kcal) to mayonnaise (~700 kcal).

### Figure 3 — Macronutrient composition by group (RQ3)
Energy-dense groups get their energy from different sources: fats & oils are almost pure
fat, nuts combine fat with the highest protein (≈22.5 g), while biscuits, bread and sweet
spreads are dominated by carbohydrates. Dairy, beverages and protein foods have small totals
because most of their weight is water.

### Figure 4 — Sugar vs protein (RQ4)
52 of 265 products (20 %) are **high in sugar** (> 22.5 g/100 g): all sweet spreads,
most chocolate and biscuits; 153 (58 %) are low in sugar. High-sugar products contain
little protein (mostly < 11 g), whereas products with > 20 g protein (yeast extract,
mackerel, peanut butters, nuts) are low or medium in sugar. Practically no product is
both high in sugar and high in protein.

### Figure 5 — Correlation between nutrients (RQ5)
Energy is most strongly correlated with **fat (r = 0.83)**; carbohydrates (0.41),
protein (0.36) and sugar (0.33) matter much less. Sugar is correlated with carbohydrates
(0.60), as expected because sugar is part of carbohydrates. Salt is independent of the
other nutrients.

### Figure 6 — Declared vs calculated energy (RQ6, data quality)
For 251 of 265 products the declared energy matches 4·protein + 4·carbs + 9·fat.
14 products differ by more than 25 %, which indicates input errors in Open Food Facts
(e.g. energy entered for a portion, or a missing nutrient). They are kept and reported.

## Main conclusions

1. Fat content is the main driver of calories in packaged foods.
2. Sweet spreads, chocolate and biscuits are both high in energy and high in sugar;
   nuts and nut butters are equally energy-dense but provide protein instead of sugar.
3. Beverages and dairy are the lightest groups by energy per 100 g.
4. Open Food Facts is crowd-sourced: about 5 % of products have inconsistent energy values,
   18 products had no energy value at all, and 2 reported more sugar than carbohydrates,
   so validation and cleaning were necessary before the analysis.

## Pipeline verification summary

| Stage | Records | Explanation |
|---|---|---|
| Collected from API | 300 | 3 pages × 100 products |
| Published to Kafka | 300 | broker topic offsets / producer delivery callbacks (`data/kafka_stats.json`) |
| Consumed from Kafka | 300 | identical to the API records after the JSON round-trip |
| Valid (Pandera) | 292 | |
| Invalid (Pandera) | 8 | 5 missing `product_name`, 2 energy > 1000 kcal (kJ entered as kcal), 1 sugars/carbs > 100 g |
| After cleaning | 272 | 0 duplicates; 18 without energy value removed; 2 with sugars > carbohydrates removed (`data/removed_products.csv`) |
| Stored in SQLite3 | 272 | equals cleaned records; PK `code`, no duplicates, values identical to the CSV |
| Repeated load ×2 | 272 | 0 new rows; row count and SHA-256 hash of the table unchanged → idempotent |

All 20 automated checks passed (the topic-offset check additionally requires a running broker).

## Cleaning decisions (why)

| Problem found | Decision |
|---|---|
| 18 products without any energy value (mostly waters, coffee and cocoa powders) | Removed: energy is the main variable; median imputation gave water 263 kcal |
| Missing nutrients for 0 kcal products (water, salt) | Set to 0 — a product with 0 kcal cannot contain macronutrients |
| One or two missing nutrients in other products | Median of products with energy > 0; sugar fill ≤ carbohydrates, carbohydrate fill ≥ sugar |
| Sugars > carbohydrates (impossible) | Removed by the multi-condition filter |
| Barcodes lost leading zeros (06175700 → 6175700) | `code` read as text in cleaning and database stages |
| Brand spelled differently (Jaouda / jaouda, alpro / Alpro) | All spellings mapped to the most frequent one (178 → 167 brands) |
