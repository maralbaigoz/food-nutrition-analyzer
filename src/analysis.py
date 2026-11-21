"""
Food Nutrition Analyzer - Part 6: Exploratory Data Analysis and Visualization

Reads the processed products from the SQLite3 database (the final output of
the pipeline), examines the dataset, calculates statistics, performs grouped
analysis and saves the figures to figures/ and a text summary to reports/.

Research questions:
    RQ1. How are calories distributed across the products?
    RQ2. Which food groups are the most energy-dense?
    RQ3. How does the macronutrient composition (protein / fat / carbs)
         differ between food groups?
    RQ4. Which products are high in sugar and which provide protein
         without much sugar?
    RQ5. How are the nutrients related to each other (correlation)?
    RQ6. Are the declared calories consistent with the macronutrients?

Run from the project root:
    python src/analysis.py
"""

import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")  # save figures without opening windows
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

DATABASE_FILE = Path("data/food_nutrition.db")
VALID_FILE = Path("data/valid_products.csv")
FIGURES_DIR = Path("figures")
REPORT_FILE = Path("reports/eda_summary.txt")

NUTRIENT_COLUMNS = [
    "energy_kcal_100g",
    "sugars_100g",
    "proteins_100g",
    "fat_100g",
    "carbohydrates_100g",
    "salt_100g",
]

NUTRIENT_LABELS = {
    "energy_kcal_100g": "Energy (kcal)",
    "sugars_100g": "Sugars (g)",
    "proteins_100g": "Protein (g)",
    "fat_100g": "Fat (g)",
    "carbohydrates_100g": "Carbohydrates (g)",
    "salt_100g": "Salt (g)",
}

# Rule-based food groups built from the Open Food Facts "categories" text.
# Rules are checked in order; the first match wins.
FOOD_GROUP_RULES = [
    ("Nuts & nut butters", r"peanut|nut butter|walnut|cashew nuts|brazil nut|mixed nuts|sunflower seed|^almonds"),
    ("Sweet spreads & jams", r"hazelnut|chocolate spread|tartiner|jam|honey|syrup|speculoos"),
    ("Biscuits & cereal bars", r"biscuit|cookie|wafer|digestive|shortbread|petit-beurre|cereal bar|protein bar"),
    ("Bread & cereals", r"bread|brioche|rusk|bagel|cereal|muesli|oat(?!-based)|porridge|flakes|crispbread|rice cakes|corn cakes"),
    ("Fats & oils", r"margarine|butter|vegetable oil|oils|vegetable fats|huile"),
    ("Savoury snacks", r"crisp|chips|cracker|apéritif|snacks salés"),
    ("Beverages", r"water|\bcolas?\b|soda|juice|nectar|coffee|beverage|(?<!yogurt )(?<!dairy )drink|green tea|mandelmilch"),
    ("Chocolate & sweets", r"chocolat|chocolade|candy|confection|cocoa|cacao|^sugars|vanillin sugar"),
    ("Dairy", r"yogh?urt|cheese|cheddar|fromage|milk|kefir|skyr|lben|dairy"),
    ("Fish, meat & protein foods", r"mackerel|sardine|tofu|chicken|meat|beans"),
    ("Sauces & condiments", r"sauce|ketchup|mayonnaise|mustard|pesto|vinegar|table salt|tomato|bouillon|stock|broth|soja|yeast|flavors"),
]

# Colors (validated reference palette from the dataviz guidelines)
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
BLUE = "#2a78d6"     # categorical slot 1
ORANGE = "#eb6834"   # categorical slot 2
AQUA = "#1baf7a"     # categorical slot 3
RED = "#e34948"
NEUTRAL_MID = "#f0efec"
BLUE_LIGHT = "#cde2fb"
BLUE_MID = "#6da7ec"
BLUE_DARK = "#184f95"


def setup_style():
    plt.rcParams.update({
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "axes.edgecolor": BASELINE,
        "axes.labelcolor": TEXT_SECONDARY,
        "axes.titlecolor": TEXT_PRIMARY,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "axes.labelsize": 10.5,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "xtick.color": TEXT_SECONDARY,
        "ytick.color": TEXT_SECONDARY,
        "legend.frameon": False,
        "legend.labelcolor": TEXT_SECONDARY,
        "font.size": 10,
    })


# ----------------------------------------------------------------------
# Loading
# ----------------------------------------------------------------------
def load_from_database():
    if not DATABASE_FILE.exists():
        raise FileNotFoundError(
            f"Database not found: {DATABASE_FILE}. Run src/database.py first."
        )

    with sqlite3.connect(DATABASE_FILE) as connection:
        df = pd.read_sql_query("SELECT * FROM products", connection)

    df["last_modified_t"] = pd.to_datetime(df["last_modified_t"], errors="coerce")
    return df


def flag_imputed_rows(df):
    """
    The cleaning stage fills a few missing nutrients with the column
    median. For the nutrient analysis we use only really reported values,
    so we compare with the validated (pre-cleaning) data. Products with
    0 kcal whose missing values were set to 0 count as reported.
    """

    if not VALID_FILE.exists():
        print(f"Warning: {VALID_FILE} not found - imputed values cannot be flagged.")
        df["nutrients_imputed"] = False
        return df

    valid = pd.read_csv(VALID_FILE, dtype={"code": str})
    valid["nutrients_imputed"] = (
        valid[NUTRIENT_COLUMNS].isna().any(axis=1)
        & (valid["energy_kcal_100g"] != 0)
    )

    df = df.merge(
        valid[["code", "nutrients_imputed"]].drop_duplicates("code"),
        on="code",
        how="left",
    )
    df["nutrients_imputed"] = df["nutrients_imputed"].fillna(False).astype(bool)
    return df


def assign_food_group(categories):
    text = str(categories).lower().strip()
    for group_name, pattern in FOOD_GROUP_RULES:
        if re.search(pattern, text):
            return group_name
    return "Other"


def add_derived_columns(df):
    df["food_group"] = df["categories"].apply(assign_food_group)

    # Sugar level using the UK Food Standards Agency front-of-pack thresholds
    # (per 100 g): low <= 5 g, medium 5-22.5 g, high > 22.5 g
    df["sugar_level"] = pd.cut(
        df["sugars_100g"],
        bins=[-np.inf, 5, 22.5, np.inf],
        labels=["Low (≤5 g)", "Medium (5–22.5 g)", "High (>22.5 g)"],
    )

    # Share of energy coming from each macronutrient (NumPy vectorised)
    protein_kcal = df["proteins_100g"].to_numpy() * 4
    carbs_kcal = df["carbohydrates_100g"].to_numpy() * 4
    fat_kcal = df["fat_100g"].to_numpy() * 9
    calculated_kcal = protein_kcal + carbs_kcal + fat_kcal

    df["calculated_kcal"] = calculated_kcal
    with np.errstate(divide="ignore", invalid="ignore"):
        df["protein_energy_share"] = np.where(
            calculated_kcal > 0, protein_kcal / calculated_kcal * 100, np.nan
        )
    df["modified_month"] = df["last_modified_t"].dt.to_period("M").astype(str)
    return df


# ----------------------------------------------------------------------
# Statistics
# ----------------------------------------------------------------------
def describe_dataset(df, analysis_df, lines):
    lines.append("=" * 70)
    lines.append("FOOD NUTRITION ANALYZER - EXPLORATORY DATA ANALYSIS")
    lines.append("=" * 70)

    lines.append("\n1. DATASET DIMENSIONS")
    lines.append(f"Rows in database: {df.shape[0]}, columns: {df.shape[1] - 1}")
    lines.append(
        f"Rows with some nutrient values filled by the median during cleaning: "
        f"{int(df['nutrients_imputed'].sum())}"
    )
    lines.append(
        f"Rows used for nutrient analysis (real reported values only): "
        f"{len(analysis_df)}"
    )

    lines.append("\n2. DATA TYPES")
    for column, dtype in df.dtypes.items():
        lines.append(f"  {column:<24} {dtype}")

    lines.append("\n3. DESCRIPTIVE STATISTICS (per 100 g, real values only)")
    stats = analysis_df[NUTRIENT_COLUMNS].describe().T
    stats["median"] = analysis_df[NUTRIENT_COLUMNS].median()
    stats["skewness"] = analysis_df[NUTRIENT_COLUMNS].skew()
    lines.append(stats.round(2).to_string())

    lines.append("\n4. CATEGORICAL STATISTICS")
    lines.append(f"Unique brands: {df['brands'].nunique()}")
    lines.append(
        "Unique brands after lower-casing: "
        f"{df['brands'].str.lower().str.strip().nunique()}"
    )
    lines.append("\nTop 10 brands by number of products:")
    lines.append(df["brands"].value_counts().head(10).to_string())
    lines.append("\nProducts per food group:")
    lines.append(df["food_group"].value_counts().to_string())
    lines.append("\nSugar level (real values only):")
    lines.append(
        analysis_df["sugar_level"].value_counts().sort_index().to_string()
    )

    lines.append("\n5. DATE/TIME STATISTICS (last_modified_t)")
    lines.append(f"Earliest update: {df['last_modified_t'].min()}")
    lines.append(f"Latest update:   {df['last_modified_t'].max()}")
    by_year = df["last_modified_t"].dt.year.value_counts().sort_index()
    lines.append("Products by year of last update:")
    lines.append(by_year.to_string())


def grouped_analysis(analysis_df, lines):
    lines.append("\n6. GROUPED ANALYSIS BY FOOD GROUP (real values only)")
    group_stats = (
        analysis_df.groupby("food_group")
        .agg(
            products=("code", "count"),
            mean_kcal=("energy_kcal_100g", "mean"),
            median_kcal=("energy_kcal_100g", "median"),
            mean_sugar=("sugars_100g", "mean"),
            mean_protein=("proteins_100g", "mean"),
            mean_fat=("fat_100g", "mean"),
            mean_salt=("salt_100g", "mean"),
        )
        .sort_values("median_kcal", ascending=False)
        .round(2)
    )
    lines.append(group_stats.to_string())

    lines.append("\n7. PIVOT TABLE - number of products by food group and sugar level")
    pivot = pd.pivot_table(
        analysis_df,
        index="food_group",
        columns="sugar_level",
        values="code",
        aggfunc="count",
        fill_value=0,
        observed=False,
    )
    lines.append(pivot.to_string())

    lines.append("\n8. TOP 10 PRODUCTS")
    top_columns = ["product_name", "brands", "energy_kcal_100g",
                   "sugars_100g", "proteins_100g"]
    lines.append("\nHighest sugar:")
    lines.append(
        analysis_df.nlargest(10, "sugars_100g")[top_columns].to_string(index=False)
    )
    lines.append("\nHighest protein:")
    lines.append(
        analysis_df.nlargest(10, "proteins_100g")[top_columns].to_string(index=False)
    )

    lines.append("\n9. CORRELATION MATRIX (Pearson)")
    lines.append(analysis_df[NUTRIENT_COLUMNS].corr().round(2).to_string())
    return group_stats


# ----------------------------------------------------------------------
# Figures
# ----------------------------------------------------------------------
def save_figure(fig, file_name):
    path = FIGURES_DIR / file_name
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved figure: {path}")


def plot_energy_distribution(analysis_df):
    """RQ1: distribution of calories."""
    energy = analysis_df["energy_kcal_100g"]
    fig, ax = plt.subplots(figsize=(9, 5))

    ax.hist(energy, bins=np.arange(0, 950, 50), color=BLUE,
            edgecolor=SURFACE, linewidth=2)
    ax.axvline(energy.median(), color=TEXT_PRIMARY, linewidth=1.5,
               linestyle="--", label=f"Median = {energy.median():.0f} kcal")
    ax.axvline(energy.mean(), color=ORANGE, linewidth=2,
               label=f"Mean = {energy.mean():.0f} kcal")

    ax.set_title("RQ1. Distribution of energy per 100 g")
    ax.set_xlabel("Energy (kcal per 100 g)")
    ax.set_ylabel("Number of products")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper right")
    save_figure(fig, "fig1_energy_distribution.png")


def plot_energy_by_group(analysis_df):
    """RQ2: energy density by food group (box plot)."""
    order = (
        analysis_df.groupby("food_group")["energy_kcal_100g"]
        .median()
        .sort_values()
        .index.tolist()
    )
    data = [analysis_df.loc[analysis_df["food_group"] == g,
                            "energy_kcal_100g"] for g in order]
    counts = [len(values) for values in data]

    fig, ax = plt.subplots(figsize=(10, 6.5))
    # Matplotlib >= 3.10 uses "orientation", older versions use "vert"
    major, minor = (int(x) for x in matplotlib.__version__.split(".")[:2])
    direction = ({"orientation": "horizontal"} if (major, minor) >= (3, 10)
                 else {"vert": False})
    box = ax.boxplot(
        data,
        **direction,
        patch_artist=True,
        widths=0.55,
        medianprops={"color": TEXT_PRIMARY, "linewidth": 2},
        whiskerprops={"color": MUTED},
        capprops={"color": MUTED},
        flierprops={"marker": "o", "markersize": 5,
                    "markerfacecolor": BLUE_MID, "markeredgecolor": SURFACE},
    )
    for patch in box["boxes"]:
        patch.set_facecolor(BLUE_LIGHT)
        patch.set_edgecolor(BLUE)

    ax.set_yticks(range(1, len(order) + 1))
    ax.set_yticklabels([f"{g} (n={n})" for g, n in zip(order, counts)])
    ax.set_title("RQ2. Energy density by food group (sorted by median)")
    ax.set_xlabel("Energy (kcal per 100 g)")
    ax.grid(axis="y", visible=False)
    legend_items = [
        Patch(facecolor=BLUE_LIGHT, edgecolor=BLUE, label="Middle 50% of products"),
        Line2D([0], [0], color=TEXT_PRIMARY, linewidth=2, label="Median"),
    ]
    ax.legend(handles=legend_items, loc="lower right")
    save_figure(fig, "fig2_energy_by_food_group.png")


def plot_macro_composition(macro_stats):
    """RQ3: average protein / fat / carbs per food group (stacked bars)."""
    stats = macro_stats.sort_values("total", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6.5))
    left = np.zeros(len(stats))
    series = [
        ("mean_protein", "Protein", BLUE),
        ("mean_fat", "Fat", ORANGE),
        ("mean_carbs", "Carbohydrates", AQUA),
    ]
    for column, label, color in series:
        ax.barh(stats.index, stats[column], left=left, color=color,
                edgecolor=SURFACE, linewidth=2, height=0.62, label=label)
        left += stats[column].to_numpy()

    for y, total in enumerate(stats["total"]):
        ax.text(total + 1, y, f"{total:.0f} g", va="center",
                color=TEXT_SECONDARY, fontsize=9)

    ax.set_title("RQ3. Average macronutrient composition by food group")
    ax.set_xlabel("Average grams per 100 g of product")
    ax.set_xlim(0, 105)
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower right", ncol=3)
    save_figure(fig, "fig3_macro_composition_by_group.png")


def plot_sugar_vs_protein(analysis_df):
    """RQ4: sugar vs protein with sugar thresholds."""
    fig, ax = plt.subplots(figsize=(9.5, 6.5))

    ax.axvspan(22.5, 105, color=NEUTRAL_MID, zorder=0)
    ax.axvline(5, color=MUTED, linewidth=1, linestyle=":")
    ax.axvline(22.5, color=MUTED, linewidth=1, linestyle=":")
    ax.text(23.5, 35.5, "High sugar (>22.5 g / 100 g)", color=TEXT_SECONDARY,
            fontsize=9, va="top")
    ax.text(5.8, 35.5, "Medium", color=TEXT_SECONDARY, fontsize=9, va="top")
    ax.text(0.5, 35.5, "Low", color=TEXT_SECONDARY, fontsize=9, va="top")

    ax.scatter(analysis_df["sugars_100g"], analysis_df["proteins_100g"],
               s=42, color=BLUE, alpha=0.75, edgecolor=SURFACE,
               linewidth=1.5, zorder=3)

    # Label a few products that answer the question directly
    # (skip unnamed products and names the default font cannot render)
    def is_labelable(name):
        name = str(name)
        return name != "Unknown product" and name.isascii()

    labelable = analysis_df[analysis_df["product_name"].apply(is_labelable)]
    high_protein = labelable.nlargest(3, "proteins_100g")
    high_sugar = labelable.nlargest(2, "sugars_100g")
    for _, row in high_protein.iterrows():
        ax.annotate(str(row["product_name"])[:28],
                    (row["sugars_100g"], row["proteins_100g"]),
                    xytext=(8, 0), textcoords="offset points", va="center",
                    fontsize=8.5, color=TEXT_PRIMARY)
    for _, row in high_sugar.iterrows():
        ax.annotate(str(row["product_name"])[:28],
                    (row["sugars_100g"], row["proteins_100g"]),
                    xytext=(0, 10), textcoords="offset points", ha="right",
                    fontsize=8.5, color=TEXT_PRIMARY)

    ax.set_xlim(-2, 105)
    ax.set_ylim(-1, 37)
    ax.set_title("RQ4. Sugar vs protein per 100 g")
    ax.set_xlabel("Sugars (g per 100 g)")
    ax.set_ylabel("Protein (g per 100 g)")
    save_figure(fig, "fig4_sugar_vs_protein.png")


def plot_correlation_heatmap(analysis_df):
    """RQ5: correlation between nutrients (diverging colormap)."""
    corr = analysis_df[NUTRIENT_COLUMNS].corr()
    labels = [NUTRIENT_LABELS[c] for c in NUTRIENT_COLUMNS]
    cmap = LinearSegmentedColormap.from_list(
        "blue_gray_red", [BLUE_DARK, BLUE, NEUTRAL_MID, RED, "#a32f2f"]
    )

    fig, ax = plt.subplots(figsize=(8, 6.5))
    image = ax.imshow(corr.to_numpy(), cmap=cmap, vmin=-1, vmax=1)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=35, ha="right")
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_visible(False)

    for i in range(len(labels)):
        for j in range(len(labels)):
            value = corr.iloc[i, j]
            text_color = SURFACE if abs(value) > 0.6 else TEXT_PRIMARY
            ax.text(j, i, f"{value:.2f}", ha="center", va="center",
                    color=text_color, fontsize=9)

    colorbar = fig.colorbar(image, ax=ax, shrink=0.8)
    colorbar.set_label("Pearson correlation coefficient")
    colorbar.outline.set_visible(False)
    ax.set_title("RQ5. Correlation between nutrients")
    save_figure(fig, "fig5_nutrient_correlation.png")


def plot_energy_consistency(analysis_df):
    """RQ6: declared kcal vs kcal calculated from macronutrients."""
    declared = analysis_df["energy_kcal_100g"].to_numpy()
    calculated = analysis_df["calculated_kcal"].to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        relative_diff = np.abs(declared - calculated) / np.maximum(declared, 1)
    inconsistent = (relative_diff > 0.25) & (np.abs(declared - calculated) > 30)

    fig, ax = plt.subplots(figsize=(8, 7))
    limit = 1000
    ax.plot([0, limit], [0, limit], color=MUTED, linewidth=1.2,
            linestyle="--", label="Declared = calculated")
    ax.scatter(calculated[~inconsistent], declared[~inconsistent], s=40,
               color=BLUE, edgecolor=SURFACE, linewidth=1.5, alpha=0.8,
               label=f"Consistent (n={int((~inconsistent).sum())})", zorder=3)
    ax.scatter(calculated[inconsistent], declared[inconsistent], s=60,
               color=ORANGE, marker="D", edgecolor=SURFACE, linewidth=1.5,
               label=f"Differs by >25% (n={int(inconsistent.sum())})", zorder=4)

    ax.set_xlim(0, limit)
    ax.set_ylim(0, limit)
    ax.set_title("RQ6. Declared vs calculated energy\n"
                 "(calculated = 4·protein + 4·carbs + 9·fat)")
    ax.set_xlabel("Calculated energy (kcal per 100 g)")
    ax.set_ylabel("Declared energy (kcal per 100 g)")
    ax.legend(loc="upper left")
    save_figure(fig, "fig6_energy_consistency.png")
    return int(inconsistent.sum())


# ----------------------------------------------------------------------
def main():
    print("Food Nutrition Analyzer - Exploratory Data Analysis")
    FIGURES_DIR.mkdir(exist_ok=True)
    REPORT_FILE.parent.mkdir(exist_ok=True)
    setup_style()

    try:
        df = load_from_database()
    except (FileNotFoundError, sqlite3.Error) as error:
        print(f"Could not load data: {error}")
        return

    df = flag_imputed_rows(df)
    df = add_derived_columns(df)

    # Median-filled values are estimates, so nutrient statistics use only
    # products whose values were really reported.
    analysis_df = df[~df["nutrients_imputed"]].copy()

    lines = []
    describe_dataset(df, analysis_df, lines)
    group_stats = grouped_analysis(analysis_df, lines)

    macro_stats = analysis_df.groupby("food_group").agg(
        mean_protein=("proteins_100g", "mean"),
        mean_fat=("fat_100g", "mean"),
        mean_carbs=("carbohydrates_100g", "mean"),
    )
    macro_stats["total"] = macro_stats.sum(axis=1)

    plot_energy_distribution(analysis_df)
    plot_energy_by_group(analysis_df)
    plot_macro_composition(macro_stats)
    plot_sugar_vs_protein(analysis_df)
    plot_correlation_heatmap(analysis_df)
    inconsistent_count = plot_energy_consistency(analysis_df)

    lines.append("\n10. ENERGY CONSISTENCY CHECK")
    lines.append(
        f"Products whose declared kcal differs from 4P+4C+9F by more than "
        f"25% (and >30 kcal): {inconsistent_count} of {len(analysis_df)}"
    )

    REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nEDA summary saved to: {REPORT_FILE}")
    print(f"Figures saved to: {FIGURES_DIR}/")


if __name__ == "__main__":
    main()
