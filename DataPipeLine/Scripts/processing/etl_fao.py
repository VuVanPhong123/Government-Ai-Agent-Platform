import os
import glob
import logging
from pyspark.sql.functions import col, lit, create_map

logger = logging.getLogger(__name__)

def process_fao(spark, selected_countries, dim_indicator=None):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/fao/"
    all_csv = glob.glob(os.path.join(data_dir, "**", "*.csv"), recursive=True)
    data_files = [f for f in all_csv if "All_Data_(Normalized)" in f and not any(x in f for x in ["AreaCodes", "Elements", "Flags", "ItemCodes", "Currencys"])]
    logger.info(f"Found {len(data_files)} FAO data files")
    if not data_files:
        return None

    name_to_iso = {v: k for k, v in selected_countries.items()}
    name_to_iso.update({
        "United States of America": "USA", "United States": "USA",
        "Russia": "RUS", "Russian Federation": "RUS", "Viet Nam": "VNM",
        "Korea, Republic of": "KOR", "Iran (Islamic Republic of)": "IRN",
        "Venezuela (Bolivarian Republic of)": "VEN", "Bolivia (Plurinational State of)": "BOL",
        "Tanzania, United Republic of": "TZA", "Congo, Democratic Republic of the": "COD",
        "Congo": "COG", "Côte d'Ivoire": "CIV", "Egypt": "EGY",
        "United Kingdom": "GBR", "United Kingdom of Great Britain and Northern Ireland": "GBR",
        "China": "CHN", "India": "IND", "Brazil": "BRA", "Mexico": "MEX",
        "Germany": "DEU", "France": "FRA", "Japan": "JPN", "Australia": "AUS",
        "Canada": "CAN", "South Africa": "ZAF", "Turkey": "TUR",
    })

    all_facts = []
    for file_path in data_files:
        file_name = os.path.basename(file_path)
        logger.info(f"Processing FAO file: {file_name}")
        try:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            continue

        if not all(c in df.columns for c in ["Area", "Element", "Year", "Value"]):
            continue

        df = df.filter((col("Year") >= 1995) & (col("Year") <= 2024))
        keep_names = list(name_to_iso.keys())
        df = df.filter(col("Area").isin(keep_names))
        if df.count() == 0:
            continue

        mapping_expr = create_map([lit(x) for x in sum(name_to_iso.items(), ())])
        df = df.withColumn("country_code", mapping_expr.getItem(col("Area"))) \
               .filter(col("country_code").isNotNull())

        elements = df.select("Element").distinct().collect()
        for row in elements:
            element = row["Element"]
            element_df = df.filter(col("Element") == element)
            temp_df = element_df.select(
                col("country_code"),
                col("Year").alias("year"),
                lit(element).alias("indicator_code"),
                col("Value").cast("double").alias("value"),
                lit(4).alias("source_priority"),
                lit(f"FAO_{file_name}_{element}").alias("source_specific")
            ).filter(col("value").isNotNull())
            if temp_df.count() > 0:
                all_facts.append(temp_df)

    if not all_facts:
        return None
    result = all_facts[0]
    for df in all_facts[1:]:
        result = result.union(df)
    logger.info(f"FAO processed: {result.count():,} rows")
    return result