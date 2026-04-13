import os
import logging
from pyspark.sql.functions import col, lit

logger = logging.getLogger(__name__)

def process_gmd(spark, selected_countries, dim_indicator=None):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/gmd/"
    file_path = os.path.join(data_dir, "GMD.csv")
    if not os.path.exists(file_path):
        logger.error(f"GMD file not found: {file_path}")
        return None
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    df = df.filter(col("ISO3").isin(list(selected_countries.keys())))
    df = df.filter((col("year") >= 1995) & (col("year") <= 2024))

    metadata_cols = ["countryname", "ISO3", "id", "year", "income_group"]
    indicator_cols = [c for c in df.columns if c not in metadata_cols]

    fact_dfs = []
    for col_name in indicator_cols:
        temp_df = df.select(
            col("ISO3").alias("country_code"),
            col("year"),
            lit(col_name).alias("indicator_code"),
            col(col_name).cast("double").alias("value"),
            lit(1).alias("source_priority"),
            lit(f"GMD_{col_name}").alias("source_specific")
        ).filter(col("value").isNotNull())
        fact_dfs.append(temp_df)

    if not fact_dfs:
        return None

    result = fact_dfs[0]
    for part_df in fact_dfs[1:]:
        result = result.union(part_df)

    logger.info(f"GMD processed: {result.count():,} rows")
    return result