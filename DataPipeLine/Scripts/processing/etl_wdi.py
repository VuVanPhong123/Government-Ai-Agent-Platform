import os
import logging
from pyspark.sql.functions import col, lit, expr

logger = logging.getLogger(__name__)

def process_wdi(spark, selected_countries, dim_indicator=None):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/worldBank/"
    file_path = os.path.join(data_dir, "WDICSV.csv")
    if not os.path.exists(file_path):
        logger.error(f"WDI file not found: {file_path}")
        return None

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    df = df.filter(col("Country Code").isin(list(selected_countries.keys())))

    year_cols = [str(y) for y in range(1990, 2025)]
    existing = [c for c in year_cols if c in df.columns]
    stack_expr = f"stack({len(existing)}, " + ", ".join([f"'{y}', `{y}`" for y in existing]) + ") as (year, value)"

    df_long = df.select(
        col("Country Code").alias("country_code"),
        col("Indicator Code").alias("indicator_code"),
        expr(stack_expr)
    ).filter(col("value").isNotNull())

    result = df_long.select(
        "country_code",
        col("year").cast("int"),
        "indicator_code",
        col("value").cast("double"),
        lit(2).alias("source_priority"),
        col("indicator_code").alias("source_specific")
    )
    logger.info(f"WDI processed: {result.count():,} rows")
    return result
def get_wdi_metadata(spark):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/worldBank/"
    country_path = os.path.join(data_dir, "WDICountry.csv")
    series_path = os.path.join(data_dir, "WDISeries.csv")
    
    df_country = None
    df_series = None
    
    if os.path.exists(country_path):
        df_country = spark.read.option("header", "true").option("inferSchema", "true").csv(country_path)
        
    if os.path.exists(series_path):
        df_series = spark.read.option("header", "true").option("inferSchema", "true").csv(series_path)
        
    return df_country, df_series