import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType


os.environ["PYSPARK_PYTHON"] = sys.executable.replace("\\", "/")
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable.replace("\\", "/")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine"
RAW_DIR = f"{BASE_DIR}/data/raw"
PROCESSED_DIR = f"{BASE_DIR}/data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)

def create_spark_session():
    return SparkSession.builder \
        .appName("EconomicIndicatorsETL") \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

spark = create_spark_session()
logger.info("Spark session initialized")
SELECTED_COUNTRIES = {
    # East Asia & Pacific
    "AUS": "Australia", "CHN": "China", "FJI": "Fiji", "IDN": "Indonesia",
    "JPN": "Japan", "KHM": "Cambodia", "KOR": "Korea, Rep.", "LAO": "Lao PDR",
    "MMR": "Myanmar", "MYS": "Malaysia", "PHL": "Philippines", "PNG": "Papua New Guinea",
    "SGP": "Singapore", "THA": "Thailand", "VNM": "Vietnam",
    # Europe & Central Asia
    "ALB": "Albania", "ARM": "Armenia", "AUT": "Austria", "BEL": "Belgium",
    "BGR": "Bulgaria", "CHE": "Switzerland", "CYP": "Cyprus", "CZE": "Czech Republic",
    "DEU": "Germany", "DNK": "Denmark", "ESP": "Spain", "EST": "Estonia",
    "FIN": "Finland", "FRA": "France", "GBR": "United Kingdom", "GRC": "Greece",
    "HRV": "Croatia", "HUN": "Hungary", "IRL": "Ireland", "ITA": "Italy",
    "LTU": "Lithuania", "LVA": "Latvia", "NLD": "Netherlands", "NOR": "Norway",
    "POL": "Poland", "PRT": "Portugal", "ROU": "Romania", "RUS": "Russian Federation",
    "SVK": "Slovak Republic", "SVN": "Slovenia", "SWE": "Sweden", "TUR": "Turkey",
    "UKR": "Ukraine",
    # Latin America & Caribbean
    "ARG": "Argentina", "BOL": "Bolivia", "BRA": "Brazil", "CHL": "Chile",
    "COL": "Colombia", "CRI": "Costa Rica", "DOM": "Dominican Republic", "ECU": "Ecuador",
    "GTM": "Guatemala", "HND": "Honduras", "MEX": "Mexico", "NIC": "Nicaragua",
    "PAN": "Panama", "PER": "Peru", "PRY": "Paraguay", "SLV": "El Salvador",
    "URY": "Uruguay", "VEN": "Venezuela, RB",
    # Middle East & North Africa
    "ARE": "United Arab Emirates", "DZA": "Algeria", "EGY": "Egypt, Arab Rep.",
    "IRN": "Iran, Islamic Rep.", "IRQ": "Iraq", "ISR": "Israel", "JOR": "Jordan",
    "KWT": "Kuwait", "LBN": "Lebanon", "MAR": "Morocco", "SAU": "Saudi Arabia",
    "TUN": "Tunisia", "YEM": "Yemen, Rep.",
    # North America
    "CAN": "Canada", "USA": "United States",
    # South Asia
    "AFG": "Afghanistan", "BGD": "Bangladesh", "IND": "India", "LKA": "Sri Lanka",
    "NPL": "Nepal", "PAK": "Pakistan",
    # Sub-Saharan Africa
    "AGO": "Angola", "BEN": "Benin", "BFA": "Burkina Faso", "BWA": "Botswana",
    "CIV": "Cote d'Ivoire", "CMR": "Cameroon", "COD": "Congo, Dem. Rep.",
    "COG": "Congo, Rep.", "ETH": "Ethiopia", "GHA": "Ghana", "GIN": "Guinea",
    "KEN": "Kenya", "MDG": "Madagascar", "MLI": "Mali", "MOZ": "Mozambique",
    "MRT": "Mauritania", "MWI": "Malawi", "NAM": "Namibia", "NER": "Niger",
    "NGA": "Nigeria", "RWA": "Rwanda", "SEN": "Senegal", "SLE": "Sierra Leone",
    "TCD": "Chad", "TGO": "Togo", "UGA": "Uganda", "ZAF": "South Africa",
    "ZMB": "Zambia", "ZWE": "Zimbabwe"
}

def create_dim_country(spark, df_wdi_country, df_unuwider_meta, selected_countries):
    if df_wdi_country is None:
        logger.error("WDI Country metadata is missing.")
        return None
        
    dim_country = df_wdi_country.select(
        col("Country Code").alias("country_code"),
        col("Short Name").alias("short_name"),
        col("Long Name").alias("long_name"),
        col("Region").alias("region"),
        col("Income Group").alias("income_group"),
        col("Currency Unit").alias("currency_unit"),
        col("Lending category").alias("lending_category"),
        col("Special Notes").alias("special_notes")
    ).filter(col("country_code").isin(list(selected_countries.keys())))
    
    if df_unuwider_meta is not None:
        dim_country = dim_country.join(df_unuwider_meta, on="country_code", how="left")
    
    output_path = f"{PROCESSED_DIR}/dim_country"
    dim_country.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Created dim_country with {dim_country.count()} rows")
    return dim_country

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from etl_gmd import process_gmd, get_gmd_metadata
from etl_wdi import process_wdi, get_wdi_metadata
#from etl_fao import process_fao
from etl_unuwider import process_unuwider, get_unuwider_country_metadata, get_unuwider_indicator_metadata

def main():
    logger.info("=" * 80)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 80)
    
    logger.info("Loading Metadata for all sources...")
    df_wdi_country, df_wdi_series = get_wdi_metadata(spark)
    df_gmd_meta = get_gmd_metadata(spark)
    df_unuwider_meta = get_unuwider_country_metadata(spark)
    df_unuwider_ind_meta = get_unuwider_indicator_metadata(spark)
    dim_country = create_dim_country(spark, df_wdi_country, df_unuwider_meta, SELECTED_COUNTRIES)
    
    # 2. Process sources
    all_facts = []
    
    logger.info("Processing GMD...")
    gmd_facts = process_gmd(spark, SELECTED_COUNTRIES, None)
    if gmd_facts is not None:
        all_facts.append(gmd_facts)
        logger.info(f"GMD rows: {gmd_facts.count():,}")
    
    logger.info("Processing WDI...")
    wdi_facts = process_wdi(spark, SELECTED_COUNTRIES, None)
    if wdi_facts is not None:
        all_facts.append(wdi_facts)
        logger.info(f"WDI rows: {wdi_facts.count():,}")
    
    # logger.info("Processing FAO...")
    # fao_facts = process_fao(spark, SELECTED_COUNTRIES, None)
    # if fao_facts is not None:
    #     all_facts.append(fao_facts)
    #     logger.info(f"FAO rows: {fao_facts.count():,}")
    
    logger.info("Processing UNU-WIDER...")
    unw_facts = process_unuwider(spark, SELECTED_COUNTRIES, None)
    if unw_facts is not None:
        all_facts.append(unw_facts)
        logger.info(f"UNU-WIDER rows: {unw_facts.count():,}")
    
    if not all_facts:
        logger.error("No data processed!")
        spark.stop()
        sys.exit(1)
    
    logger.info("Combining all fact tables (keeping all rows)...")
    combined = all_facts[0]
    for df in all_facts[1:]:
        combined = combined.union(df)
    
    total_rows = combined.count()
    logger.info(f"Total rows before deduplication: {total_rows:,}")
    
    fact_output = f"{PROCESSED_DIR}/fact_economic_indicators"
    
    logger.info("Sorting fact table by Country -> Indicator -> Year...")
    combined_sorted = combined.orderBy("country_code", "indicator_code", "year")
    
    combined_sorted.write.mode("overwrite").parquet(fact_output)
    logger.info(f"Fact table saved with {total_rows:,} rows")
    
    logger.info("Creating dim_indicator from distinct indicator codes...")
    distinct_indicators = combined.select("indicator_code").distinct()
    
    if df_wdi_series is not None:
        wdi_meta = df_wdi_series.select(
            col("Series Code").alias("meta_code"),
            col("Topic").alias("topic"),
            col("Indicator Name").alias("indicator_name"),
            col("Long definition").alias("long_definition"),
            col("Unit of measure").alias("unit_of_measure"),
            col("Periodicity").alias("periodicity"),
            col("Source").alias("source"),
            col("Statistical concept and methodology").alias("statistical_concept")
        )
        
        all_meta = wdi_meta
        if df_gmd_meta is not None:
            all_meta = all_meta.unionByName(df_gmd_meta, allowMissingColumns=True)
        if df_unuwider_ind_meta is not None:
            all_meta = all_meta.unionByName(df_unuwider_ind_meta, allowMissingColumns=True)
            
        dim_indicator = distinct_indicators.join(
            all_meta,
            distinct_indicators.indicator_code == all_meta.meta_code,
            "left"
        ).drop("meta_code")
    
    dim_indicator_output = f"{PROCESSED_DIR}/dim_indicator"
    dim_indicator.coalesce(1).write.mode("overwrite").parquet(dim_indicator_output)
    logger.info(f"dim_indicator created with {dim_indicator.count():,} rows")
    
    logger.info("=" * 80)
    logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()