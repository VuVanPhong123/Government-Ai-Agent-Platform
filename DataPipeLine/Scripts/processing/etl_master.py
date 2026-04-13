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

REGION_MAPPING = {
    # East Asia & Pacific
    "AUS": "East Asia & Pacific", "CHN": "East Asia & Pacific", "FJI": "East Asia & Pacific",
    "IDN": "East Asia & Pacific", "JPN": "East Asia & Pacific", "KHM": "East Asia & Pacific",
    "KOR": "East Asia & Pacific", "LAO": "East Asia & Pacific", "MMR": "East Asia & Pacific",
    "MYS": "East Asia & Pacific", "PHL": "East Asia & Pacific", "PNG": "East Asia & Pacific",
    "SGP": "East Asia & Pacific", "THA": "East Asia & Pacific", "VNM": "East Asia & Pacific",
    # Europe & Central Asia
    "ALB": "Europe & Central Asia", "ARM": "Europe & Central Asia", "AUT": "Europe & Central Asia",
    "BEL": "Europe & Central Asia", "BGR": "Europe & Central Asia", "CHE": "Europe & Central Asia",
    "CYP": "Europe & Central Asia", "CZE": "Europe & Central Asia", "DEU": "Europe & Central Asia",
    "DNK": "Europe & Central Asia", "ESP": "Europe & Central Asia", "EST": "Europe & Central Asia",
    "FIN": "Europe & Central Asia", "FRA": "Europe & Central Asia", "GBR": "Europe & Central Asia",
    "GRC": "Europe & Central Asia", "HRV": "Europe & Central Asia", "HUN": "Europe & Central Asia",
    "IRL": "Europe & Central Asia", "ITA": "Europe & Central Asia", "LTU": "Europe & Central Asia",
    "LVA": "Europe & Central Asia", "NLD": "Europe & Central Asia", "NOR": "Europe & Central Asia",
    "POL": "Europe & Central Asia", "PRT": "Europe & Central Asia", "ROU": "Europe & Central Asia",
    "RUS": "Europe & Central Asia", "SVK": "Europe & Central Asia", "SVN": "Europe & Central Asia",
    "SWE": "Europe & Central Asia", "TUR": "Europe & Central Asia", "UKR": "Europe & Central Asia",
    # Latin America & Caribbean
    "ARG": "Latin America & Caribbean", "BOL": "Latin America & Caribbean",
    "BRA": "Latin America & Caribbean", "CHL": "Latin America & Caribbean",
    "COL": "Latin America & Caribbean", "CRI": "Latin America & Caribbean",
    "DOM": "Latin America & Caribbean", "ECU": "Latin America & Caribbean",
    "GTM": "Latin America & Caribbean", "HND": "Latin America & Caribbean",
    "MEX": "Latin America & Caribbean", "NIC": "Latin America & Caribbean",
    "PAN": "Latin America & Caribbean", "PER": "Latin America & Caribbean",
    "PRY": "Latin America & Caribbean", "SLV": "Latin America & Caribbean",
    "URY": "Latin America & Caribbean", "VEN": "Latin America & Caribbean",
    # Middle East & North Africa
    "ARE": "Middle East & North Africa", "DZA": "Middle East & North Africa",
    "EGY": "Middle East & North Africa", "IRN": "Middle East & North Africa",
    "IRQ": "Middle East & North Africa", "ISR": "Middle East & North Africa",
    "JOR": "Middle East & North Africa", "KWT": "Middle East & North Africa",
    "LBN": "Middle East & North Africa", "MAR": "Middle East & North Africa",
    "SAU": "Middle East & North Africa", "TUN": "Middle East & North Africa",
    "YEM": "Middle East & North Africa",
    # North America
    "CAN": "North America", "USA": "North America",
    # South Asia
    "AFG": "South Asia", "BGD": "South Asia", "IND": "South Asia", "LKA": "South Asia",
    "NPL": "South Asia", "PAK": "South Asia",
    # Sub-Saharan Africa
    "AGO": "Sub-Saharan Africa", "BEN": "Sub-Saharan Africa", "BFA": "Sub-Saharan Africa",
    "BWA": "Sub-Saharan Africa", "CIV": "Sub-Saharan Africa", "CMR": "Sub-Saharan Africa",
    "COD": "Sub-Saharan Africa", "COG": "Sub-Saharan Africa", "ETH": "Sub-Saharan Africa",
    "GHA": "Sub-Saharan Africa", "GIN": "Sub-Saharan Africa", "KEN": "Sub-Saharan Africa",
    "MDG": "Sub-Saharan Africa", "MLI": "Sub-Saharan Africa", "MOZ": "Sub-Saharan Africa",
    "MRT": "Sub-Saharan Africa", "MWI": "Sub-Saharan Africa", "NAM": "Sub-Saharan Africa",
    "NER": "Sub-Saharan Africa", "NGA": "Sub-Saharan Africa", "RWA": "Sub-Saharan Africa",
    "SEN": "Sub-Saharan Africa", "SLE": "Sub-Saharan Africa", "TCD": "Sub-Saharan Africa",
    "TGO": "Sub-Saharan Africa", "UGA": "Sub-Saharan Africa", "ZAF": "Sub-Saharan Africa",
    "ZMB": "Sub-Saharan Africa", "ZWE": "Sub-Saharan Africa"
}

def create_dim_country():
    schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("income_group", StringType(), True),
        StructField("is_selected", BooleanType(), True)
    ])
    data = [(code, name, REGION_MAPPING.get(code, "Other"), "Unknown", True) 
            for code, name in SELECTED_COUNTRIES.items()]
    df = spark.createDataFrame(data, schema)
    
    output_path = f"{PROCESSED_DIR}/dim_country"
    
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Created dim_country with {df.count()} rows")
    return df

# Import các module ETL
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from etl_gmd import process_gmd
from etl_wdi import process_wdi
from etl_fao import process_fao
from etl_unuwider import process_unuwider

def main():
    logger.info("=" * 80)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 80)
    
    # 1. dim_country
    dim_country = create_dim_country()
    
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
    
    logger.info("Processing FAO...")
    fao_facts = process_fao(spark, SELECTED_COUNTRIES, None)
    if fao_facts is not None:
        all_facts.append(fao_facts)
        logger.info(f"FAO rows: {fao_facts.count():,}")
    
    logger.info("Processing UNU-WIDER...")
    unw_facts = process_unuwider(spark, SELECTED_COUNTRIES, None)
    if unw_facts is not None:
        all_facts.append(unw_facts)
        logger.info(f"UNU-WIDER rows: {unw_facts.count():,}")
    
    if not all_facts:
        logger.error("No data processed!")
        spark.stop()
        sys.exit(1)
    
    # 3. Combine all facts (without deduplication)
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
    
    # 5. Create dim_indicator
    logger.info("Creating dim_indicator from distinct indicator codes...")
    distinct_indicators = combined.select("indicator_code", "source_specific").distinct()
    dim_indicator = distinct_indicators.withColumn("indicator_name", col("indicator_code")) \
                                       .withColumn("category", lit("Unknown")) \
                                       .withColumn("unit", lit("Unknown")) \
                                       .withColumn("source_priority_str", lit(""))
    
    dim_indicator_output = f"{PROCESSED_DIR}/dim_indicator"
    dim_indicator.coalesce(1).write.mode("overwrite").parquet(dim_indicator_output)
    logger.info(f"dim_indicator created with {dim_indicator.count():,} rows")
    
    logger.info("=" * 80)
    logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()