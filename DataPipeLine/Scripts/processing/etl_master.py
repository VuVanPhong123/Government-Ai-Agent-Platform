import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

os.environ["PYSPARK_PYTHON"] = sys.executable.replace("\\", "/")
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable.replace("\\", "/")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Đường dẫn thư mục
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
    # East Asia & Pacific (19)
    "AUS": "Australia", "CHN": "China", "FJI": "Fiji", "IDN": "Indonesia",
    "JPN": "Japan", "KHM": "Cambodia", "KOR": "Korea, Rep.", "LAO": "Lao PDR",
    "MMR": "Myanmar", "MYS": "Malaysia", "PHL": "Philippines", "PNG": "Papua New Guinea",
    "SGP": "Singapore", "THA": "Thailand", "VNM": "Vietnam",
    "NZL": "New Zealand", "MNG": "Mongolia", "BRN": "Brunei Darussalam", "PRK": "Korea, Dem. People's Rep.",
    
    # Europe & Central Asia (47)
    "ALB": "Albania", "ARM": "Armenia", "AUT": "Austria", "BEL": "Belgium",
    "BGR": "Bulgaria", "CHE": "Switzerland", "CYP": "Cyprus", "CZE": "Czech Republic",
    "DEU": "Germany", "DNK": "Denmark", "ESP": "Spain", "EST": "Estonia",
    "FIN": "Finland", "FRA": "France", "GBR": "United Kingdom", "GRC": "Greece",
    "HRV": "Croatia", "HUN": "Hungary", "IRL": "Ireland", "ITA": "Italy",
    "LTU": "Lithuania", "LVA": "Latvia", "NLD": "Netherlands", "NOR": "Norway",
    "POL": "Poland", "PRT": "Portugal", "ROU": "Romania", "RUS": "Russian Federation",
    "SVK": "Slovak Republic", "SVN": "Slovenia", "SWE": "Sweden", "TUR": "Turkey", "UKR": "Ukraine",
    "AZE": "Azerbaijan", "BIH": "Bosnia and Herzegovina", "GEO": "Georgia", "ISL": "Iceland",
    "KAZ": "Kazakhstan", "KGZ": "Kyrgyz Republic", "LUX": "Luxembourg", "MKD": "North Macedonia",
    "MLT": "Malta", "MNE": "Montenegro", "SRB": "Serbia", "TJK": "Tajikistan", "TKM": "Turkmenistan", "UZB": "Uzbekistan",
    
    # Latin America & Caribbean (22)
    "ARG": "Argentina", "BOL": "Bolivia", "BRA": "Brazil", "CHL": "Chile",
    "COL": "Colombia", "CRI": "Costa Rica", "DOM": "Dominican Republic", "ECU": "Ecuador",
    "GTM": "Guatemala", "HND": "Honduras", "MEX": "Mexico", "NIC": "Nicaragua",
    "PAN": "Panama", "PER": "Peru", "PRY": "Paraguay", "SLV": "El Salvador",
    "URY": "Uruguay", "VEN": "Venezuela, RB",
    "CUB": "Cuba", "JAM": "Jamaica", "HTI": "Haiti", "BHS": "Bahamas, The",
    
    # Middle East & North Africa (18)
    "ARE": "United Arab Emirates", "DZA": "Algeria", "EGY": "Egypt, Arab Rep.",
    "IRN": "Iran, Islamic Rep.", "IRQ": "Iraq", "ISR": "Israel", "JOR": "Jordan",
    "KWT": "Kuwait", "LBN": "Lebanon", "MAR": "Morocco", "SAU": "Saudi Arabia",
    "TUN": "Tunisia", "YEM": "Yemen, Rep.",
    "QAT": "Qatar", "OMN": "Oman", "BHR": "Bahrain", "SYR": "Syrian Arab Republic", "PSE": "West Bank and Gaza",
    
    # North America (2)
    "CAN": "Canada", "USA": "United States",
    
    # South Asia (8)
    "AFG": "Afghanistan", "BGD": "Bangladesh", "IND": "India", "LKA": "Sri Lanka",
    "NPL": "Nepal", "PAK": "Pakistan",
    "MDV": "Maldives", "BTN": "Bhutan",
    
    # Sub-Saharan Africa (34)
    "AGO": "Angola", "BEN": "Benin", "BFA": "Burkina Faso", "BWA": "Botswana",
    "CIV": "Cote d'Ivoire", "CMR": "Cameroon", "COD": "Congo, Dem. Rep.",
    "COG": "Congo, Rep.", "ETH": "Ethiopia", "GHA": "Ghana", "GIN": "Guinea",
    "KEN": "Kenya", "MDG": "Madagascar", "MLI": "Mali", "MOZ": "Mozambique",
    "MRT": "Mauritania", "MWI": "Malawi", "NAM": "Namibia", "NER": "Niger",
    "NGA": "Nigeria", "RWA": "Rwanda", "SEN": "Senegal", "SLE": "Sierra Leone",
    "TCD": "Chad", "TGO": "Togo", "UGA": "Uganda", "ZAF": "South Africa",
    "ZMB": "Zambia", "ZWE": "Zimbabwe",
    "TZA": "Tanzania", "SDN": "Sudan", "BDI": "Burundi", "GAB": "Gabon", "SOM": "Somalia"
}

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from etl_gmd import process_gmd, get_gmd_metadata
from etl_wdi import process_wdi, get_wdi_metadata
from etl_unuwider import process_unuwider, get_unuwider_country_metadata, get_unuwider_indicator_metadata
from etl_fao import process_fao

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
        unw_cols = [c for c in df_unuwider_meta.columns if c not in dim_country.columns]
        if unw_cols:
            dim_country = dim_country.join(
                df_unuwider_meta.select("country_code", *unw_cols),
                on="country_code",
                how="left"
            )

    output_path = f"{PROCESSED_DIR}/dim_country"
    dim_country.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info(f"Created dim_country with {dim_country.count()} rows")
    return dim_country

def create_dim_indicator(spark, fact_df, list_of_metadata_dfs):
    distinct_indicators = fact_df.select("indicator_code").distinct()

    all_meta = None
    for meta_df in list_of_metadata_dfs:
        if meta_df is None:
            continue
        meta_renamed = meta_df.withColumnRenamed("meta_code", "indicator_code")
        if all_meta is None:
            all_meta = meta_renamed
        else:
            all_meta = all_meta.unionByName(meta_renamed, allowMissingColumns=True)

    if all_meta is not None:
        all_meta = all_meta.dropDuplicates(["indicator_code"])
        dim_indicator = distinct_indicators.join(all_meta, on="indicator_code", how="left")
    else:
        dim_indicator = distinct_indicators

    required_cols = ["topic", "indicator_name", "long_definition", "unit_of_measure",
                     "periodicity", "statistical_concept", "source"]
    for c in required_cols:
        if c not in dim_indicator.columns:
            dim_indicator = dim_indicator.withColumn(c, lit(None))

    output_path = f"{PROCESSED_DIR}/dim_indicator"
    dim_indicator.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info(f"Created dim_indicator with {dim_indicator.count()} rows")
    return dim_indicator

def main():
    logger.info("=" * 80)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 80)

    logger.info("Loading metadata from all sources...")
    df_wdi_country, df_wdi_series = get_wdi_metadata(spark)
    df_gmd_meta = get_gmd_metadata(spark)
    df_unuwider_meta = get_unuwider_country_metadata(spark)
    df_unuwider_ind_meta = get_unuwider_indicator_metadata(spark)

    dim_country = create_dim_country(spark, df_wdi_country, df_unuwider_meta, SELECTED_COUNTRIES)
    if dim_country is None:
        logger.error("Cannot proceed without dim_country.")
        spark.stop()
        sys.exit(1)

    all_facts = []
    all_metadata = []

    # GMD
    logger.info("Processing GMD...")
    gmd_facts = process_gmd(spark, SELECTED_COUNTRIES, None)
    if gmd_facts is not None:
        all_facts.append(gmd_facts)
        logger.info(f"GMD rows: {gmd_facts.count():,}")
    if df_gmd_meta is not None:
        all_metadata.append(df_gmd_meta)

    # WDI
    logger.info("Processing WDI...")
    wdi_facts = process_wdi(spark, SELECTED_COUNTRIES, None)
    if wdi_facts is not None:
        all_facts.append(wdi_facts)
        logger.info(f"WDI rows: {wdi_facts.count():,}")
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
        all_metadata.append(wdi_meta)

    # UNU-WIDER
    logger.info("Processing UNU-WIDER...")
    unw_facts = process_unuwider(spark, SELECTED_COUNTRIES, None)
    if unw_facts is not None:
        all_facts.append(unw_facts)
        logger.info(f"UNU-WIDER rows: {unw_facts.count():,}")
    if df_unuwider_ind_meta is not None:
        all_metadata.append(df_unuwider_ind_meta)

    # FAO
    logger.info("Processing FAO...")
    fao_facts, fao_meta = process_fao(spark, SELECTED_COUNTRIES, None)
    if fao_facts is not None:
        all_facts.append(fao_facts)
        logger.info(f"FAO rows: {fao_facts.count():,}")
    if fao_meta is not None:
        all_metadata.append(fao_meta)

    if not all_facts:
        logger.error("No data processed from any source!")
        spark.stop()
        sys.exit(1)

    logger.info("Combining all fact tables...")
    combined_fact = all_facts[0]
    for df in all_facts[1:]:
        combined_fact = combined_fact.union(df)

    total_rows = combined_fact.count()
    logger.info(f"Total rows before deduplication: {total_rows:,}")

    combined_fact = combined_fact.orderBy("country_code", "indicator_code", "year")

    fact_output = f"{PROCESSED_DIR}/fact_economic_indicators"
    combined_fact.write.mode("overwrite").parquet(fact_output)
    logger.info(f"Fact table saved with {total_rows:,} rows")

    logger.info("Creating dim_indicator...")
    dim_indicator = create_dim_indicator(spark, combined_fact, all_metadata)

    logger.info("=" * 80)
    logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)

    spark.stop()

if __name__ == "__main__":
    main()