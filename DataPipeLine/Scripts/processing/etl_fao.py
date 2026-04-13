import os
import re
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, when, coalesce, regexp_extract, trim, lower, regexp_replace, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd

logger = logging.getLogger(__name__)

def read_fao_csv(spark, file_path):
    encodings = ["UTF-8", "ISO-8859-1", "latin1"]
    for enc in encodings:
        try:
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", enc) \
                .csv(file_path)
            logger.info(f"Read {file_path} with encoding {enc}")
            return df
        except Exception as e:
            continue
    logger.error(f"Cannot read {file_path}")
    return None

def load_fao_metadata(spark, data_dir, dataset_name):
    base = os.path.join(data_dir, dataset_name)
    elements_path = None
    itemcodes_path = None
    areacodes_path = None
    
    for f in os.listdir(base):
        if f.endswith("_E_Elements.csv"):
            elements_path = os.path.join(base, f)
        elif f.endswith("_E_ItemCodes.csv"):
            itemcodes_path = os.path.join(base, f)
        elif f.endswith("_E_AreaCodes.csv"):
            areacodes_path = os.path.join(base, f)
    
    elements_df = None
    itemcodes_df = None
    areacodes_df = None
    
    if elements_path:
        elements_df = spark.read.option("header", "true").csv(elements_path)
        actual_cols = elements_df.columns
        
        unit_col = col("Unit") if "Unit" in actual_cols else lit(None)
        desc_col = col("Description") if "Description" in actual_cols else lit(None)
        
        elements_df = elements_df.select(
            col("Element Code").alias("element_code"),
            col("Element").alias("element_name"),
            unit_col.alias("unit"),
            desc_col.alias("element_description")
        )
        
    if itemcodes_path:
        itemcodes_df = spark.read.option("header", "true").csv(itemcodes_path)
        actual_cols = itemcodes_df.columns
        
        desc_col = col("Description") if "Description" in actual_cols else lit(None)
        itemcodes_df = itemcodes_df.select(
            col("Item Code").alias("item_code"),
            col("Item").alias("item_name"),
            desc_col.alias("item_description")
        )
        
    if areacodes_path:
        areacodes_df = spark.read.option("header", "true").csv(areacodes_path)
        actual_cols = areacodes_df.columns
        
        m49_col = lit(None)
        if "M49 Code" in actual_cols:
            m49_col = col("M49 Code")
        elif "Area Code (M49)" in actual_cols:
            m49_col = col("Area Code (M49)")
            
        areacodes_df = areacodes_df.select(
            col("Area Code").alias("area_code"),
            m49_col.alias("area_code_m49"),
            col("Area").alias("area_name")
        )
        
    return elements_df, itemcodes_df, areacodes_df

def normalize_name(name):
    """Chuẩn hóa tên quốc gia: lowercase, bỏ dấu câu, khoảng trắng thừa"""
    if name is None:
        return ""
    name = str(name).lower()
    # Xóa dấu câu nhưng giữ chữ, số và khoảng trắng
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def build_country_mapping(spark, data_dir, selected_countries):
    """
    Xây dựng mapping từ FAO area_code (số) và area_code_m49 sang ISO3.
    Sử dụng alias cho các tên không khớp chính xác.
    """
    # Định nghĩa alias cho các quốc gia có tên khác biệt lớn
    country_aliases = {
        "VNM": ["vietnam", "viet nam"],
        "USA": ["united states", "united states of america"],
        "KOR": ["korea rep", "korea republic of", "republic of korea"],
        "EGY": ["egypt arab rep", "egypt"],
        "RUS": ["russian federation", "russia"],
        "VEN": ["venezuela rb", "venezuela bolivarian republic of", "venezuela"],
        "GBR": ["united kingdom", "uk"],
        "LAO": ["lao pdr", "laos"],
        "IRN": ["iran islamic rep", "iran"],
        "SYR": ["syrian arab republic", "syria"],
        "TZA": ["tanzania united republic of", "tanzania"],
        "COD": ["congo democratic republic of the", "drc", "congo dr"],
        "COG": ["congo republic of the", "congo rep"],
        "CIV": ["cote d'ivoire", "ivory coast"],
        "MKD": ["north macedonia", "macedonia"],
        "PSE": ["palestine", "state of palestine"],
        "BOL": ["bolivia plurinational state of", "bolivia"],
        "PRK": ["korea democratic people's republic of", "north korea"],
    }
    
    # Tìm file AreaCodes bất kỳ
    for dataset in os.listdir(data_dir):
        subdir = os.path.join(data_dir, dataset)
        if os.path.isdir(subdir):
            for f in os.listdir(subdir):
                if f.endswith("_E_AreaCodes.csv"):
                    path = os.path.join(subdir, f)
                    df = spark.read.option("header", "true").csv(path)
                    df = df.select(
                        col("Area Code").alias("area_code"),
                        col("M49 Code").alias("area_code_m49"),
                        col("Area").alias("area_name")
                    ).distinct()
                    df = df.filter(col("area_code_m49").isNotNull() & (col("area_code_m49") != ""))
                    pdf = df.toPandas()
                    # Tạo mapping
                    mapping = {}
                    pdf['area_name_norm'] = pdf['area_name'].apply(normalize_name)
                    for iso3, wdi_name in selected_countries.items():
                        wdi_name_norm = normalize_name(wdi_name)
                        possible_names = {wdi_name_norm}
                        if iso3 in country_aliases:
                            possible_names.update(country_aliases[iso3])
                        match = pdf[pdf['area_name_norm'].isin(possible_names)]
                        if not match.empty:
                            area_code = match.iloc[0]['area_code']
                            area_code_m49 = match.iloc[0]['area_code_m49']
                            mapping[area_code] = iso3
                            mapping[area_code_m49] = iso3
                        else:
                            pass
                    return mapping
    return {}

def process_fao_dataset(spark, data_dir, dataset_name, country_mapping, years=range(1990, 2025)):
    main_file = None
    dataset_path = os.path.join(data_dir, dataset_name)
    if not os.path.isdir(dataset_path):
        logger.warning(f"Dataset directory not found: {dataset_path}")
        return None, None
    for f in os.listdir(dataset_path):
        if f.endswith("_E_All_Data_(Normalized).csv"):
            main_file = os.path.join(dataset_path, f)
            break
    if not main_file:
        logger.warning(f"No main data file for {dataset_name}")
        return None, None

    df = read_fao_csv(spark, main_file)
    if df is None:
        return None, None

    if "Year" in df.columns:
        df = df.filter(col("Year").cast("int").isin(list(years)))
    else:
        logger.warning(f"No Year column in {dataset_name}")
        return None, None

    # Xác định cột area
    area_col = None
    if "Area Code (M49)" in df.columns:
        area_col = "Area Code (M49)"
    elif "Area Code" in df.columns:
        area_col = "Area Code"
    else:
        logger.warning(f"No area column in {dataset_name}")
        return None, None

    mapping_expr = None
    for area_code, iso3 in country_mapping.items():
        condition = col(area_col).cast("string") == str(area_code)
        
        if mapping_expr is None:
            mapping_expr = when(condition, lit(iso3))
        else:
            mapping_expr = mapping_expr.when(condition, lit(iso3))
            
    df = df.withColumn("country_code", mapping_expr)
    df = df.filter(col("country_code").isNotNull())

    # Tạo indicator_code
    if "Element Code" not in df.columns:
        logger.warning(f"No Element Code in {dataset_name}")
        return None, None

    prefix = dataset_name.split("_")[0]
    prefix = re.sub(r'[^A-Za-z]', '', prefix)  # chỉ giữ chữ cái

    if "Item Code" in df.columns:
        df = df.withColumn("indicator_code",
                           expr(f"concat('FAO_{prefix}_', `Element Code`, '_', `Item Code`)"))
    else:
        df = df.withColumn("indicator_code",
                           expr(f"concat('FAO_{prefix}_', `Element Code`)"))

    # Chuyển value sang double
    if "Value" in df.columns:
        df = df.withColumn("value", col("Value").cast(DoubleType()))
        df = df.filter(col("value").isNotNull())
    else:
        logger.warning(f"No Value column in {dataset_name}")
        return None, None

    # Chọn các cột fact
    fact_df = df.select(
        col("country_code"),
        col("Year").cast("int").alias("year"),
        col("indicator_code"),
        col("value"),
        lit(4).alias("source_priority"),
        col("indicator_code").alias("source_specific")
    )

    elements_df, itemcodes_df, _ = load_fao_metadata(spark, data_dir, dataset_name)
    
    all_indicators = fact_df.select("indicator_code").distinct()
    
    if "Unit" in df.columns:
        fact_units = df.select("indicator_code", col("Unit").alias("unit_from_fact")) \
                       .filter(col("Unit").isNotNull() & (col("Unit") != "")) \
                       .dropDuplicates(["indicator_code"])
        distinct_indicators = all_indicators.join(fact_units, on="indicator_code", how="left")
    else:
        distinct_indicators = all_indicators.withColumn("unit_from_fact", lit(None))
        
    distinct_indicators = distinct_indicators.withColumn(
        "element_code",
        regexp_extract(col("indicator_code"), r'FAO_[A-Za-z]+_([A-Za-z0-9]+)(?:_([A-Za-z0-9]+))?', 1)
    )
    distinct_indicators = distinct_indicators.withColumn(
        "item_code",
        regexp_extract(col("indicator_code"), r'FAO_[A-Za-z]+_[A-Za-z0-9]+_([A-Za-z0-9]+)', 1)
    )
    distinct_indicators = distinct_indicators.withColumn(
        "item_code",
        when(col("item_code") == "", lit(None)).otherwise(col("item_code"))
    )
    
    if elements_df is not None:
        metadata = distinct_indicators.join(elements_df, on="element_code", how="left")
    else:
        metadata = distinct_indicators
    if itemcodes_df is not None:
        metadata = metadata.join(itemcodes_df, on="item_code", how="left")
    
    metadata = metadata.withColumn("topic", lit(f"FAO - {dataset_name}"))
    if "item_name" in metadata.columns:
        metadata = metadata.withColumn(
            "indicator_name",
            when(col("item_name").isNotNull(),
                 concat_ws(" - ", col("element_name"), col("item_name"))
            ).otherwise(col("element_name"))
        )
    else:
        metadata = metadata.withColumn("indicator_name", col("element_name"))
        
    if "item_description" in metadata.columns:
        metadata = metadata.withColumn(
            "long_definition",
            when(col("item_description").isNotNull(),
                 concat_ws(": ", col("element_description"), col("item_description"))
            ).otherwise(col("element_description"))
        )
    else:
        metadata = metadata.withColumn("long_definition", col("element_description"))
        
    element_unit = col("unit") if "unit" in metadata.columns else lit(None)
    
    metadata = metadata.withColumn(
        "unit_of_measure",
        coalesce(element_unit, col("unit_from_fact"), lit("Not specified"))
    )
    metadata = metadata.withColumn("periodicity", lit("Annual"))
    metadata = metadata.withColumn(
        "statistical_concept",
        lit("FAO statistical database. Methodology: http://www.fao.org/statistics/methodology")
    )
    metadata = metadata.withColumn("source", lit("Food and Agriculture Organization (FAO)"))
    
    dim_indicator = metadata.select(
        col("indicator_code").alias("meta_code"),
        "topic",
        "indicator_name",
        "long_definition",
        "unit_of_measure",
        "periodicity",
        "statistical_concept",
        "source"
    )
    
    return fact_df, dim_indicator

def process_fao(spark, selected_countries, dim_indicator=None):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/fao/"
    if not os.path.exists(data_dir):
        logger.error(f"FAO data directory not found: {data_dir}")
        return None, None

    country_mapping = build_country_mapping(spark, data_dir, selected_countries)
    if not country_mapping:
        logger.error("Could not build country mapping from FAO area codes")
        return None, None

    datasets = [
        "Exchange_rate_E_All_Data_(Normalized)",
        "Macro-Statistics_Key_Indicators_E_All_Data_(Normalized)",
        "Prices_E_All_Data_(Normalized)",
        "Production_Crops_Livestock_E_All_Data_(Normalized)",
        "Production_Indices_E_All_Data_(Normalized)",
        "Trade_CropsLivestock_E_All_Data_(Normalized)",
        "Trade_Indices_E_All_Data_(Normalized)",
        "Value_of_Production_E_All_Data_(Normalized) (1)"
    ]
    
    all_facts = []
    all_metadata = []
    
    for ds in datasets:
        logger.info(f"Processing FAO dataset: {ds}")
        fact_df, meta_df = process_fao_dataset(spark, data_dir, ds, country_mapping)
        if fact_df is not None:
            all_facts.append(fact_df)
            logger.info(f"  Fact rows: {fact_df.count()}")
        if meta_df is not None:
            all_metadata.append(meta_df)
            logger.info(f"  Metadata rows: {meta_df.count()}")
    
    if not all_facts:
        logger.error("No FAO data processed")
        return None, None
    
    combined_fact = all_facts[0]
    for df in all_facts[1:]:
        combined_fact = combined_fact.union(df)
    
    combined_metadata = None
    if all_metadata:
        combined_metadata = all_metadata[0]
        for df in all_metadata[1:]:
            combined_metadata = combined_metadata.union(df)
        combined_metadata = combined_metadata.dropDuplicates(["meta_code"])
    
    logger.info(f"Total FAO fact rows: {combined_fact.count()}")
    if combined_metadata:
        logger.info(f"Total FAO metadata rows: {combined_metadata.count()}")
    
    return combined_fact, combined_metadata