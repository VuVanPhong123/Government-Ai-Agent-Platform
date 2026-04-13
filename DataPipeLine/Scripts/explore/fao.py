import os
import glob
import logging
from pyspark.sql.functions import col, lit

logger = logging.getLogger(__name__)

FAO_ELEMENT_MAPPING = {
    "Production": "AGRI_PRODUCTION",
    "Yield": "AGRI_YIELD",
    "Area harvested": "AGRI_AREA_HARVESTED",
    "Gross Production Value (current thousand US$)": "AGRI_VALUE_USD",
    "Gross Production Value (constant 2014-2016 thousand I$)": "AGRI_VALUE_INT",
    "Gross Production Value (current thousand SLC)": "AGRI_VALUE_SLC",
    "Gross Production Index Number (2014-2016 = 100)": "AGRI_PROD_INDEX",
    "Export value": "AGRI_TRADE_EXPORT_VALUE",
    "Import value": "AGRI_TRADE_IMPORT_VALUE",
    "Export quantity": "AGRI_TRADE_EXPORT_QUANTITY",
    "Import quantity": "AGRI_TRADE_IMPORT_QUANTITY",
    "Export Unit/Value Index (2014-2016 = 100)": "EXPORT_PRICE_INDEX",
    "Import Unit/Value Index (2014-2016 = 100)": "IMPORT_PRICE_INDEX",
    "Export Value Index (2014-2016 = 100)": "EXPORT_VALUE_INDEX",
    "Import Value Index (2014-2016 = 100)": "IMPORT_VALUE_INDEX",
    "Local currency units per USD": "EXCHANGE_RATE",
    "Producer Price (USD/tonne)": "PRODUCER_PRICE",
    "Producer Price Index (2014-2016 = 100)": "PRODUCER_PRICE_INDEX",
    "Annual growth US$": "GDP_GROWTH",
    "Value US$": "GDP_USD",
}

def read_csv_robust(spark, file_path):
    for encoding in ["UTF-8", "ISO-8859-1", "latin1"]:
        try:
            return spark.read.option("header", "true") \
                             .option("inferSchema", "true") \
                             .option("encoding", encoding) \
                             .csv(file_path)
        except:
            continue
    return None

def process_fao(spark, selected_countries, dim_indicator):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/fao/"
    # Tìm tất cả file CSV nằm trong thư mục con (sâu 1 cấp)
    data_files = glob.glob(os.path.join(data_dir, "*", "*.csv"))
    # Lọc chỉ lấy các file dữ liệu chính (không phải metadata)
    data_files = [f for f in data_files if "_All_Data_" in f]
    logger.info(f"Found {len(data_files)} FAO data files")
    
    all_facts = []
    # Map tên quốc gia từ FAO (Area) sang ISO3
    # Tạo dictionary từ selected_countries: name -> code
    name_to_iso = {v: k for k, v in selected_countries.items()}
    # Thêm các tên viết tắt nếu có
    # Một số quốc gia trong FAO có thể dùng tên khác, cần bổ sung mapping
    extra_mapping = {
        "United States of America": "USA",
        "Russian Federation": "RUS",
        "Korea, Republic of": "KOR",
        "Iran (Islamic Republic of)": "IRN",
        "Viet Nam": "VNM",
        "Lao People's Democratic Republic": "LAO",
        "Syrian Arab Republic": "SYR",
        "Venezuela (Bolivarian Republic of)": "VEN",
        "Bolivia (Plurinational State of)": "BOL",
        "Tanzania, United Republic of": "TZA",
        "Côte d'Ivoire": "CIV",
        "Congo, Democratic Republic of the": "COD",
        "Congo": "COG",
        "Egypt": "EGY",
        "Gambia": "GMB",
        "Guinea-Bissau": "GNB",
        "Equatorial Guinea": "GNQ",
        "Macedonia": "MKD",
        "Moldova": "MDA",
        "Kyrgyzstan": "KGZ",
        "Tajikistan": "TJK",
        "Turkmenistan": "TKM",
        "Uzbekistan": "UZB",
        "Azerbaijan": "AZE",
        "Georgia": "GEO",
        "Armenia": "ARM",
        "Belarus": "BLR",
        "Bosnia and Herzegovina": "BIH",
        "Serbia": "SRB",
        "Montenegro": "MNE",
        "Kosovo": "XKX",
    }
    for name, code in extra_mapping.items():
        name_to_iso[name] = code
    
    for file_path in data_files:
        file_name = os.path.basename(file_path)
        logger.info(f"Processing FAO file: {file_name}")
        df = read_csv_robust(spark, file_path)
        if df is None:
            continue
        
        if "Area" in df.columns and "Element" in df.columns and "Year" in df.columns:
            # Lọc các quốc gia có trong mapping (cả tên gốc và tên thay thế)
            fao_areas = df.select("Area").distinct().collect()
            fao_areas_list = [row["Area"] for row in fao_areas]
            matched_areas = [area for area in fao_areas_list if area in name_to_iso]
            logger.info(f"  Matching {len(matched_areas)} countries out of {len(fao_areas_list)}")
            if not matched_areas:
                logger.warning(f"  No matching countries in {file_name}")
                continue
            
            df_filtered = df.filter(col("Area").isin(matched_areas))
            df_filtered = df_filtered.filter((col("Year") >= 1995) & (col("Year") <= 2024))
            
            for element, std_code in FAO_ELEMENT_MAPPING.items():
                # Thử lấy trực tiếp cột có tên element
                if element in df.columns:
                    temp_df = df_filtered.select(
                        col("Area").alias("country_name"),
                        col("Year").alias("year"),
                        lit(std_code).alias("indicator_code"),
                        col(element).cast("double").alias("value"),
                        lit(4).alias("source_priority"),
                        lit(f"FAO_{file_name}_{element}").alias("source_specific")
                    ).filter(col("value").isNotNull())
                    if temp_df.count() > 0:
                        all_facts.append(temp_df)
                else:
                    # Dùng cột Element
                    element_df = df_filtered.filter(col("Element") == element)
                    if element_df.count() > 0:
                        temp_df = element_df.select(
                            col("Area").alias("country_name"),
                            col("Year").alias("year"),
                            lit(std_code).alias("indicator_code"),
                            col("Value").cast("double").alias("value"),
                            lit(4).alias("source_priority"),
                            lit(f"FAO_{file_name}_{element}").alias("source_specific")
                        ).filter(col("value").isNotNull())
                        all_facts.append(temp_df)
    
    if not all_facts:
        logger.warning("No FAO facts collected")
        return None
    
    result = all_facts[0]
    for df in all_facts[1:]:
        result = result.union(df)
    
    # Map country name to ISO3
    from pyspark.sql.functions import create_map, lit
    mapping_expr = create_map([lit(x) for x in sum(name_to_iso.items(), ())])
    result = result.withColumn("country_code", mapping_expr.getItem(col("country_name"))) \
                   .drop("country_name") \
                   .filter(col("country_code").isNotNull())
    
    logger.info(f"FAO processed: {result.count():,} rows")
    return result