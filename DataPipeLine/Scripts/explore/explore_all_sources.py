import os
import sys
import logging
from datetime import datetime

# 1. Cấu hình Python (Né lỗi Microsoft Store)
python_path = sys.executable.replace("\\", "/")
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# 2. Cấu hình Hadoop (Cấp quyền ghi file Parquet)
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, isnull, lit, when, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("ExploreAllDataSources") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

spark = create_spark_session()
logger.info("Spark session initialized")

DATA_DIRS = {
    "FAO": "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/fao/",
    "GMD": "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/gmd/",
    "UNUWIDER": "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/unuwider(tax)/",
    "WDI": "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/worldBank/"
}
OUTPUT_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/Scripts/explore/outputExplore"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def read_csv_robust(file_path, encoding_list=None):
    if encoding_list is None:
        encoding_list = ["UTF-8", "ISO-8859-1", "latin1"]
    for encoding in encoding_list:
        try:
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("encoding", encoding) \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("multiLine", "true") \
                .csv(file_path)
            logger.info(f"Successfully read {file_path} with encoding {encoding}")
            return df
        except Exception as e:
            logger.debug(f"Failed with {encoding}: {str(e)[:50]}")
            continue
    logger.error(f"Cannot read {file_path}")
    return None

def clean_column_name(col_name):
    if not col_name or str(col_name).strip() == '':
        return None
    clean = str(col_name).strip()
    clean = re.sub(r'[^\w\s]', '', clean)
    clean = re.sub(r'\s+', '_', clean)
    return clean

def read_multi_header_excel(file_path, sheet_name):
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    data_start = 0
    for i in range(min(50, len(df_raw))):
        non_null = df_raw.iloc[i].notna().sum()
        if non_null >= 3:
            data_start = i
            break
    try:
        df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=[0, 1])
        unnamed_cols = [col for col in df_multi.columns.levels[0] if 'Unnamed' in str(col)]
        if len(unnamed_cols) > len(df_multi.columns.levels[0]) / 2:
            df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
            new_cols = []
            for col in df_multi.columns:
                clean = clean_column_name(col)
                if clean is None or clean == '':
                    clean = f"Column_{len(new_cols)}"
                new_cols.append(clean)
            df_multi.columns = new_cols
        else:
            new_cols = []
            for level0, level1 in df_multi.columns:
                level0_str = clean_column_name(level0) if pd.notna(level0) else ''
                level1_str = clean_column_name(level1) if pd.notna(level1) else ''
                if level0_str and level1_str and level1_str not in ['Unnamed', '']:
                    col_name = f"{level0_str}_{level1_str}"
                elif level1_str:
                    col_name = level1_str
                elif level0_str:
                    col_name = level0_str
                else:
                    col_name = f"Column_{len(new_cols)}"
                new_cols.append(col_name)
            df_multi.columns = new_cols
    except Exception as e:
        logger.warning(f"Multi-header failed for {sheet_name}: {e}")
        df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
        new_cols = []
        for col in df_multi.columns:
            clean = clean_column_name(col)
            if clean is None or clean == '' or 'Unnamed' in clean:
                clean = f"Column_{len(new_cols)}"
            new_cols.append(clean)
        df_multi.columns = new_cols
    df_multi = df_multi.dropna(axis=1, how='all')
    df_multi = df_multi.dropna(axis=0, how='all')
    return df_multi

def excel_to_csvs(excel_path, temp_dir, base_name):
    if not os.path.exists(excel_path):
        return {}
    file_temp_dir = os.path.join(temp_dir, base_name)
    if os.path.exists(file_temp_dir):
        shutil.rmtree(file_temp_dir)
    os.makedirs(file_temp_dir)
    sheet_to_csv = {}
    try:
        xls = pd.ExcelFile(excel_path, engine='openpyxl')
        sheet_names = xls.sheet_names
    except Exception:
        try:
            xls = pd.ExcelFile(excel_path, engine='xlrd')
            sheet_names = xls.sheet_names
        except Exception as e:
            logger.error(f"Cannot read {base_name}: {e}")
            return {}
    for sheet_name in sheet_names:
        try:
            df = read_multi_header_excel(excel_path, sheet_name)
            if df.empty:
                continue
            safe_name = sheet_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
            csv_path = os.path.join(file_temp_dir, f"{safe_name}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8')
            full_key = f"{base_name}::{sheet_name}"
            sheet_to_csv[full_key] = csv_path
        except Exception as e:
            logger.error(f"Error processing sheet {sheet_name}: {e}")
    return sheet_to_csv

# ------------------ SỬA HÀM analyze_fao ------------------
def analyze_fao():
    logger.info("Analyzing FAO data...")
    results = []
    data_dir = DATA_DIRS["FAO"]
    # Tìm tất cả file CSV (có thể nằm trong thư mục con)
    csv_files = glob.glob(os.path.join(data_dir, "**", "*.csv"), recursive=True)
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        logger.info(f"  Processing {file_name}")
        try:
            df = read_csv_robust(file_path)
            if df is None:
                continue

            # Lấy danh sách các cột
            columns = df.columns
            # Xác định các cột đặc trưng của FAO
            # Thông thường: Area, Item, Element, Year, Value, Unit, Flag
            # Nhưng một số file có Currency thay vì Item, hoặc thiếu Element

            # Trường hợp 1: Có cột Element -> mỗi Element là một chỉ số
            if "Element" in columns:
                elements = df.select("Element").distinct().collect()
                for row in elements:
                    element = row["Element"]
                    # Lấy Unit mẫu (chỉ lấy 1 giá trị đầu tiên)
                    sample_row = df.filter(col("Element") == element).limit(1).collect()
                    unit = sample_row[0]["Unit"] if sample_row and "Unit" in sample_row[0] else None
                    # Lấy thông tin về Item hoặc Currency (nếu có)
                    item_info = ""
                    if "Item" in columns:
                        num_items = df.filter(col("Element") == element).select("Item").distinct().count()
                        item_info = f", {num_items} distinct items"
                    elif "Currency" in columns:
                        num_currencies = df.filter(col("Element") == element).select("Currency").distinct().count()
                        item_info = f", {num_currencies} distinct currencies"
                    results.append({
                        "source": "FAO",
                        "file": file_name,
                        "indicator_code": element,
                        "indicator_name": element,
                        "unit": unit,
                        "data_format": "long (Element per row)",
                        "notes": f"File structure: {', '.join(columns[:5])}...{item_info}"
                    })
            else:
                # Trường hợp 2: Không có cột Element -> coi mỗi cột (không phải cột định danh) là một chỉ số
                exclude_patterns = ["Area", "Year", "Value", "Flag", "Unit", "Months", "ISO Currency Code", "Currency"]
                indicator_cols = [c for c in columns if not any(p in c for p in exclude_patterns)]
                for col_name in indicator_cols:
                    results.append({
                        "source": "FAO",
                        "file": file_name,
                        "indicator_code": col_name,
                        "indicator_name": col_name,
                        "unit": None,
                        "data_format": "wide",
                        "notes": f"Columns: {', '.join(columns[:5])}..."
                    })
            df.unpersist()
        except Exception as e:
            logger.error(f"Error analyzing {file_name}: {e}")
            continue
    return results
# -----------------------------------------------------

def analyze_gmd():
    logger.info("Analyzing GMD data...")
    results = []
    data_dir = DATA_DIRS["GMD"]
    file_path = os.path.join(data_dir, "GMD.csv")
    if not os.path.exists(file_path):
        logger.error(f"GMD file not found: {file_path}")
        return results
    df = read_csv_robust(file_path)
    if df is None:
        return results
    id_cols = ["countryname", "ISO3", "id", "year", "income_group"]
    indicator_cols = [c for c in df.columns if c not in id_cols]
    for col_name in indicator_cols:
        results.append({
            "source": "GMD",
            "file": "GMD.csv",
            "indicator_code": col_name,
            "indicator_name": col_name,
            "unit": None,
            "data_format": "wide (column per indicator)",
            "notes": ""
        })
    df.unpersist()
    return results

def analyze_unuwider():
    logger.info("Analyzing UNU-WIDER data...")
    results = []
    data_dir = DATA_DIRS["UNUWIDER"]
    temp_dir = os.path.join(OUTPUT_DIR, "temp_unuwider")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    excel_files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    for excel_path in excel_files:
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        sheets = excel_to_csvs(excel_path, temp_dir, base_name)
        for sheet_key, csv_path in sheets.items():
            df = read_csv_robust(csv_path)
            if df is None:
                continue
            id_patterns = ['identifier', 'country', 'iso', 'year', 'reg', 'inc', 'fiscal_year', 'downloadyr']
            indicator_cols = [c for c in df.columns if not any(p in c.lower() for p in id_patterns)]
            for col_name in indicator_cols:
                results.append({
                    "source": "UNU-WIDER",
                    "file": f"{base_name} - {sheet_key.split('::')[-1]}",
                    "indicator_code": col_name,
                    "indicator_name": col_name,
                    "unit": None,
                    "data_format": "wide",
                    "notes": f"Sheet type: {sheet_key}"
                })
            df.unpersist()
    shutil.rmtree(temp_dir)
    return results

def analyze_wdi():
    logger.info("Analyzing WDI data...")
    results = []
    data_dir = DATA_DIRS["WDI"]
    file_names = ["WDICountry.csv", "WDICountry-series.csv", "WDICSV.csv", 
                  "WDIFootnote.csv", "WDISeries.csv", "WDIseries-time.csv"]
    for file_name in file_names:
        file_path = os.path.join(data_dir, file_name)
        if not os.path.exists(file_path):
            logger.warning(f"WDI file not found: {file_path}")
            continue
        df = read_csv_robust(file_path)
        if df is None:
            continue
        if file_name == "WDICSV.csv":
            year_cols = [c for c in df.columns if c.isdigit() or (c.startswith('20') and len(c)==4)]
            for col_name in year_cols:
                results.append({
                    "source": "WDI",
                    "file": file_name,
                    "indicator_code": f"year_{col_name}",
                    "indicator_name": f"Value in {col_name}",
                    "unit": None,
                    "data_format": "wide (years as columns)",
                    "notes": "Indicator is actually time dimension, but treated as indicator for inventory"
                })
            if "Indicator Code" in df.columns:
                indicators = df.select("Indicator Code").distinct().collect()
                for row in indicators:
                    code = row["Indicator Code"]
                    results.append({
                        "source": "WDI",
                        "file": file_name,
                        "indicator_code": code,
                        "indicator_name": code,
                        "unit": None,
                        "data_format": "long (each row is country-year-indicator)",
                        "notes": "Actual indicator codes"
                    })
        else:
            id_cols = ["CountryCode", "SeriesCode", "Year", "DESCRIPTION", "Country Code", "Short Name"]
            indicator_cols = [c for c in df.columns if c not in id_cols]
            for col_name in indicator_cols:
                results.append({
                    "source": "WDI",
                    "file": file_name,
                    "indicator_code": col_name,
                    "indicator_name": col_name,
                    "unit": None,
                    "data_format": "wide",
                    "notes": ""
                })
        df.unpersist()
    return results

def main():
    all_indicators = []
    all_indicators.extend(analyze_fao())
    all_indicators.extend(analyze_gmd())
    all_indicators.extend(analyze_unuwider())
    all_indicators.extend(analyze_wdi())
    
    df_inventory = pd.DataFrame(all_indicators)
    df_inventory = df_inventory.drop_duplicates(subset=["source", "file", "indicator_code"])
    
    output_path = os.path.join(OUTPUT_DIR, "indicator_inventory.csv")
    df_inventory.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved indicator inventory to {output_path}")
    
    summary = df_inventory.groupby("source").size().reset_index(name="num_indicators")
    summary_path = os.path.join(OUTPUT_DIR, "source_summary.csv")
    summary.to_csv(summary_path, index=False)
    logger.info(f"Saved source summary to {summary_path}")
    
    df_inventory['name_lower'] = df_inventory['indicator_name'].str.lower()
    dup = df_inventory[df_inventory.duplicated(subset=['name_lower'], keep=False)]
    dup_path = os.path.join(OUTPUT_DIR, "duplicate_indicators.csv")
    dup.to_csv(dup_path, index=False)
    logger.info(f"Saved potential duplicates to {dup_path}")
    
    spark.stop()
    logger.info("Spark session stopped")

if __name__ == "__main__":
    main()