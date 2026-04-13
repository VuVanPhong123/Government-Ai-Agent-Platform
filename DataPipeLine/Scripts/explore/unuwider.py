from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull
import os
import sys
import logging
import pandas as pd
from datetime import datetime
import shutil
import re
import glob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def create_optimized_spark_session():
    return SparkSession.builder \
        .appName("UNUWIDERDataExploration") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

spark = create_optimized_spark_session()
logger.info("Spark session initialized")

DATA_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/unuwider(tax)/"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "outputExplore")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "unuwider_profile_output.txt")
TEMP_CSV_DIR = os.path.join(SCRIPT_DIR, "temp_unuwider_csv")

def clean_column_name(col_name):
    if pd.isna(col_name) or str(col_name).strip() == '':
        return ""
    
    clean = str(col_name).strip()
    
    if re.search(r'(?i)unnamed', clean):
        return ""
        
    clean = re.sub(r'[^\w\s]', '', clean)
    clean = re.sub(r'\s+', '_', clean)
    clean = clean.strip('_')
    
    return clean

def read_excel_smart_headers(file_path, sheet_name):
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=[0, 1, 2])
    
    df_raw = df_raw.dropna(axis=1, how='all').dropna(axis=0, how='all')
    
    new_cols = []
    for col_tuple in df_raw.columns:
        parts = []
        for val in col_tuple:
            clean_val = clean_column_name(val)
            if clean_val and clean_val not in parts:
                parts.append(clean_val)
        
        combined_name = "_".join(parts) if parts else "Column"
        new_cols.append(combined_name)
        
    final_cols = []
    seen = {}
    for col in new_cols:
        if col in seen:
            seen[col] += 1
            final_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            final_cols.append(col)
            
    # Gán danh sách cột đã làm phẳng
    df_raw.columns = final_cols
    
    return df_raw

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
            logger.info(f"Successfully read CSV with encoding: {encoding}")
            return df
        except Exception as e:
            logger.debug(f"Failed with encoding {encoding}: {str(e)[:50]}")
            continue
    logger.error(f"Failed to read CSV: {file_path}")
    return None

def analyze_null_values(df, sheet_key):
    print(f"\nNULL MISSING VALUE ANALYSIS: {sheet_key}")
    print("-" * 60)
    try:
        row_count = df.count()
        if row_count == 0:
            print("Dataset is empty, cannot analyze nulls.")
            return
            
        null_stats = []
        dtypes_dict = dict(df.dtypes)
        
        for column in df.columns:
            safe_col = f"`{column}`"
            dtype = dtypes_dict.get(column, "string")
            
            if dtype in ('double', 'float'):
                null_count = df.where(col(safe_col).isNull() | isnan(col(safe_col))).count()
            else:
                null_count = df.where(col(safe_col).isNull()).count()
                
            null_percent = (null_count / row_count * 100) if row_count > 0 else 0
            null_stats.append({"column": column, "null_count": null_count, "null_percent": null_percent})
            
        null_stats.sort(key=lambda x: x["null_percent"], reverse=True)
        
        print("All Columns Missing Values:")
        print(f"{'Column':<60} {'Null Count':>15} {'Percentage':>12}")
        for stat in null_stats:
            print(f"{stat['column']:<60} {stat['null_count']:>15,} {stat['null_percent']:>11.2f}%")
            
    except Exception as e:
        logger.error(f"Error in null analysis: {e}")

def analyze_data_types(df, sheet_key):
    print("\nData Type Distribution:")
    print("-" * 60)
    dtype_dist = {}
    for _, dtype in df.dtypes:
        dtype_dist[str(dtype)] = dtype_dist.get(str(dtype), 0) + 1
    for dtype, count in sorted(dtype_dist.items(), key=lambda x: x[1], reverse=True):
        print(f"{dtype:<15}: {count:>3} columns")

def generate_full_profile(df, sheet_key):
    print(f"\n{'='*80}")
    print(f"FULL DATA PROFILE: {sheet_key}")
    print(f"{'='*80}")
    try:
        row_count = df.count()
        col_count = len(df.columns)
        print("BASIC STATISTICS:")
        print(f"Total Rows: {row_count:,}")
        print(f"Total Columns: {col_count}")
        
        print("\nSCHEMA (ALL COLUMNS):")
        for name, dtype in df.dtypes:
            print(f"{name:<60} : {dtype}")
            
        analyze_data_types(df, sheet_key)
        
        print("\nSAMPLE DATA (first 5 rows, ALL columns):")
        df.show(5, truncate=50, vertical=False)
        
        analyze_null_values(df, sheet_key)
        
        numeric_cols = [name for name, dtype in df.dtypes if dtype in ("int", "double", "float", "bigint", "long")]
        if numeric_cols:
            print("\nNUMERIC STATISTICS (ALL numeric columns):")
            safe_numeric_cols = [f"`{c}`" for c in numeric_cols]
            df.select(*safe_numeric_cols).describe().show()
        else:
            print("\nNo numeric columns found in this dataset")
    except Exception as e:
        logger.error(f"Error generating profile: {e}")

def main():
    if not os.path.exists(TEMP_CSV_DIR):
        os.makedirs(TEMP_CSV_DIR)
        
    all_files = glob.glob(os.path.join(DATA_DIR, "**", "*.xlsx"), recursive=True) + \
                glob.glob(os.path.join(DATA_DIR, "**", "*.xls"), recursive=True) + \
                glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
                
    target_files = [f for f in all_files if "UNUWIDERGRD_2025" in os.path.basename(f) and "Central" not in os.path.basename(f) and "General" not in os.path.basename(f)]
    
    logger.info(f"Found {len(target_files)} target data files matching 'UNUWIDERGRD_2025'")
    
    all_sheets = {}
    
    for file_path in target_files:
        base_name = os.path.basename(file_path)
        logger.info(f"Reading file: {base_name}")
        
        if file_path.endswith(('.xlsx', '.xls')):
            try:
                xls = pd.ExcelFile(file_path)
                target_sheets = [s for s in xls.sheet_names if s.strip().lower() in ['merged', 'country metadata', 'countrymetadata']]
                
                for sheet_name in target_sheets:
                    logger.info(f"  Extracting sheet: {sheet_name}")
                    try:
                        df_sheet = read_excel_smart_headers(file_path, sheet_name)
                        safe_sheet_name = clean_column_name(sheet_name)
                        if not safe_sheet_name: safe_sheet_name = "Sheet"
                        
                        csv_name = f"{base_name.split('.')[0]}_{safe_sheet_name}.csv"
                        csv_path = os.path.join(TEMP_CSV_DIR, csv_name)
                        
                        df_sheet.to_csv(csv_path, index=False)
                        all_sheets[f"{base_name}::{sheet_name}"] = csv_path
                    except Exception as e:
                        logger.error(f"  Failed to read sheet {sheet_name}: {e}")
            except Exception as e:
                logger.error(f"Failed to open Excel file {file_path}: {e}")
        else:
            logger.info(f"Skipping CSV file (expecting only the main Excel file): {base_name}")
            
    if not all_sheets:
        logger.error("No valid sheets found to process. Exiting.")
        return

    output_file = os.path.join(OUTPUT_DIR, "unuwider_profile_output.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        original_stdout = sys.stdout
        sys.stdout = f
        
        print(f"REPORT GENERATED ON: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print(f"Source directory: {DATA_DIR}")
        print(f"Total target sheets processed: {len(all_sheets)}\n")
        
        successful_sheets = 0
        failed_sheets = 0
        
        for sheet_key, csv_path in all_sheets.items():
            logger.info(f"Profiling: {sheet_key}")
            try:
                df = read_csv_robust(csv_path)
                if df is None:
                    failed_sheets += 1
                    continue
                df.cache()
                generate_full_profile(df, sheet_key)
                successful_sheets += 1
                df.unpersist()
            except Exception as e:
                logger.error(f"Error profiling {sheet_key}: {e}")
                failed_sheets += 1
        
        print(f"\n{'='*80}")
        print("EXPLORATION COMPLETED")
        print(f"Sheets processed successfully: {successful_sheets}")
        print(f"Sheets failed or not found: {failed_sheets}")
        
        sys.stdout = original_stdout
    
    if os.path.exists(TEMP_CSV_DIR):
        shutil.rmtree(TEMP_CSV_DIR)
        logger.info("Removed temporary CSV files.")
        
    logger.info(f"Done! Profile saved to: {output_file}")

if __name__ == "__main__":
    main()