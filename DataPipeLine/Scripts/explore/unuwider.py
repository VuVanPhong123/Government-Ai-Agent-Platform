# unuwider.py - improved version with multi-level header handling
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull
import os
import sys
import logging
import pandas as pd
from datetime import datetime
import shutil
import re

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

FILES_CONFIG = {
    "UNUWIDERGRD_2025_Central": "UNUWIDERGRD_2025_Central.xlsx",
    "UNUWIDERGRD_2025_General": "UNUWIDERGRD_2025_General.xlsx",
    "UNUWIDERGRD_2025": "UNUWIDERGRD_2025.xlsx"
}

def validate_file_exists(file_path):
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return False
    logger.info(f"File found: {file_path}")
    return True

def clean_column_name(col_name):
    """Làm sạch tên cột: loại bỏ ký tự đặc biệt, thay khoảng trắng bằng _"""
    if not col_name or str(col_name).strip() == '':
        return None
    clean = str(col_name).strip()
    clean = re.sub(r'[^\w\s]', '', clean)  # xóa ký tự đặc biệt
    clean = re.sub(r'\s+', '_', clean)     # thay khoảng trắng bằng _
    return clean

def read_multi_header_excel(file_path, sheet_name):
    """
    Đọc Excel với multi-level header, gộp các cột phân cấp.
    Trả về DataFrame với tên cột dạng 'Parent_Child' hoặc 'Child' nếu parent là NULL.
    """
    # Đọc toàn bộ sheet, lấy nhiều dòng đầu để phân tích header
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # Tìm dòng bắt đầu dữ liệu (dòng đầu tiên có nhiều giá trị không null)
    data_start = 0
    for i in range(min(50, len(df_raw))):
        non_null = df_raw.iloc[i].notna().sum()
        if non_null >= 3:  # có ít nhất 3 giá trị không null
            data_start = i
            break
    
    # Đọc lại với header ở dòng data_start (giả sử header ở trên)
    # Nhưng thực tế, các file UNU-WIDER có header ở dòng 1-3
    # Ta sẽ đọc với header=0,1,2 để lấy multi-level
    try:
        # Thử đọc với 2 dòng header
        df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=[0, 1])
        # Kiểm tra xem có nhiều cột Unnamed không
        unnamed_cols = [col for col in df_multi.columns.levels[0] if 'Unnamed' in str(col)]
        if len(unnamed_cols) > len(df_multi.columns.levels[0]) / 2:
            # Quá nhiều Unnamed, thử với 1 dòng header
            df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
            # Tạo tên cột phẳng
            new_cols = []
            for col in df_multi.columns:
                clean = clean_column_name(col)
                if clean is None or clean == '':
                    clean = f"Column_{len(new_cols)}"
                new_cols.append(clean)
            df_multi.columns = new_cols
        else:
            # Ghép các level header lại
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
        logger.warning(f"Multi-header read failed for {sheet_name}: {e}")
        # Fallback: đọc với header mặc định
        df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
        new_cols = []
        for col in df_multi.columns:
            clean = clean_column_name(col)
            if clean is None or clean == '' or 'Unnamed' in clean:
                clean = f"Column_{len(new_cols)}"
            new_cols.append(clean)
        df_multi.columns = new_cols
    
    # Loại bỏ các cột toàn NaN
    df_multi = df_multi.dropna(axis=1, how='all')
    # Loại bỏ các dòng toàn NaN
    df_multi = df_multi.dropna(axis=0, how='all')
    
    return df_multi

def excel_file_to_csvs(excel_path, temp_dir, base_name):
    """Chuyển đổi Excel thành CSV, xử lý multi-level header."""
    if not validate_file_exists(excel_path):
        return {}
    
    file_temp_dir = os.path.join(temp_dir, base_name)
    if os.path.exists(file_temp_dir):
        shutil.rmtree(file_temp_dir)
    os.makedirs(file_temp_dir)
    
    sheet_to_csv = {}
    try:
        # Lấy danh sách sheet names
        xls = pd.ExcelFile(excel_path, engine='openpyxl')
        sheet_names = xls.sheet_names
    except Exception:
        try:
            xls = pd.ExcelFile(excel_path, engine='xlrd')
            sheet_names = xls.sheet_names
        except Exception as e:
            logger.error(f"Cannot read Excel file {base_name}: {e}")
            return {}
    
    for sheet_name in sheet_names:
        logger.info(f"Reading sheet: {sheet_name} from {base_name}")
        try:
            df = read_multi_header_excel(excel_path, sheet_name)
            if df.empty:
                logger.warning(f"Sheet {sheet_name} is empty after processing.")
                continue
            
            safe_name = sheet_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
            csv_path = os.path.join(file_temp_dir, f"{safe_name}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8')
            full_key = f"{base_name}::{sheet_name}"
            sheet_to_csv[full_key] = csv_path
            logger.info(f"Saved {full_key} -> {csv_path} ({len(df)} rows, {len(df.columns)} cols)")
        except Exception as e:
            logger.error(f"Error processing sheet {sheet_name} in {base_name}: {e}")
    
    return sheet_to_csv

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
        for column in df.columns:
            null_count = df.where(col(column).isNull() | isnan(col(column))).count()
            null_percent = (null_count / row_count * 100) if row_count > 0 else 0
            null_stats.append({"column": column, "null_count": null_count, "null_percent": null_percent})
        null_stats.sort(key=lambda x: x["null_percent"], reverse=True)
        print("Top 15 Columns with Most Missing Values:")
        print(f"{'Column':<40} {'Null Count':>15} {'Percentage':>12}")
        for i, stat in enumerate(null_stats[:15], 1):
            print(f"{stat['column']:<40} {stat['null_count']:>15,} {stat['null_percent']:>11.2f}%")
        if len(null_stats) > 15:
            print(f"and {len(null_stats) - 15} more columns")
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
        print("\nSCHEMA (first 30 columns):")
        for i, (name, dtype) in enumerate(df.dtypes):
            if i >= 30:
                print(f"... and {col_count - 30} more columns")
                break
            print(f"{name:<50} : {dtype}")
        analyze_data_types(df, sheet_key)
        print("\nSAMPLE DATA (first 5 rows, first 10 columns):")
        df.select(df.columns[:10]).show(5, truncate=50, vertical=False)
        analyze_null_values(df, sheet_key)
        numeric_cols = [name for name, dtype in df.dtypes if dtype in ("int", "double", "float", "bigint", "long")]
        if numeric_cols:
            print("\nNUMERIC STATISTICS (first 5 numeric columns):")
            df.select(numeric_cols[:5]).describe().show()
        else:
            print("\nNo numeric columns found in this dataset")
    except Exception as e:
        logger.error(f"Error generating profile: {e}")

def main():
    logger.info(f"Starting UNU-WIDER data exploration from: {DATA_DIR}")
    
    if os.path.exists(TEMP_CSV_DIR):
        shutil.rmtree(TEMP_CSV_DIR)
    os.makedirs(TEMP_CSV_DIR)
    
    all_sheets = {}
    for base_name, filename in FILES_CONFIG.items():
        full_path = os.path.join(DATA_DIR, filename)
        sheets = excel_file_to_csvs(full_path, TEMP_CSV_DIR, base_name)
        all_sheets.update(sheets)
    
    if not all_sheets:
        logger.error("No sheets found or unable to read any Excel file.")
        spark.stop()
        sys.exit(1)
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        original_stdout = sys.stdout
        sys.stdout = f
        
        print(f"REPORT GENERATED ON: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print(f"Source directory: {DATA_DIR}")
        print(f"Total sheets across all files: {len(all_sheets)}\n")
        
        successful_sheets = 0
        failed_sheets = 0
        
        for sheet_key, csv_path in all_sheets.items():
            logger.info(f"Processing: {sheet_key}")
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
                logger.error(f"Error processing {sheet_key}: {e}")
                failed_sheets += 1
        
        print(f"\n{'='*80}")
        print("EXPLORATION COMPLETED")
        print(f"Sheets processed successfully: {successful_sheets}")
        print(f"Sheets failed or not found: {failed_sheets}")
        
        sys.stdout = original_stdout
    
    if os.path.exists(TEMP_CSV_DIR):
        shutil.rmtree(TEMP_CSV_DIR)
        logger.info("Removed temporary CSV directory")
    
    logger.info(f"Data profiling completed. Results saved to: {OUTPUT_FILE}")
    spark.stop()
    logger.info("Spark session stopped")

if __name__ == "__main__":
    main()