from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull
import os
import sys
import logging
from datetime import datetime
import glob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def create_optimized_spark_session():
    return SparkSession.builder \
        .appName("FAODataExploration") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

spark = create_optimized_spark_session()
logger.info("Spark session initialized")

DATA_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/fao/"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "outputExplore")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "fao_profile_output.txt")

def find_data_files(base_dir):
    pattern = os.path.join(base_dir, "*", "*_E_All_Data_(Normalized).csv")
    files = glob.glob(pattern)
    # Sắp xếp để có thứ tự nhất quán
    files.sort()
    return files

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
            logger.info(f"Successfully read file with encoding: {encoding}")
            return df
        except Exception as e:
            logger.debug(f"Failed with encoding {encoding}: {str(e)[:50]}")
            continue
    logger.error(f"Failed to read file with all encodings: {file_path}")
    return None

def analyze_null_values(df, dataset_name):
    print(f"\nNULL MISSING VALUE ANALYSIS: {dataset_name}")
    print("-" * 60)
    
    try:
        row_count = df.count()
        if row_count == 0:
            print("Dataset is empty, cannot analyze nulls.")
            return
            
        null_stats = []
        for column in df.columns:
            null_count = df.where(
                col(column).isNull() | isnan(col(column))
            ).count()
            null_percent = (null_count / row_count * 100) if row_count > 0 else 0
            null_stats.append({
                "column": column,
                "null_count": null_count,
                "null_percent": null_percent
            })
            
        null_stats.sort(key=lambda x: x["null_percent"], reverse=True)
        
        print("All Columns Missing Values:")
        print(f"{'Column':<35} {'Null Count':>15} {'Percentage':>12}")
        for stat in null_stats:
            print(f"{stat['column']:<35} {stat['null_count']:>15,} {stat['null_percent']:>11.2f}%")
            
    except Exception as e:
        logger.error(f"Error in null analysis: {e}")

def analyze_data_types(df, dataset_name):
    print("\nData Type Distribution:")
    print("-" * 60)
    dtype_dist = {}
    for _, dtype in df.dtypes:
        dtype_dist[str(dtype)] = dtype_dist.get(str(dtype), 0) + 1
    for dtype, count in sorted(dtype_dist.items(), key=lambda x: x[1], reverse=True):
        print(f"{dtype:<15}: {count:>3} columns")

def generate_full_profile(df, dataset_name):
    print(f"\n{'='*80}")
    print(f"FULL DATA PROFILE: {dataset_name}")
    print(f"{'='*80}")
    
    try:
        row_count = df.count()
        col_count = len(df.columns)
        
        print("BASIC STATISTICS:")
        print(f"Total Rows: {row_count:,}")
        print(f"Total Columns: {col_count}")
        
        print("\nSCHEMA:")
        for name, dtype in df.dtypes:
            print(f"{name:<40} : {dtype}")
            
        analyze_data_types(df, dataset_name)
        
        print("\nSAMPLE DATA (first 5 rows):")
        df.show(5, truncate=50, vertical=False)
        
        analyze_null_values(df, dataset_name)
        
        # Cột Value thường là cột số chính
        if "Value" in df.columns:
            print("\nNUMERIC STATISTICS (Value column):")
            df.select("Value").describe().show()
        else:
            # Tìm các cột số để thống kê
            numeric_cols = [name for name, dtype in df.dtypes if dtype in ("int", "double", "float", "bigint", "long")]
            if numeric_cols:
                print(f"\nNUMERIC STATISTICS (first 5 numeric columns):")
                df.select(numeric_cols[:5]).describe().show()
            else:
                print("\nNo numeric columns found in this dataset")
                
        # Thêm thống kê về Flag nếu có
        if "Flag" in df.columns:
            print("\nFLAG DISTRIBUTION (top 10):")
            flag_counts = df.groupBy("Flag").count().orderBy(col("count").desc())
            flag_counts.show(10, truncate=False)
            
    except Exception as e:
        logger.error(f"Error generating profile: {e}")

def main():
    logger.info(f"Starting FAO data exploration from: {DATA_DIR}")
    
    data_files = find_data_files(DATA_DIR)
    if not data_files:
        logger.error(f"No data files found in {DATA_DIR}")
        spark.stop()
        return
    
    logger.info(f"Found {len(data_files)} data files to process")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        original_stdout = sys.stdout
        sys.stdout = f
        
        print(f"REPORT GENERATED ON: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print(f"Source directory: {DATA_DIR}")
        print(f"Total datasets found: {len(data_files)}\n")
        
        successful = 0
        failed = 0
        
        for file_path in data_files:
            # Lấy tên thư mục cha làm tên dataset
            dir_name = os.path.basename(os.path.dirname(file_path))
            file_name = os.path.basename(file_path)
            dataset_name = f"{dir_name} / {file_name}"
            
            logger.info(f"Processing: {dataset_name}")
            
            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                failed += 1
                continue
                
            try:
                df = read_csv_robust(file_path)
                if df is None:
                    failed += 1
                    continue
                    
                df.cache()
                generate_full_profile(df, dataset_name)
                successful += 1
                df.unpersist()
                
            except Exception as e:
                logger.error(f"Error processing {dataset_name}: {e}")
                failed += 1
                
        print(f"\n{'='*80}")
        print("EXPLORATION COMPLETED")
        print(f"Datasets processed successfully: {successful}")
        print(f"Datasets failed: {failed}")
        
        sys.stdout = original_stdout
        
    logger.info(f"Data profiling completed. Results saved to: {OUTPUT_FILE}")
    spark.stop()
    logger.info("Spark session stopped")

if __name__ == "__main__":
    main()