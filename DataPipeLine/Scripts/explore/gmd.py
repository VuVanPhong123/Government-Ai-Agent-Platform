from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull
import os
import sys
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def create_optimized_spark_session():
    return SparkSession.builder \
        .appName("GMDDataExploration") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

spark = create_optimized_spark_session()
logger.info("Spark session initialized")

DATA_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/gmd/"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "outputExplore")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "gmd_profile_output.txt")

FILES_CONFIG = {
    "GMD": "GMD.csv"
}

def validate_file_exists(file_path):
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return False
    logger.info(f"File found: {file_path}")
    return True

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

def analyze_null_values(df, file_name):
    print(f"\nNULL MISSING VALUE ANALYSIS: {file_name}")
    print("-" * 60)
    
    try:
        row_count = df.count()
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
        
        print("Top 15 Columns with Most Missing Values:")
        print(f"{'Column':<35} {'Null Count':>15} {'Percentage':>12}")
        
        for i, stat in enumerate(null_stats[:15], 1):
            print(f"{stat['column']:<35} {stat['null_count']:>15,} {stat['null_percent']:>11.2f}%")
            
        if len(null_stats) > 15:
            print(f"and {len(null_stats) - 15} more columns")
            
    except Exception as e:
        logger.error(f"Error in null analysis: {e}")

def analyze_data_types(df, file_name):
    print("\nData Type Distribution:")
    print("-" * 60)
    
    dtype_dist = {}
    for _, dtype in df.dtypes:
        dtype_dist[str(dtype)] = dtype_dist.get(str(dtype), 0) + 1
        
    for dtype, count in sorted(dtype_dist.items(), key=lambda x: x[1], reverse=True):
        print(f"{dtype:<15}: {count:>3} columns")

def generate_full_profile(df, file_name):
    print(f"\n{'='*80}")
    print(f"FULL DATA PROFILE: {file_name}")
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
            
        analyze_data_types(df, file_name)
        
        print("\nSAMPLE DATA:")
        df.show(5, truncate=50, vertical=False)
        
        analyze_null_values(df, file_name)
        
        numeric_cols = [name for name, dtype in df.dtypes if dtype in ("int", "double", "float", "bigint", "long")]
        
        if numeric_cols:
            print("\nNUMERIC STATISTICS:")
            df.select(numeric_cols[:5]).describe().show()
        else:
            print("\nNo numeric columns found in this dataset")
            
    except Exception as e:
        logger.error(f"Error generating profile: {e}")

def main():
    logger.info(f"Starting GMD data exploration from: {DATA_DIR}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        original_stdout = sys.stdout
        sys.stdout = f
        
        print(f"REPORT GENERATED ON: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        successful_files = 0
        failed_files = 0
        
        for name, filename in FILES_CONFIG.items():
            full_path = os.path.join(DATA_DIR, filename)
            
            if not validate_file_exists(full_path):
                failed_files += 1
                continue
                
            try:
                logger.info(f"Reading {name}")
                
                df = read_csv_robust(full_path)
                
                if df is None:
                    failed_files += 1
                    continue
                    
                df.cache()
                
                generate_full_profile(df, name)
                
                successful_files += 1
                
                df.unpersist()
                
            except Exception as e:
                logger.error(f"Error processing {name}: {e}")
                failed_files += 1
                
        print(f"\n{'='*80}")
        print("EXPLORATION COMPLETED")
        print(f"Files processed successfully: {successful_files}")
        print(f"Files failed or not found: {failed_files}")
        
        sys.stdout = original_stdout

    logger.info(f"Data profiling completed. Results saved to: {OUTPUT_FILE}")
    spark.stop()
    logger.info("Spark session stopped")

if __name__ == "__main__":
    main()