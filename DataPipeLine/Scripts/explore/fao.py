# fao.py
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
        .config("spark.sql.files.maxPartitionBytes", "256m") \
        .getOrCreate()

spark = create_optimized_spark_session()
logger.info("Spark session initialized")

DATA_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/fao/"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
OUTPUT_BASE_DIR = os.path.join(SCRIPT_DIR, "outputExplore", "fao")
os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

SUMMARY_FILE = os.path.join(OUTPUT_BASE_DIR, "fao_processing_summary.txt")

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
            logger.info(f"Successfully read {file_path} with encoding: {encoding}")
            return df
        except Exception as e:
            logger.debug(f"Failed with encoding {encoding}: {str(e)[:50]}")
            continue
    logger.error(f"Failed to read file: {file_path}")
    return None

def analyze_null_values(df, file_name):
    print(f"\nNULL MISSING VALUE ANALYSIS: {file_name}")
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

def generate_full_profile(df, file_name, output_file_path):
    with open(output_file_path, "w", encoding="utf-8") as f:
        original_stdout = sys.stdout
        sys.stdout = f
        print(f"PROFILE FOR: {file_name}")
        print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
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
                print("\nNUMERIC STATISTICS (first 5 numeric columns):")
                df.select(numeric_cols[:5]).describe().show()
            else:
                print("\nNo numeric columns found in this dataset")
        except Exception as e:
            print(f"Error generating profile: {e}")
        sys.stdout = original_stdout

def main():
    logger.info(f"Starting FAO data exploration from: {DATA_DIR}")
    csv_files = glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
    logger.info(f"Found {len(csv_files)} CSV files")
    summary_lines = [
        f"FAO Data Exploration Summary",
        f"Run on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Source directory: {DATA_DIR}",
        f"Total CSV files found: {len(csv_files)}",
        ""
    ]
    processed, failed = 0, 0
    for csv_path in csv_files:
        rel_path = os.path.relpath(csv_path, DATA_DIR)
        output_rel_path = os.path.splitext(rel_path)[0] + ".txt"
        output_full_path = os.path.join(OUTPUT_BASE_DIR, output_rel_path)
        os.makedirs(os.path.dirname(output_full_path), exist_ok=True)
        logger.info(f"Processing: {rel_path}")
        try:
            df = read_csv_robust(csv_path)
            if df is None:
                failed += 1
                summary_lines.append(f"FAILED: {rel_path} - Cannot read CSV")
                continue
            df.cache()
            generate_full_profile(df, rel_path, output_full_path)
            df.unpersist()
            processed += 1
            summary_lines.append(f"SUCCESS: {rel_path} -> {output_full_path}")
            logger.info(f"Profile saved to: {output_full_path}")
        except Exception as e:
            failed += 1
            error_msg = f"Error processing {rel_path}: {str(e)}"
            logger.error(error_msg)
            summary_lines.append(f"ERROR: {rel_path} - {str(e)}")
    summary_lines.extend(["", f"Processing completed. Successful: {processed}, Failed: {failed}"])
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))
    logger.info(f"Summary saved to: {SUMMARY_FILE}")
    logger.info(f"FAO data exploration completed. Processed {processed} files, failed {failed}")
    spark.stop()
    logger.info("Spark session stopped")

if __name__ == "__main__":
    main()