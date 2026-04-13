import os
import glob
import pandas as pd
import shutil
import re
import logging
from pyspark.sql.functions import col, lit, create_map
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

logger = logging.getLogger(__name__)

def clean_column_name(col_name):
    if not col_name or str(col_name).strip() == '':
        return None
    clean = str(col_name).strip()
    clean = re.sub(r'[^\w\s]', '', clean)
    clean = re.sub(r'\s+', '_', clean)
    return clean

def read_multi_header_excel(file_path, sheet_name):
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    # Tìm dòng bắt đầu dữ liệu
    data_start = 0
    for i in range(min(50, len(df_raw))):
        if df_raw.iloc[i].notna().sum() >= 3:
            data_start = i
            break
    try:
        df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=[0, 1])
        unnamed_cols = [col for col in df_multi.columns.levels[0] if 'Unnamed' in str(col)]
        if len(unnamed_cols) > len(df_multi.columns.levels[0]) / 2:
            df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
            new_cols = [clean_column_name(c) or f"col_{i}" for i, c in enumerate(df_multi.columns)]
            df_multi.columns = new_cols
        else:
            new_cols = []
            for level0, level1 in df_multi.columns:
                l0 = clean_column_name(level0) if pd.notna(level0) else ''
                l1 = clean_column_name(level1) if pd.notna(level1) else ''
                if l0 and l1 and l1 not in ['Unnamed', '']:
                    col_name = f"{l0}_{l1}"
                elif l1:
                    col_name = l1
                elif l0:
                    col_name = l0
                else:
                    col_name = f"col_{len(new_cols)}"
                new_cols.append(col_name)
            df_multi.columns = new_cols
    except Exception as e:
        logger.warning(f"Multi-header read failed: {e}")
        df_multi = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
        new_cols = [clean_column_name(c) or f"col_{i}" for i, c in enumerate(df_multi.columns)]
        df_multi.columns = new_cols
    return df_multi

def process_unuwider(spark, selected_countries, dim_indicator=None):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/unuwider(tax)/"
    temp_dir = os.path.join(os.path.dirname(data_dir), "temp_unuwider")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    excel_files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    all_facts = []

    name_to_iso = {v: k for k, v in selected_countries.items()}
    name_to_iso.update({
        "United States": "USA", "United Kingdom": "GBR", "Russia": "RUS",
        "Korea": "KOR", "Iran": "IRN", "Venezuela": "VEN", "Bolivia": "BOL",
        "Tanzania": "TZA", "Congo, Dem. Rep.": "COD", "Congo, Rep.": "COG",
        "Côte d'Ivoire": "CIV", "Egypt": "EGY", "Viet Nam": "VNM",
    })

    for excel_path in excel_files:
        base_name = os.path.splitext(os.path.basename(excel_path))[0]
        logger.info(f"Processing UNU-WIDER file: {base_name}")
        xls = pd.ExcelFile(excel_path)
        for sheet_name in xls.sheet_names:
            if "Info" in sheet_name or "Documentation" in sheet_name:
                continue
            logger.info(f"  Sheet: {sheet_name}")
            df_pd = read_multi_header_excel(excel_path, sheet_name)
            if df_pd.empty:
                continue

            # Tìm cột quốc gia
            country_col = None
            for c in df_pd.columns:
                if 'country' in c.lower() or 'iso' in c.lower():
                    country_col = c
                    break
            if country_col is None:
                continue

            # Lọc quốc gia
            selected_names = list(selected_countries.values()) + list(selected_countries.keys())
            df_pd = df_pd[df_pd[country_col].isin(selected_names)]
            if df_pd.empty:
                continue

            # Tìm cột năm
            year_col = None
            for c in df_pd.columns:
                if 'year' in c.lower():
                    year_col = c
                    break
            if year_col is None:
                continue

            # Chuyển đổi cột số sang float
            for c in df_pd.columns:
                if c not in [country_col, year_col]:
                    df_pd[c] = pd.to_numeric(df_pd[c], errors='coerce')

            # Tạo mapping country -> iso
            def get_iso(val):
                if val in selected_countries:
                    return val
                if val in name_to_iso:
                    return name_to_iso[val]
                return None

            for idx, row in df_pd.iterrows():
                country_val = row[country_col]
                iso = get_iso(country_val)
                if iso is None:
                    continue
                year_val = row[year_col]
                try:
                    year = int(float(year_val))
                except:
                    continue
                if year < 1995 or year > 2024:
                    continue
                # Duyệt tất cả các cột còn lại làm indicator
                for col_name in df_pd.columns:
                    if col_name in [country_col, year_col]:
                        continue
                    val = row[col_name]
                    if pd.isna(val):
                        continue
                    try:
                        val_float = float(val)
                    except:
                        continue
                    all_facts.append((
                        iso, year, col_name, val_float, 3,
                        f"UNU-WIDER_{base_name}_{sheet_name}_{col_name}"
                    ))

    if not all_facts:
        return None

    schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("indicator_code", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("source_priority", IntegerType(), True),
        StructField("source_specific", StringType(), True)
    ])
    result = spark.createDataFrame(all_facts, schema)
    logger.info(f"UNU-WIDER processed: {result.count():,} rows")
    return result