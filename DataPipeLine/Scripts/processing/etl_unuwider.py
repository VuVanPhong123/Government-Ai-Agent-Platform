import os
import re
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, concat
from pyspark.sql.types import StructType, StructField, StringType

logger = logging.getLogger(__name__)

def process_unuwider(spark, selected_countries, dim_indicator=None):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/unuwider(tax)/"
    file_path = None
    for f in os.listdir(data_dir):
        if f.startswith("UNUWIDERGRD_2025") and f.endswith(".xlsx"):
            file_path = os.path.join(data_dir, f)
            break
            
    if not file_path:
        logger.error("UNU-WIDER GRD file not found.")
        return None
        
    logger.info("Processing UNU-WIDER Merged sheet...")
    
    # Đọc với 3 dòng Header
    df_raw = pd.read_excel(file_path, sheet_name="Merged", header=[0, 1, 2])
    df_raw = df_raw.dropna(axis=1, how='all').dropna(axis=0, how='all')
    
    new_cols = []
    if isinstance(df_raw.columns, pd.MultiIndex):
        for col_tuple in df_raw.columns:
            parts = []
            for val in col_tuple:
                if pd.isna(val) or str(val).strip() == '':
                    continue
                clean_val = str(val).strip()
                if re.search(r'(?i)unnamed', clean_val):
                    continue
                clean_val = re.sub(r'[^\w\s]', '', clean_val)
                clean_val = re.sub(r'\s+', '_', clean_val).strip('_')
                if clean_val and clean_val not in parts:
                    parts.append(clean_val)
            combined = "_".join(parts) if parts else "Column"
            new_cols.append(combined)
    else:
        new_cols = [str(c) for c in df_raw.columns]
        
    final_cols = []
    seen = {}
    for c in new_cols:
        if c in seen:
            seen[c] += 1
            final_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final_cols.append(c)
            
    df_raw.columns = final_cols
    df_raw = df_raw.astype(str)
    df = spark.createDataFrame(df_raw)
    
    if "ISO" in df.columns:
        df = df.filter(col("ISO").isin(list(selected_countries.keys())))
    else:
        logger.error("Column 'ISO' not found in Merged sheet.")
        return None
    
    metadata_cols = [
        "Identifier", "General_1_if_General", "Source", "Country", "Reg", "Inc",
        "Year", "ISO", "General_Notes",
        "Caution1_Accuracy_Quality_or_Comparability_of_data_is_questionable",
        "Caution_1_Notes",
        "Caution2_Unexcluded_resource_revenues_taxes_are_significant_but_cannot_be_isolated_from_total_revenues_taxes",
        "Caution3_Unexcluded_Resource_RevenuesTaxes_are_Marginal_but_NonNegligible_and_cannot_be_isolated_from_total_revenue_taxes",
        "Resource_Revenue_Notes", "Caution_4_Inconsistencies_with_Social_Contributions",
        "Social_contributions_notes", "GDP_LCU_mn"
    ]
    
    indicator_cols = [c for c in df.columns if c not in metadata_cols and c not in ["Year", "General_1_if_General"]]
    
    stack_expr = f"stack({len(indicator_cols)}, " + ", ".join([f"'{c}', `{c}`" for c in indicator_cols]) + ") as (indicator_code, value)"
    
    df_long = df.select(
        col("ISO").alias("country_code"),
        col("Year").alias("year"),
        expr(stack_expr)
    )
    
    df_long = df_long.filter(
        col("value").isNotNull() & 
        (col("value") != "nan") & 
        (col("value") != "None") & 
        (col("value") != "")
    )
    
    result = df_long.select(
        "country_code",
        col("year").cast("int"),
        "indicator_code",
        col("value").cast("double"),
        lit(3).alias("source_priority"),
        concat(lit("UNUWIDER_"), col("indicator_code")).alias("source_specific")
    ).filter((col("value").isNotNull()) & (col("year") >= 1990) & (col("year") <= 2024))
    
    logger.info(f"UNU-WIDER processed: {result.count():,} rows")
    return result

def get_unuwider_country_metadata(spark):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/unuwider(tax)/"
    file_path = None
    for f in os.listdir(data_dir):
        if f.startswith("UNUWIDERGRD_2025") and f.endswith(".xlsx"):
            file_path = os.path.join(data_dir, f)
            break
            
    if not file_path:
        logger.warning("UNU-WIDER GRD file not found for country metadata.")
        return None
    
    logger.info("Processing UNU-WIDER Country Metadata sheet...")
    pdf = pd.read_excel(file_path, sheet_name="Country Metadata", header=1)
    pdf = pdf.dropna(axis=1, how='all').dropna(axis=0, how='all')
    pdf.columns = [re.sub(r'\s+', ' ', str(c)).strip() for c in pdf.columns]
    pdf = pdf.astype(str)
    df = spark.createDataFrame(pdf)
    
    # LOẠI BỎ country_name và currency_unit khỏi mapping
    mapping = {
        "iso": "country_code",
        "general=1": "government_level_unuwider",
        "system of national accounts": "sna_system",
        "base year": "gdp_base_year",
        "accounting practice": "accounting_practice",
        "finance statistics manual": "gfsm_version",
        "subsectors coverage": "subsectors_coverage"
    }
    
    mapped_new_names = set()
    for old_name, new_name in mapping.items():
        matching_cols = [c for c in df.columns if old_name.lower() in c.lower() and c not in mapped_new_names]
        if matching_cols:
            target_col = matching_cols[0]
            df = df.withColumnRenamed(target_col, new_name)
            mapped_new_names.add(new_name)
    
    keep_cols = list(mapping.values())
    df = df.select(*[col(c) for c in keep_cols if c in df.columns])
    
    logger.info(f"UNU-WIDER country metadata loaded: {df.count()} rows")
    return df

def get_unuwider_indicator_metadata(spark):
    schema = StructType([
        StructField("meta_code", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("indicator_name", StringType(), True),
        StructField("long_definition", StringType(), True),
        StructField("unit_of_measure", StringType(), True),
        StructField("periodicity", StringType(), True),
        StructField("statistical_concept", StringType(), True),
        StructField("source", StringType(), True)
    ])
    data = [
        ("Total_Revenue_Including_Grants_Inc_SC", "Government Revenue", "Total revenue including grants and social contributions", "Total government revenue = taxes + non-tax revenue + grants + social contributions.", "% of GDP", "Annual", "Recommended aggregate.", "UNU-WIDER GRD"),
        ("Total_Revenue_Including_Grants_Ex_SC", "Government Revenue", "Total revenue including grants, excluding social contributions", "Total government revenue = taxes + non-tax revenue + grants.", "% of GDP", "Annual", None, "UNU-WIDER GRD"),
        ("Total_Revenue_Excluding_Grants_Inc_SC", "Government Revenue", "Total revenue excluding grants, including social contributions", "Total government revenue = taxes + non-tax revenue + social contributions.", "% of GDP", "Annual", "Recommended aggregate.", "UNU-WIDER GRD"),
        ("Total_Revenue_Excluding_Grants_Ex_SC", "Government Revenue", "Total revenue excluding grants and social contributions", "Total government revenue = taxes + non-tax revenue.", "% of GDP", "Annual", None, "UNU-WIDER GRD"),
        ("Total_Resource_Revenue", "Natural Resource Revenue", "Total resource revenue", "Total government revenue from natural resources (tax + non-tax).", "% of GDP", "Annual", "Includes oil, gas, mining.", "UNU-WIDER GRD"),
        ("Total_NonResource_Revenue_inc_SC", "Government Revenue", "Total non-resource revenue including social contributions", "Total revenue minus total resource revenue.", "% of GDP", "Annual", "Includes taxes, non-tax, social contributions from non-resource sectors.", "UNU-WIDER GRD"),
        ("Taxes_Including_SC", "Government Tax Revenue", "Total tax revenue including social contributions", "Total tax revenue = taxes + social contributions.", "% of GDP", "Annual", "Recommended tax aggregate.", "UNU-WIDER GRD"),
        ("Taxes_Excluding_SC", "Government Tax Revenue", "Total tax revenue excluding social contributions", "Total tax revenue = taxes (compulsory unrequited payments).", "% of GDP", "Annual", "Standard definition.", "UNU-WIDER GRD"),
        ("Resource_Taxes", "Natural Resource Revenue", "Resource tax revenue", "Taxes levied on natural resource extraction (e.g., corporate tax on oil companies).", "% of GDP", "Annual", "Part of total resource revenue.", "UNU-WIDER GRD"),
        ("NonResource_Tax_Including_SC", "Government Tax Revenue", "Non-resource tax revenue including social contributions", "Total tax revenue minus resource taxes.", "% of GDP", "Annual", "Useful for non-resource fiscal analysis.", "UNU-WIDER GRD"),
        ("NonResource_Tax_Excluding_SC", "Government Tax Revenue", "Non-resource tax revenue excluding social contributions", "Total taxes minus resource taxes.", "% of GDP", "Annual", "Standard definition for non-resource tax.", "UNU-WIDER GRD"),
        ("Direct_Taxes_Including_SC_Inc_Resource", "Direct Taxation", "Direct taxes including social contributions and resource revenue", "Sum of taxes on income/profits, payroll, property + social contributions. Includes resource component.", "% of GDP", "Annual", "Broad direct tax measure.", "UNU-WIDER GRD"),
        ("Direct_Taxes_Including_SC_Ex_Resource", "Direct Taxation", "Direct taxes including social contributions, excluding resource revenue", "Direct taxes (incl. SC) minus resource-related direct taxes.", "% of GDP", "Annual", "Better for cross-country comparison of non-resource direct taxation.", "UNU-WIDER GRD"),
        ("Direct_Taxes_Excluding_SC_Inc_Resource", "Direct Taxation", "Direct taxes excluding social contributions, including resource revenue", "Direct taxes (excl. SC) – includes resource component.", "% of GDP", "Annual", "Standard direct tax definition.", "UNU-WIDER GRD"),
        ("Direct_Taxes_Excluding_SC_Ex_Resource", "Direct Taxation", "Direct taxes excluding social contributions and resource revenue", "Direct taxes (excl. SC) minus resource-related direct taxes.", "% of GDP", "Annual", "Cleanest measure of non-resource direct taxes.", "UNU-WIDER GRD"),
        ("Taxes_on_Income_Profits_Capital_Gains_Total", "Direct Taxation", "Taxes on income, profits, capital gains (TIPCG)", "Total TIPCG, exclusive of social contributions.", "% of GDP", "Annual", "Key direct tax component.", "UNU-WIDER GRD"),
        ("Taxes_on_Income_Profits_Capital_Gains_Resource_Component", "Natural Resource Revenue", "Resource component of TIPCG", "TIPCG from natural resource sectors (mainly corporate tax).", "% of GDP", "Annual", "May be underreported. See country notes.", "UNU-WIDER GRD"),
        ("Taxes_on_Income_Profits_Capital_Gains_NonResource_Component", "Direct Taxation", "Non-resource component of TIPCG", "TIPCG minus resource component.", "% of GDP", "Annual", "For non-resource sector analysis.", "UNU-WIDER GRD"),
        ("Taxes_on_Income_Profits_Capital_Gains_ow_PIT", "Direct Taxation", "Personal income tax (PIT)", "Taxes on income of individuals.", "% of GDP", "Annual", "Subcomponent of TIPCG.", "UNU-WIDER GRD"),
        ("Taxes_on_Income_Profits_Capital_Gains_ow_CIT_Total", "Direct Taxation", "Corporate income tax (CIT) total", "Taxes on income of corporations.", "% of GDP", "Annual", "Subcomponent of TIPCG.", "UNU-WIDER GRD"),
        ("Taxes_on_Income_Profits_Capital_Gains_ow_CIT_Resource_component", "Natural Resource Revenue", "Resource component of CIT", "CIT paid by resource extraction companies.", "% of GDP", "Annual", "Part of resource taxes.", "UNU-WIDER GRD"),
        ("Taxes_on_Income_Profits_Capital_Gains_ow_CIT_Non_Resource_component", "Direct Taxation", "Non-resource CIT", "CIT from non-resource sectors.", "% of GDP", "Annual", "For non-resource corporate taxation.", "UNU-WIDER GRD"),
        ("Taxes_on_Payroll_Workforce", "Direct Taxation", "Payroll taxes", "Taxes on payroll and workforce (excl. social contributions).", "% of GDP", "Annual", "Often conflated with SC in some countries.", "UNU-WIDER GRD"),
        ("Property_Taxes", "Direct Taxation", "Property taxes", "Recurrent and non-recurrent taxes on property.", "% of GDP", "Annual", "Excludes taxes on financial/capital transactions (reclassified to GST for OECD).", "UNU-WIDER GRD"),
        ("Indirect_Taxes_Total", "Indirect Taxation", "Indirect taxes total", "Sum of taxes on goods & services, international trade, and other taxes.", "% of GDP", "Annual", "Broad indirect tax aggregate.", "UNU-WIDER GRD"),
        ("Indirect_Taxes_Resource_Component", "Natural Resource Revenue", "Resource component of indirect taxes", "Indirect taxes from resource sectors (e.g., export taxes on minerals).", "% of GDP", "Annual", "Often small or zero.", "UNU-WIDER GRD"),
        ("Indirect_Taxes_NonResource_Component", "Indirect Taxation", "Non-resource indirect taxes", "Indirect taxes minus resource component.", "% of GDP", "Annual", "For non-resource sector analysis.", "UNU-WIDER GRD"),
        ("Taxes_on_Goods_and_Services_Total", "Indirect Taxation", "Taxes on goods and services (GST)", "VAT, sales tax, excises, etc.", "% of GDP", "Annual", "Major indirect tax category.", "UNU-WIDER GRD"),
        ("Taxes_on_Goods_and_Services_ow_General_Sales_VAT_Turnover_TFCT", "Indirect Taxation", "General sales / VAT / turnover / TFCT", "Broad-based consumption taxes, including VAT and sales tax.", "% of GDP", "Annual", "For OECD countries, TFCT is reclassified here (from property tax).", "UNU-WIDER GRD"),
        ("Taxes_on_Goods_and_Services_VAT", "Indirect Taxation", "Value-added tax (VAT)", "VAT revenue.", "% of GDP", "Annual", "Subcomponent of general sales taxes.", "UNU-WIDER GRD"),
        ("Taxes_on_Goods_and_Services_ow_Excises", "Indirect Taxation", "Excise taxes", "Excises on specific goods (alcohol, tobacco, fuel).", "% of GDP", "Annual", "Subcomponent of GST.", "UNU-WIDER GRD"),
        ("Taxes_on_International_Trade_Total", "Indirect Taxation", "Taxes on international trade", "Import and export duties, other trade taxes.", "% of GDP", "Annual", "Often includes development components (e.g., SACU).", "UNU-WIDER GRD"),
        ("Taxes_on_International_Trade_ow_Import", "Indirect Taxation", "Import duties", "Taxes on imports.", "% of GDP", "Annual", "Subcomponent of trade taxes.", "UNU-WIDER GRD"),
        ("Taxes_on_International_Trade_ow_Export", "Indirect Taxation", "Export duties", "Taxes on exports.", "% of GDP", "Annual", "Often resource-related in developing countries.", "UNU-WIDER GRD"),
        ("Other_Taxes", "Indirect Taxation", "Other taxes", "Residual tax revenue not classified elsewhere (e.g., stamp duties).", "% of GDP", "Annual", "Use with caution.", "UNU-WIDER GRD"),
        ("NonTax_Revenue_Total", "Non-Tax Revenue", "Non-tax revenue total", "Property income, sales of goods/services, fines, fees, etc.", "% of GDP", "Annual", "Excludes grants, taxes, social contributions.", "UNU-WIDER GRD"),
        ("NonTax_Revenue_Resource_Component", "Natural Resource Revenue", "Resource component of non-tax revenue", "Resource-related non-tax revenue (e.g., royalties, licenses).", "% of GDP", "Annual", "Part of total resource revenue.", "UNU-WIDER GRD"),
        ("NonTax_Revenue_NonResource_Component", "Non-Tax Revenue", "Non-resource non-tax revenue", "Non-tax revenue from non-resource activities.", "% of GDP", "Annual", "E.g., administrative fees, property income.", "UNU-WIDER GRD"),
        ("Social_Contributions", "Social Security", "Social contributions", "Compulsory/voluntary contributions to social security schemes.", "% of GDP", "Annual", "Includes employer, employee, self-employed contributions.", "UNU-WIDER GRD"),
        ("Grants", "External Transfers", "Grants received", "Transfers from foreign governments and international organizations.", "% of GDP", "Annual", "Excluded from many revenue aggregates.", "UNU-WIDER GRD")
    ]
    
    df = spark.createDataFrame(data, schema)
    return df.withColumn("source", lit("UNU-WIDER GRD"))