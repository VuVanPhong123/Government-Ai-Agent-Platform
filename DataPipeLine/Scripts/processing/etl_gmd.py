import os
import logging
from pyspark.sql.functions import col, lit, expr, concat
from pyspark.sql.types import StructType, StructField, StringType

logger = logging.getLogger(__name__)
def process_gmd(spark, selected_countries, dim_indicator=None):
    data_dir = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/raw/gmd/"
    file_path = os.path.join(data_dir, "GMD.csv")
    if not os.path.exists(file_path):
        logger.error(f"GMD file not found: {file_path}")
        return None
        
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    df = df.filter(col("ISO3").isin(list(selected_countries.keys())))
    df = df.filter((col("year") >= 1990) & (col("year") <= 2024))
    metadata_cols = ["countryname", "ISO3", "id", "year", "income_group"]
    indicator_cols = [c for c in df.columns if c not in metadata_cols]

    stack_expr = f"stack({len(indicator_cols)}, " + ", ".join([f"'{c}', CAST(`{c}` AS DOUBLE)" for c in indicator_cols]) + ") as (indicator_code, value)"

    df_long = df.select(
        col("ISO3").alias("country_code"),
        col("year"),
        expr(stack_expr)
    ).filter(col("value").isNotNull())

    result = df_long.select(
        "country_code",
        col("year").cast("int"),
        "indicator_code",
        col("value").cast("double"),
        lit(1).alias("source_priority"),
        concat(lit("GMD_"), col("indicator_code")).alias("source_specific")
    )

    logger.info(f"GMD processed: {result.count():,} rows")
    return result
def get_gmd_metadata(spark):
    schema = StructType([
        StructField("meta_code", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("indicator_name", StringType(), True),
        StructField("long_definition", StringType(), True),
        StructField("unit_of_measure", StringType(), True),
        StructField("periodicity", StringType(), True),
        StructField("statistical_concept", StringType(), True)
    ])
    
    data = [
        ("nGDP", "National Accounts", "Nominal GDP", "Nominal GDP measured in millions of local currency units.", "Millions of local currency units", "Annual", "Sum of consumption + investment + government spending + (exports - imports). Chain-linked."),
        ("nGDP_USD", "National Accounts", "Nominal GDP (USD)", "Nominal GDP converted to US dollars using average annual market exchange rate (USDfx).", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("rGDP", "National Accounts", "Real GDP", "Real GDP measured in millions of local currency units, chain-linked to a reference year.", "Millions of local currency units (chain-linked)", "Annual", "Chain-linked volume measure. Deflated using GDP deflator."),
        ("rGDP_pc", "National Accounts", "Real GDP per capita", "Real GDP per capita in local currency units.", "Local currency units (per capita)", "Annual", "Calculated as rGDP / population (pop)."),
        ("rGDP_USD", "National Accounts", "Real GDP (USD)", "Real GDP converted to US dollars using average annual market exchange rate (USDfx).", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("deflator", "National Accounts", "GDP deflator", "GDP deflator (price index).", "Index (2015 = 100)", "Annual", "Calculated as (nGDP / rGDP) * 100."),
        ("cons", "Consumption & Investment", "Total consumption", "Total consumption (household + government) in millions of local currency units.", "Millions of local currency units", "Annual", "Sum of hcons + gcons."),
        ("cons_GDP", "Consumption & Investment", "Total consumption (% GDP)", "Total consumption as percentage of nominal GDP.", "Percentage", "Annual", "(cons / nGDP) * 100."),
        ("cons_USD", "Consumption & Investment", "Total consumption (USD)", "Total consumption converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("hcons", "Consumption & Investment", "Household consumption", "Household consumption expenditure in millions of local currency units.", "Millions of local currency units", "Annual", "Spending by resident households on goods and services."),
        ("hcons_GDP", "Consumption & Investment", "Household consumption (% GDP)", "Household consumption as percentage of nominal GDP.", "Percentage", "Annual", "(hcons / nGDP) * 100."),
        ("hcons_USD", "Consumption & Investment", "Household consumption (USD)", "Household consumption converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("gcons", "Consumption & Investment", "Government consumption", "Government consumption expenditure in millions of local currency units.", "Millions of local currency units", "Annual", "Spending by government on goods and services."),
        ("gcons_GDP", "Consumption & Investment", "Government consumption (% GDP)", "Government consumption as percentage of nominal GDP.", "Percentage", "Annual", "(gcons / nGDP) * 100."),
        ("gcons_USD", "Consumption & Investment", "Government consumption (USD)", "Government consumption converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("inv", "Consumption & Investment", "Total investment", "Gross capital formation (total investment) in millions of local currency units.", "Millions of local currency units", "Annual", "Includes changes in inventories and gross fixed capital formation."),
        ("inv_GDP", "Consumption & Investment", "Total investment (% GDP)", "Gross capital formation as percentage of nominal GDP.", "Percentage", "Annual", "(inv / nGDP) * 100."),
        ("inv_USD", "Consumption & Investment", "Total investment (USD)", "Gross capital formation converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("finv", "Consumption & Investment", "Gross fixed capital formation", "Gross fixed capital formation (investment in fixed assets) in millions of local currency units.", "Millions of local currency units", "Annual", "Excludes changes in inventories."),
        ("finv_GDP", "Consumption & Investment", "Gross fixed capital formation (% GDP)", "Gross fixed capital formation as percentage of nominal GDP.", "Percentage", "Annual", "(finv / nGDP) * 100."),
        ("finv_USD", "Consumption & Investment", "Gross fixed capital formation (USD)", "Gross fixed capital formation converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("exports", "External Sector", "Exports", "Exports of goods and services in millions of local currency units.", "Millions of local currency units", "Annual", "Value of exports."),
        ("exports_GDP", "External Sector", "Exports (% GDP)", "Exports as percentage of nominal GDP.", "Percentage", "Annual", "(exports / nGDP) * 100."),
        ("exports_USD", "External Sector", "Exports (USD)", "Exports converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("imports", "External Sector", "Imports", "Imports of goods and services in millions of local currency units.", "Millions of local currency units", "Annual", "Value of imports."),
        ("imports_GDP", "External Sector", "Imports (% GDP)", "Imports as percentage of nominal GDP.", "Percentage", "Annual", "(imports / nGDP) * 100."),
        ("imports_USD", "External Sector", "Imports (USD)", "Imports converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("CA", "External Sector", "Current account balance", "Current account balance in millions of local currency units.", "Millions of local currency units", "Annual", "Net exports + net income + net transfers."),
        ("CA_GDP", "External Sector", "Current account balance (% GDP)", "Current account balance as percentage of nominal GDP.", "Percentage", "Annual", "(CA / nGDP) * 100."),
        ("USDfx", "External Sector", "Market exchange rate", "Average annual market exchange rate (local currency units per US dollar).", "Local currency units per USD", "Annual", "Market exchange rate."),
        ("REER", "External Sector", "Real effective exchange rate", "Real effective exchange rate index.", "Index (2015 = 100)", "Annual", "Trade-weighted average of bilateral real exchange rates."),
        ("govexp", "Government Finance (General)", "General government expenditure", "General government expenditure in millions of local currency units.", "Millions of local currency units", "Annual", "Total spending by general government."),
        ("gen_govexp", "Government Finance (General)", "General government expenditure (alt)", "General government expenditure (alternative definition) in millions of local currency units.", "Millions of local currency units", "Annual", "May differ based on consolidation or coverage."),
        ("gen_govexp_GDP", "Government Finance (General)", "General government expenditure (% GDP)", "General government expenditure as percentage of nominal GDP.", "Percentage", "Annual", "(gen_govexp / nGDP) * 100."),
        ("cgovexp", "Government Finance (Central)", "Central government expenditure", "Central government expenditure in millions of local currency units.", "Millions of local currency units", "Annual", "Total spending by central government."),
        ("cgovexp_GDP", "Government Finance (Central)", "Central government expenditure (% GDP)", "Central government expenditure as percentage of nominal GDP.", "Percentage", "Annual", "(cgovexp / nGDP) * 100."),
        ("govrev", "Government Finance (General)", "General government revenue", "General government revenue in millions of local currency units.", "Millions of local currency units", "Annual", "Total receipts of general government."),
        ("gen_govrev", "Government Finance (General)", "General government revenue (alt)", "General government revenue (alternative definition) in millions of local currency units.", "Millions of local currency units", "Annual", "May differ based on consolidation or coverage."),
        ("cgovrev", "Government Finance (Central)", "Central government revenue", "Central government revenue in millions of local currency units.", "Millions of local currency units", "Annual", "Total receipts of central government."),
        ("gen_govrev_GDP", "Government Finance (General)", "General government revenue (% GDP)", "General government revenue as percentage of nominal GDP.", "Percentage", "Annual", "(gen_govrev / nGDP) * 100."),
        ("cgovrev_GDP", "Government Finance (Central)", "Central government revenue (% GDP)", "Central government revenue as percentage of nominal GDP.", "Percentage", "Annual", "(cgovrev / nGDP) * 100."),
        ("govtax", "Government Finance (General)", "General government tax revenue", "General government tax revenue in millions of local currency units.", "Millions of local currency units", "Annual", "Total tax receipts of general government."),
        ("gen_govtax", "Government Finance (General)", "General government tax revenue (alt)", "General government tax revenue (alternative definition) in millions of local currency units.", "Millions of local currency units", "Annual", "May differ based on consolidation or coverage."),
        ("cgovtax", "Government Finance (Central)", "Central government tax revenue", "Central government tax revenue in millions of local currency units.", "Millions of local currency units", "Annual", "Total tax receipts of central government."),
        ("gen_govtax_GDP", "Government Finance (General)", "General government tax revenue (% GDP)", "General government tax revenue as percentage of nominal GDP.", "Percentage", "Annual", "(gen_govtax / nGDP) * 100."),
        ("cgovtax_GDP", "Government Finance (Central)", "Central government tax revenue (% GDP)", "Central government tax revenue as percentage of nominal GDP.", "Percentage", "Annual", "(cgovtax / nGDP) * 100."),
        ("govdef_GDP", "Government Finance (General)", "General government deficit/surplus (% GDP)", "General government deficit (-) / surplus (+) as percentage of nominal GDP.", "Percentage", "Annual", "(gen_govrev - gen_govexp) / nGDP."),
        ("gen_govdef_GDP", "Government Finance (General)", "General government deficit/surplus (alt) (% GDP)", "General government deficit/surplus (alternative definition) as percentage of GDP.", "Percentage", "Annual", "May differ based on consolidation or coverage."),
        ("gen_govdef", "Government Finance (General)", "General government deficit/surplus", "General government deficit (-) / surplus (+) in millions of local currency units.", "Millions of local currency units", "Annual", "gen_govrev - gen_govexp."),
        ("cgovdef_GDP", "Government Finance (Central)", "Central government deficit/surplus (% GDP)", "Central government deficit (-) / surplus (+) as percentage of nominal GDP.", "Percentage", "Annual", "(cgovrev - cgovexp) / nGDP."),
        ("cgovdef", "Government Finance (Central)", "Central government deficit/surplus", "Central government deficit (-) / surplus (+) in millions of local currency units.", "Millions of local currency units", "Annual", "cgovrev - cgovexp."),
        ("govdebt_GDP", "Government Finance (General)", "General government gross debt (% GDP)", "General government gross debt as percentage of nominal GDP.", "Percentage", "Annual", "Total gross debt of general government."),
        ("gen_govdebt_GDP", "Government Finance (General)", "General government gross debt (alt) (% GDP)", "General government gross debt (alternative definition) as percentage of GDP.", "Percentage", "Annual", "May differ based on consolidation or coverage."),
        ("gen_govdebt", "Government Finance (General)", "General government gross debt", "General government gross debt in millions of local currency units.", "Millions of local currency units", "Annual", "Total gross debt of general government."),
        ("cgovdebt_GDP", "Government Finance (Central)", "Central government gross debt (% GDP)", "Central government gross debt as percentage of nominal GDP.", "Percentage", "Annual", "Total gross debt of central government."),
        ("cgovdebt", "Government Finance (Central)", "Central government gross debt", "Central government gross debt in millions of local currency units.", "Millions of local currency units", "Annual", "Total gross debt of central government."),
        ("HPI", "Prices & Labor", "House price index", "House price index.", "Index (2015 = 100)", "Annual", "Measures changes in residential property prices."),
        ("CPI", "Prices & Labor", "Consumer price index", "Consumer price index.", "Index (2015 = 100)", "Annual", "Measures changes in price level of a basket of consumer goods and services."),
        ("infl", "Prices & Labor", "Inflation rate", "Inflation rate (annual percentage change in CPI).", "Percentage", "Annual", "Year-on-year percentage change in CPI."),
        ("pop", "Prices & Labor", "Total population", "Total population.", "Thousands", "Annual", "Total number of residents."),
        ("unemp", "Prices & Labor", "Unemployment rate", "Unemployment rate.", "Percentage", "Annual", "Percentage of labor force that is unemployed."),
        ("strate", "Prices & Labor", "Short-term interest rate", "Short-term interest rate.", "Percentage", "Annual", "Typically money market rate or similar."),
        ("ltrate", "Prices & Labor", "Long-term interest rate", "Long-term interest rate.", "Percentage", "Annual", "Typically government bond yield."),
        ("cbrate", "Prices & Labor", "Central bank policy rate", "Central bank policy rate.", "Percentage", "Annual", "Key interest rate set by central bank."),
        ("M0", "Money & Interest Rates", "Narrow money supply (M0)", "Narrow money supply (currency in circulation).", "Millions of local currency units", "Annual", "Notes and coins in circulation."),
        ("M1", "Money & Interest Rates", "Narrow money supply (M1)", "Narrow money supply (M0 + demand deposits).", "Millions of local currency units", "Annual", "M0 plus demand deposits."),
        ("M2", "Money & Interest Rates", "Broad money supply (M2)", "Broad money supply (M1 + savings deposits).", "Millions of local currency units", "Annual", "M1 plus savings deposits."),
        ("M3", "Money & Interest Rates", "Broad money supply (M3)", "Broad money supply (M2 + time deposits).", "Millions of local currency units", "Annual", "M2 plus time deposits."),
        ("M4", "Money & Interest Rates", "Broad money supply (M4)", "Broad money supply (M3 + other liquid assets).", "Millions of local currency units", "Annual", "M3 plus other liquid assets."),
        ("SovDebtCrisis", "Financial Crisis Dummy", "Sovereign debt crisis", "Indicator for sovereign debt crisis.", "Binary (0/1)", "Annual", "Equals 1 if a sovereign debt crisis occurred."),
        ("CurrencyCrisis", "Financial Crisis Dummy", "Currency crisis", "Indicator for currency crisis.", "Binary (0/1)", "Annual", "Equals 1 if a currency crisis occurred."),
        ("BankingCrisis", "Financial Crisis Dummy", "Banking crisis", "Indicator for banking crisis.", "Binary (0/1)", "Annual", "Equals 1 if a banking crisis occurred."),
        ("CA_USD", "External Sector", "Current account balance (USD)", "Current account balance converted to US dollars using USDfx.", "Millions of US dollars", "Annual", "Converted using USDfx."),
        ("govdebt", "Government Finance (General)", "General government gross debt", "General government gross debt in millions of local currency units.", "Millions of local currency units", "Annual", "Total gross debt of general government."),
        ("govdef", "Government Finance (General)", "General government deficit/surplus", "General government deficit (-) / surplus (+) in millions of local currency units.", "Millions of local currency units", "Annual", "govrev - govexp."),
        ("govexp_GDP", "Government Finance (General)", "General government expenditure (% GDP)", "General government expenditure as percentage of nominal GDP.", "Percentage", "Annual", "(govexp / nGDP) * 100."),
        ("govrev_GDP", "Government Finance (General)", "General government revenue (% GDP)", "General government revenue as percentage of nominal GDP.", "Percentage", "Annual", "(govrev / nGDP) * 100."),
        ("govtax_GDP", "Government Finance (General)", "General government tax revenue (% GDP)", "General government tax revenue as percentage of nominal GDP.", "Percentage", "Annual", "(govtax / nGDP) * 100."),
        ("rGDP_pc_USD", "National Accounts", "Real GDP per capita (USD)", "Real GDP per capita in US dollars.", "US dollars (per capita)", "Annual", "rGDP_USD / pop.")
    ]
    
    df = spark.createDataFrame(data, schema)
    # Gan nhan nguon GMD de hien thi trong dim_indicator
    return df.withColumn("source", lit("Global Macro Database (GMD)"))