import traceback
import pandas as pd

from src.core.logger import logger
from src.generated.indicator_contract import (
    CLUSTER_TARGET_YEARS,
    TABLES_INDICATORS,
)
from src.pipelines.trend import compute_trend_for_indicator, save_trends_to_analytics
from src.pipelines.anomaly import update_anomaly_scores
from src.pipelines.cluster import run_clustering


def run_all_analytics():
    logger.info("Starting Full Analytics Batch Process")
    
    for table_name, indicators in TABLES_INDICATORS.items():
        for indicator in indicators:
            logger.info(f"Batch Processing: {indicator} in {table_name}")
            try:
                results = compute_trend_for_indicator(table_name, indicator)
                if results:
                    df = pd.DataFrame(results)
                    save_trends_to_analytics(table_name, indicator, df)
                    update_anomaly_scores(table_name, indicator)
            except Exception as e:
                logger.error(f"Error processing {indicator} in {table_name}: {traceback.format_exc()}")

    target_years = list(CLUSTER_TARGET_YEARS)
    for year in target_years:
        logger.info(f"Batch Processing Clustering for year {year}")
        try:
            run_clustering(target_year=year, n_clusters=5)
        except Exception as e:
            logger.error(f"Error processing clustering for {year}: {traceback.format_exc()}")
            
    logger.info("Full Analytics Batch Process Completed")