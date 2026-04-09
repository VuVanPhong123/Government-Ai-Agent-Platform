import sys
import yaml
import requests
import pandas as pd
import logging
import time
from pathlib import Path
from tqdm import tqdm

SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_FILE  = PROJECT_ROOT / "config" / "worldbank_config.yaml"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw" / "worldbank"
LOG_FILE     = SCRIPT_DIR / "fetch_worldbank.log"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

WB_BASE = "https://api.worldbank.org/v2/country/{countries}/indicator/{indicator}"


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        tqdm.write(self.format(record))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        TqdmLoggingHandler(),
    ],
)
logger = logging.getLogger(__name__)


def load_config(path: Path) -> dict:
    if not path.exists():
        logger.critical(f"Config file not found: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logger.info("Config loaded successfully.")
    return cfg["worldbank"]


def fetch_one_indicator(
    indicator: str,
    countries: list[str],
    start_year: int,
    end_year: int,
    timeout: int = 30,
) -> pd.DataFrame:
    country_str = ";".join(countries)
    url = WB_BASE.format(countries=country_str, indicator=indicator)
    params = {
        "format": "json",
        "date": f"{start_year}:{end_year}",
        "per_page": 10000,
        "mrv": "",
    }

    all_records = []
    page = 1

    while True:
        params["page"] = page
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list) or len(data) < 2 or not data[1]:
            break

        meta    = data[0]
        records = data[1]

        for r in records:
            if r.get("value") is None:
                continue
            all_records.append({
                "country_code":   r["countryiso3code"],
                "year":           int(r["date"]),
                "indicator_code": indicator,
                "value":          r["value"],
            })

        if page >= meta.get("pages", 1):
            break
        page += 1

    return pd.DataFrame(all_records)


def fetch_indicator_names(indicators: list[str], timeout: int = 30) -> dict[str, str]:
    logger.info("Fetching indicator names...")
    name_map = {}
    for ind in tqdm(indicators, desc="Indicator names", unit="ind"):
        try:
            url  = f"https://api.worldbank.org/v2/indicator/{ind}?format=json"
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and len(data) > 1 and data[1]:
                name_map[ind] = data[1][0].get("name", ind)
            else:
                name_map[ind] = ind
        except Exception:
            name_map[ind] = ind
    return name_map


def make_chunks(lst: list, size: int) -> list:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def main() -> None:
    cfg = load_config(CONFIG_FILE)

    countries       = cfg["countries"]
    indicators      = cfg["indicators"]
    start_year      = cfg["years"]["start"]
    end_year        = cfg["years"]["end"]
    ctry_chunk_size = cfg["fetch"].get("country_chunk", 30)
    sleep_seconds   = cfg["fetch"]["sleep_seconds"]
    retry_delay     = cfg["fetch"]["retry_delay"]
    partition_by_yr = cfg["fetch"]["partition_by_year"]

    logger.info(
        f"Target: {len(countries)} countries | "
        f"{len(indicators)} indicators | "
        f"{start_year}-{end_year}"
    )

    name_map    = fetch_indicator_names(indicators)
    ctry_chunks = make_chunks(countries, ctry_chunk_size)
    total       = len(indicators) * len(ctry_chunks)

    logger.info(
        f"Split into {len(indicators)} indicators x "
        f"{len(ctry_chunks)} country chunks = {total} total requests"
    )

    all_frames: list[pd.DataFrame] = []

    with tqdm(total=total, desc="Fetching", unit="req") as pbar:
        for ind in indicators:
            for j, ctry_chunk in enumerate(ctry_chunks, start=1):
                label = f"{ind} ctry={j}/{len(ctry_chunks)}"

                for attempt in range(1, 4):
                    try:
                        df_chunk = fetch_one_indicator(
                            ind, ctry_chunk, start_year, end_year
                        )
                        if df_chunk.empty:
                            logger.warning(f"[{label}] Empty response.")
                        else:
                            all_frames.append(df_chunk)
                            logger.info(f"[{label}] {len(df_chunk):,} rows.")
                        break
                    except requests.exceptions.Timeout:
                        logger.error(f"[{label}] Attempt {attempt}/3 timed out.")
                        if attempt < 3:
                            time.sleep(retry_delay)
                    except Exception as exc:
                        logger.error(f"[{label}] Attempt {attempt}/3 failed: {exc}")
                        if attempt < 3:
                            time.sleep(retry_delay)
                else:
                    logger.error(f"[{label}] Skipped after 3 failures.")

                pbar.update(1)
                time.sleep(sleep_seconds)

    if not all_frames:
        logger.error("No data fetched. Check network or API availability.")
        sys.exit(1)

    logger.info("Concatenating all chunks...")
    final_df = pd.concat(all_frames, ignore_index=True)
    final_df = final_df.drop_duplicates()
    logger.info(f"Total rows after dedup: {len(final_df):,}")

    final_df["indicator_name"] = final_df["indicator_code"].map(name_map)
    final_df = final_df[["country_code", "year", "indicator_code", "indicator_name", "value"]]

    if partition_by_yr:
        final_df.to_parquet(
            RAW_DATA_DIR,
            partition_cols=["year"],
            engine="pyarrow",
            index=False,
        )
        logger.info(f"Saved {len(final_df):,} rows partitioned by year to {RAW_DATA_DIR}")
    else:
        output_file = RAW_DATA_DIR / "wdi_full_data.parquet"
        final_df.to_parquet(output_file, engine="pyarrow", index=False)
        logger.info(f"Saved {len(final_df):,} rows to {output_file}")

    sample_file = RAW_DATA_DIR / "wdi_sample.csv"
    final_df.head(10_000).to_csv(sample_file, index=False)
    logger.info(f"Sample saved to {sample_file}")

    logger.info(
        f"Done: {final_df['country_code'].nunique()} countries | "
        f"{final_df['indicator_code'].nunique()} indicators | "
        f"{final_df['year'].min()}-{final_df['year'].max()}"
    )


if __name__ == "__main__":
    main()