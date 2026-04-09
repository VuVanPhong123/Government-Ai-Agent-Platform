import sys
import random
import requests
import pandas as pd
import logging
import time
from pathlib import Path
from tqdm import tqdm

OUTPUT_DIR = Path("/kaggle/working")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = OUTPUT_DIR / "fetch_worldbank.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

WB_BASE = "https://api.worldbank.org/v2/country/{countries}/indicator/{indicator}"

COUNTRIES = [
    "USA","CHN","JPN","DEU","GBR","FRA","IND","BRA","CAN","AUS",
    "ITA","KOR","RUS","MEX","TUR","SAU","ARG","ZAF","IDN",
    "VNM","THA","MYS","SGP","PHL","MMR","KHM","LAO","BRN","TLS",
    "AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","GRC",
    "HUN","IRL","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU",
    "SVK","SVN","ESP","SWE","ISL","NOR","CHE",
    "BGD","PAK","LKA","NPL",
    "ARE","EGY","IRN","IRQ","ISR","JOR","KWT","LBN","MAR","OMN","QAT","TUN",
    "NGA","KEN","ETH","GHA","ANG","CIV","CMR","ZMB","SEN","UGA","TZA","MOZ",
    "COL","CHL","PER","ECU","VEN","URY","PRY","BOL","CRI","PAN","DOM","JAM",
    "UKR","KAZ","UZB","AZE","GEO","BLR","ARM","KGZ",
    "NZL","HKG",
]

ALL_INDICATORS = [
    "NY.GDP.MKTP.CD","NY.GDP.MKTP.KN","NY.GDP.PCAP.CD","NY.GDP.PCAP.KN",
    "NY.GDP.MKTP.KD.ZG","NY.GDP.PCAP.KD.ZG","NY.GDP.MKTP.PP.CD","NY.GDP.PCAP.PP.CD",
    "NY.GNP.MKTP.CD","NY.GNP.PCAP.CD","NE.CON.TOTL.CD","NE.CON.GOVT.CD",
    "NE.GDI.TOTL.CD","NE.EXP.GNFS.CD","NE.IMP.GNFS.CD","BX.GSR.GNFS.CD",
    "BX.KLT.DINV.WD.GD.ZS","BN.KLT.DINV.CD","BX.PEF.TOTL.CD","NE.RSB.GNFS.CD",
    "BG.GSR.NFSV.GD.ZS","NY.GDP.DEFL.KD.ZG","NY.GDP.DEFL.KD.ZG.AD",
    "GC.DOD.TOTL.GD.ZS","GC.TAX.TOTL.GD.ZS","GC.XPN.TOTL.GD.ZS",
    "GC.REV.XGRT.GD.ZS","NY.GNS.ICTR.CD","NY.ADJ.NNAT.GN.ZS","NY.TRF.NCTR.CD",
    "FP.CPI.TOTL","FP.CPI.TOTL.ZG","FP.WPI.TOTL","PA.NUS.FCRF","PA.NUS.ATLS",
    "SL.UEM.TOTL.ZS","SL.UEM.1524.ZS","SL.TLF.TOTL.IN","SL.TLF.CACT.ZS",
    "SL.EMP.TOTL.SP.ZS","SL.EMP.SELF.ZS","SL.AGR.EMPL.ZS","SL.IND.EMPL.ZS",
    "SL.SRV.EMPL.ZS","SP.POP.TOTL","SP.POP.GROW","SP.URB.TOTL.IN.ZS",
    "SP.DYN.LE00.IN","SE.XPD.TOTL.GD.ZS","SH.XPD.CHEX.GD.ZS",
    "FM.LBL.MQMY.GD.ZS","FD.RES.CASH.SG.CD","FR.INR.RINR","FR.INR.LEND",
    "FR.INR.DPST","GFDD.DM.01","CM.MKT.INDX.ZG","GFDD.OM.01",
    "CM.MKT.TRAD.CD","CM.MKT.LTCM.NV",
    "DT.DOD.DECT.CD","DT.DOD.DPPG.CD","DT.DOD.DLXF.CD","DT.DOD.DSTC.CD",
    "DT.DOD.PNGO.CD","DT.TDS.DECT.CD","DT.TDS.DPPG.CD","DT.INT.DECT.CD",
    "DT.NFL.PCEN.CD","BX.GRT.EXTA.CD.WD","BN.RES.INCL.CD","FI.RES.TOTL.CD",
    "DT.IXA.DPPG.CD","DT.IXA.DPPG.CG","DT.IXA.DSTC.CD",
    "EG.USE.PCAP.KG.OE","EG.ELC.ACCS.ZS","EN.ATM.CO2E.PC","EN.ATM.CO2E.KT",
    "AG.LND.FRST.ZS","AG.LND.AGRI.ZS","AG.LND.ARBL.ZS","EN.ATM.GHGT.KT.CE",
    "EN.URB.LUPT.ZS","EN.CLC.MDAT.ZS",
    "IT.NET.USER.ZS","IT.CEL.SETS","IT.CEL.SETS.P2","IS.ROD.DNST.K2",
    "IS.AIR.PSGR","IS.SHP.GOOD.TU","IE.PPI.ENGY.CD","IE.PPI.TELE.CD",
    "IE.PPI.TRAN.CD","IE.PPI.WATR.CD",
    "SI.POV.NAHC","SI.POV.DDAY","SI.POV.GAPS","SI.DST.10TH.10",
    "SI.DST.FRST.10","SI.DST.05TH.20","SI.DST.FRST.20","SI.POV.GINI",
    "SI.POV.URBR.CD","SI.POV.RURB.CD",
]

START_YEAR      = 1990
END_YEAR        = 2024
COUNTRY_CHUNK   = 20
TIMEOUT         = 30
MAX_RETRIES     = 5
BASE_SLEEP      = 5
JITTER_MAX      = 3
RETRY_BASE      = 15
RATE_LIMIT_WAIT = 120


def jitter_sleep(base: float = BASE_SLEEP) -> None:
    time.sleep(base + random.uniform(0, JITTER_MAX))


def exponential_backoff(attempt: int) -> None:
    delay = RETRY_BASE * (2 ** (attempt - 1)) + random.uniform(0, 5)
    logger.info(f"Backoff: waiting {delay:.1f}s...")
    time.sleep(delay)


def make_chunks(lst: list, size: int) -> list:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def fetch_indicator_names(indicators: list[str]) -> dict[str, str]:
    logger.info("Fetching indicator names...")
    name_map = {}
    for ind in tqdm(indicators, desc="Names", unit="ind"):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    f"https://api.worldbank.org/v2/indicator/{ind}?format=json",
                    timeout=TIMEOUT,
                )
                if resp.status_code == 429:
                    logger.warning(f"Rate limited on name fetch, waiting {RATE_LIMIT_WAIT}s...")
                    time.sleep(RATE_LIMIT_WAIT)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list) and len(data) > 1 and data[1]:
                    name_map[ind] = data[1][0].get("name", ind)
                else:
                    name_map[ind] = ind
                break
            except Exception:
                if attempt == MAX_RETRIES:
                    name_map[ind] = ind
                else:
                    exponential_backoff(attempt)
        jitter_sleep(1)
    return name_map


def fetch_one_indicator(indicator: str, countries: list[str]) -> pd.DataFrame:
    country_str = ";".join(countries)
    url = WB_BASE.format(countries=country_str, indicator=indicator)
    params = {
        "format":   "json",
        "date":     f"{START_YEAR}:{END_YEAR}",
        "per_page": 10000,
        "page":     1,
    }

    all_records = []
    page = 1

    while True:
        params["page"] = page
        resp = requests.get(url, params=params, timeout=TIMEOUT)

        if resp.status_code == 429:
            raise Exception("RATE_LIMIT_429")

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


def main() -> None:
    ctry_chunks = make_chunks(COUNTRIES, COUNTRY_CHUNK)
    total       = len(ALL_INDICATORS) * len(ctry_chunks)

    logger.info(
        f"Target: {len(ALL_INDICATORS)} indicators x "
        f"{len(ctry_chunks)} country chunks = {total} total requests"
    )

    name_map   = fetch_indicator_names(ALL_INDICATORS)
    all_frames: list[pd.DataFrame] = []

    with tqdm(total=total, desc="Fetching", unit="req") as pbar:
        for ind in ALL_INDICATORS:
            for j, ctry_chunk in enumerate(ctry_chunks, start=1):
                label = f"{ind} ctry={j}/{len(ctry_chunks)}"

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        df_chunk = fetch_one_indicator(ind, ctry_chunk)
                        if df_chunk.empty:
                            logger.warning(f"[{label}] Empty response.")
                        else:
                            all_frames.append(df_chunk)
                            logger.info(f"[{label}] {len(df_chunk):,} rows.")
                        break

                    except requests.exceptions.Timeout:
                        logger.error(f"[{label}] Attempt {attempt}/{MAX_RETRIES} timed out.")
                        exponential_backoff(attempt)

                    except Exception as exc:
                        if "RATE_LIMIT_429" in str(exc):
                            logger.warning(f"[{label}] Rate limited (429). Waiting {RATE_LIMIT_WAIT}s...")
                            time.sleep(RATE_LIMIT_WAIT)
                        else:
                            logger.error(f"[{label}] Attempt {attempt}/{MAX_RETRIES} failed: {exc}")
                            exponential_backoff(attempt)
                else:
                    logger.error(f"[{label}] Skipped after {MAX_RETRIES} failures.")

                pbar.update(1)
                jitter_sleep(BASE_SLEEP)

    if not all_frames:
        logger.error("No data fetched.")
        sys.exit(1)

    logger.info("Concatenating...")
    final_df = pd.concat(all_frames, ignore_index=True)
    final_df = final_df.drop_duplicates()
    logger.info(f"Total rows: {len(final_df):,}")

    final_df["indicator_name"] = final_df["indicator_code"].map(name_map)
    final_df = final_df[["country_code", "year", "indicator_code", "indicator_name", "value"]]

    output_file = OUTPUT_DIR / "worldbank_full.parquet"
    final_df.to_parquet(output_file, engine="pyarrow", index=False)
    logger.info(f"Saved to {output_file}")

    sample_file = OUTPUT_DIR / "worldbank_sample.csv"
    final_df.head(5_000).to_csv(sample_file, index=False)
    logger.info(f"Sample saved to {sample_file}")

    logger.info(
        f"Done: {final_df['country_code'].nunique()} countries | "
        f"{final_df['indicator_code'].nunique()} indicators | "
        f"{final_df['year'].min()}-{final_df['year'].max()}"
    )


if __name__ == "__main__":
    main()