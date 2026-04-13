# export_simple.py
import pandas as pd
import os

PROCESSED_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/processed"

# 1. Fact: lấy 10000 dòng đầu
fact_path = os.path.join(PROCESSED_DIR, "fact_economic_indicators")
print("Đang đọc fact (10k dòng)...")
# Đọc toàn bộ rồi lấy head (nếu file quá lớn, pandas sẽ đọc hết nhưng với 10k dòng thì ổn)
df_fact = pd.read_parquet(fact_path).head(10000)
df_fact.to_csv(os.path.join(PROCESSED_DIR, "fact_sample_10000.csv"), index=False)
print("Đã xuất fact_sample_10000.csv")

# 2. Dim country
country_path = os.path.join(PROCESSED_DIR, "dim_country")
if os.path.exists(country_path):
    df_country = pd.read_parquet(country_path)
    df_country.to_csv(os.path.join(PROCESSED_DIR, "dim_country.csv"), index=False)
    print("Đã xuất dim_country.csv")
else:
    print("Không tìm thấy dim_country")

# 3. Dim indicator
indicator_path = os.path.join(PROCESSED_DIR, "dim_indicator")
if os.path.exists(indicator_path):
    df_indicator = pd.read_parquet(indicator_path)
    df_indicator.to_csv(os.path.join(PROCESSED_DIR, "dim_indicator.csv"), index=False)
    print("Đã xuất dim_indicator.csv")
else:
    print("Không tìm thấy dim_indicator")

print("Hoàn thành!")