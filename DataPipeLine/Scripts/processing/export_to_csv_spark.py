import pandas as pd
import os

PROCESSED_DIR = "C:/Users/ADMIN/GovernmentAI/DataPipeLine/data/processed"

TARGET_COUNTRY = "USA"
fact_path = os.path.join(PROCESSED_DIR, "fact_economic_indicators")
print("Đang đọc fact...")
df_fact = pd.read_parquet(fact_path)

if 'country' not in df_fact.columns:
    # Thử một số tên cột phổ biến
    possible_names = ['country_name', 'country_code', 'country_id']
    for col in possible_names:
        if col in df_fact.columns:
            print(f"Dùng cột '{col}' để lọc quốc gia")
            country_col = col
            break
    else:
        raise KeyError("Không tìm thấy cột chứa tên/mã quốc gia trong fact. Các cột hiện có: " + str(df_fact.columns))
else:
    country_col = 'country'

# Lọc theo quốc gia
df_filtered = df_fact[df_fact[country_col] == TARGET_COUNTRY]

if df_filtered.empty:
    print(f"Không tìm thấy dữ liệu cho quốc gia '{TARGET_COUNTRY}'. Các giá trị duy nhất trong cột '{country_col}':")
    print(df_fact[country_col].unique()[:20])  # in 20 giá trị đầu để tham khảo
else:
    output_csv = os.path.join(PROCESSED_DIR, f"fact_{TARGET_COUNTRY}_all.csv")
    df_filtered.to_csv(output_csv, index=False)
    print(f"Đã xuất {len(df_filtered)} dòng cho {TARGET_COUNTRY} vào {output_csv}")

# 2. Dim country
country_path = os.path.join(PROCESSED_DIR, "dim_country")
if os.path.exists(country_path):
    df_country = pd.read_parquet(country_path)
    df_country.to_csv(os.path.join(PROCESSED_DIR, "dim_country.csv"), index=False)
    print("Đã xuất dim_country.csv")
else:
    print("Không tìm thấy dim_country")

indicator_path = os.path.join(PROCESSED_DIR, "dim_indicator")
if os.path.exists(indicator_path):
    df_indicator = pd.read_parquet(indicator_path)
    df_indicator.to_csv(os.path.join(PROCESSED_DIR, "dim_indicator.csv"), index=False)
    print("Đã xuất dim_indicator.csv")
else:
    print("Không tìm thấy dim_indicator")

print("Hoàn thành!")