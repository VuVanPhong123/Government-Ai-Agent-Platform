import yfinance as yf
import pandas as pd

# 1. Khai báo danh mục tất cả các nhóm hàng hóa (Tickers)
commodities = {
    # --- NĂNG LƯỢNG (Energy) ---
    'CL=F': 'Dau_WTI',
    'BZ=F': 'Dau_Brent',
    'NG=F': 'Khi_Tu_Nhien',
    'RB=F': 'Xang',
    'HO=F': 'dau suoi',
    'MTF=F': 'than',
    # --- KIM LOẠI CÔNG NGHIỆP (Industrial Metals) ---
    'HG=F': 'Dong',           # Đồng (Copper)
    'ALI=F': 'Nhom',
    # --- KIM LOẠI QUÝ (Precious Metals) ---
    'GC=F': 'Vang',
    'SI=F': 'Bac',
    'PL=F': 'Bach_Kim',
    'PA=F': 'Palladium',

    # --- NÔNG SẢN (Agriculture) ---
    'ZC=F': 'Ngo',            # Ngô
    'ZW=F': 'Lua_Mi',         # Lúa mì
    'ZS=F': 'Dau_Tuong',      # Đậu tương
    'KC=F': 'Ca_Phe',         # Cà phê
    'SB=F': 'Duong',          # Đường
    'CT=F': 'Bong',           # Bông

    # --- CHĂN NUÔI (Livestock) ---
    'LE=F': 'Bo_Song',        # Bò sống (Live Cattle)
    'GF=F': 'Bo_Thit',        # Bò thịt (Feeder Cattle)
    'HE=F': 'Lon_Nac'         # Lợn nạc (Lean Hogs)
}

# Lấy danh sách các mã ticker
tickers = list(commodities.keys())

print("Đang kết nối và tải dữ liệu từ Yahoo Finance...")
print("Sẽ mất khoảng 1-2 phút do lượng dữ liệu lớn (30 năm x 17 mặt hàng).\n")

# 2. Tải dữ liệu từ 1995 đến hết 2024
# group_by='ticker' giúp Pandas dễ xử lý cấu trúc cột hơn khi tải nhiều mã
data = yf.download(
    tickers, 
    start="1995-01-01", 
    end="2024-12-31", 
    auto_adjust=False,
    progress=True  # Hiển thị thanh tiến trình
)

# 3. Lọc chỉ lấy cột Giá đóng cửa (Close)
df_close = data['Close']

# 4. Đổi mã Ticker thành tên tiếng Việt cho dễ đọc
df_close.rename(columns=commodities, inplace=True)

# 5. Làm sạch dữ liệu
# Vào các ngày nghỉ lễ, thị trường không giao dịch sẽ sinh ra giá trị NaN (Not a Number).
# ffill (forward fill) sẽ lấy giá trị của ngày giao dịch gần nhất trước đó điền vào.
df_close = df_close.ffill()

# 6. Xuất ra file CSV
file_name = "Tong_Hop_Nguyen_Vat_Lieu_1995_2024.csv"
df_close.to_csv(file_name)
