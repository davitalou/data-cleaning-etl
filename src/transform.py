from src.utils import (
    normalize_dataframe_columns,
    safe_str,
    safe_float,
    safe_date,
)

def transform_kpi(df):
    df = normalize_dataframe_columns(df)
    expected_map = {
        "NĂM": "NAM",
        "NAM": "NAM",
        "CHI_NHÁNH": "CHI_NHANH",
        "CHI_NHANH": "CHI_NHANH",
        "KPI": "KPI"
    }
    df = df.rename(columns={c: expected_map.get(c, c) for c in df.columns})
    df = df[["NAM", "CHI_NHANH", "KPI"]].copy()

    df["NAM"] = df["NAM"].apply(safe_float)
    df["CHI_NHANH"] = df["CHI_NHANH"].apply(safe_str)
    df["KPI"] = df["KPI"].apply(safe_float)
    return df

def transform_khach_hang(df):
    df = normalize_dataframe_columns(df)
    rename_map = {
        "MÃ_KH": "MA_KH",
        "MA_KH": "MA_KH",
        "KHÁCH_HÀNG": "KHACH_HANG",
        "KHACH_HANG": "KHACH_HANG",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
    df = df[["MA_KH", "KHACH_HANG"]].copy()
    df["MA_KH"] = df["MA_KH"].apply(safe_str)
    df["KHACH_HANG"] = df["KHACH_HANG"].apply(safe_str)
    df = df.dropna(subset=["MA_KH"]).drop_duplicates(subset=["MA_KH"])
    return df

def transform_san_pham(df):
    df = normalize_dataframe_columns(df)
    rename_map = {
        "MÃ_SẢN_PHẨM": "MA_SAN_PHAM",
        "MA_SAN_PHAM": "MA_SAN_PHAM",
        "SẢN_PHẨM": "SAN_PHAM",
        "SAN_PHAM": "SAN_PHAM",
        "NHÓM_SẢN_PHẨM": "NHOM_SAN_PHAM",
        "NHOM_SAN_PHAM": "NHOM_SAN_PHAM",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
    df = df[["MA_SAN_PHAM", "SAN_PHAM", "NHOM_SAN_PHAM"]].copy()
    df["MA_SAN_PHAM"] = df["MA_SAN_PHAM"].apply(safe_str)
    df["SAN_PHAM"] = df["SAN_PHAM"].apply(safe_str)
    df["NHOM_SAN_PHAM"] = df["NHOM_SAN_PHAM"].apply(safe_str)
    df = df.dropna(subset=["MA_SAN_PHAM"]).drop_duplicates(subset=["MA_SAN_PHAM"])
    return df

def transform_nhan_vien(df):
    df = normalize_dataframe_columns(df)
    rename_map = {
        "MÃ_NHÂN_VIÊN_BÁN": "MA_NHAN_VIEN_BAN",
        "MA_NHAN_VIEN_BAN": "MA_NHAN_VIEN_BAN",
        "NHÂN_VIÊN_BÁN": "NHAN_VIEN_BAN",
        "NHAN_VIEN_BAN": "NHAN_VIEN_BAN",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
    df = df[["MA_NHAN_VIEN_BAN", "NHAN_VIEN_BAN"]].copy()
    df["MA_NHAN_VIEN_BAN"] = df["MA_NHAN_VIEN_BAN"].apply(safe_str)
    df["NHAN_VIEN_BAN"] = df["NHAN_VIEN_BAN"].apply(safe_str)
    df = df.dropna(subset=["MA_NHAN_VIEN_BAN"]).drop_duplicates(subset=["MA_NHAN_VIEN_BAN"])
    return df

def transform_chi_nhanh(df):
    df = normalize_dataframe_columns(df)
    rename_map = {
        "MÃ_CHI_NHÁNH": "MA_CHI_NHANH",
        "MA_CHI_NHANH": "MA_CHI_NHANH",
        "TÊN_CHI_NHÁNH": "TEN_CHI_NHANH",
        "TEN_CHI_NHANH": "TEN_CHI_NHANH",
        "TỈNH_THÀNH_PHỐ": "TINH_THANH_PHO",
        "TINH_THANH_PHO": "TINH_THANH_PHO",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
    df = df[["MA_CHI_NHANH", "TEN_CHI_NHANH", "TINH_THANH_PHO"]].copy()
    df["MA_CHI_NHANH"] = df["MA_CHI_NHANH"].apply(safe_str)
    df["TEN_CHI_NHANH"] = df["TEN_CHI_NHANH"].apply(safe_str)
    df["TINH_THANH_PHO"] = df["TINH_THANH_PHO"].apply(safe_str)
    df = df.dropna(subset=["MA_CHI_NHANH"]).drop_duplicates(subset=["MA_CHI_NHANH"])
    return df

def transform_du_lieu_ban_hang(df):
    df = normalize_dataframe_columns(df)
    rename_map = {
        "NGÀY_HẠCH_TOÁN": "NGAY_HACH_TOAN",
        "NGAY_HACH_TOAN": "NGAY_HACH_TOAN",
        "ĐƠN_HÀNG": "DON_HANG",
        "DON_HANG": "DON_HANG",
        "MÃ_KH": "MA_KH",
        "MA_KH": "MA_KH",
        "MÃ_SẢN_PHẨM": "MA_SAN_PHAM",
        "MA_SAN_PHAM": "MA_SAN_PHAM",
        "SỐ_LƯỢNG_BÁN": "SO_LUONG_BAN",
        "SO_LUONG_BAN": "SO_LUONG_BAN",
        "ĐƠN_GIÁ": "DON_GIA",
        "DON_GIA": "DON_GIA",
        "DOANH_THU": "DOANH_THU",
        "GIÁ_VỐN_HÀNG_HÓA": "GIA_VON_HANG_HOA",
        "GIA_VON_HANG_HOA": "GIA_VON_HANG_HOA",
        "MÃ_NHÂN_VIÊN_BÁN": "MA_NHAN_VIEN_BAN",
        "MA_NHAN_VIEN_BAN": "MA_NHAN_VIEN_BAN",
        "CHI_NHÁNH": "CHI_NHANH",
        "CHI_NHANH": "CHI_NHANH",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    cols = [
        "NGAY_HACH_TOAN","DON_HANG", "MA_KH", "MA_SAN_PHAM",
        "SO_LUONG_BAN", "DON_GIA", "DOANH_THU", "GIA_VON_HANG_HOA",
        "MA_NHAN_VIEN_BAN", "CHI_NHANH"
    ]
    df = df[cols].copy()

    df["NGAY_HACH_TOAN"] = df["NGAY_HACH_TOAN"].apply(safe_date)
    df["DON_HANG"] = df["DON_HANG"].apply(safe_str)
    df["MA_KH"] = df["MA_KH"].apply(safe_str)
    df["MA_SAN_PHAM"] = df["MA_SAN_PHAM"].apply(safe_str)
    df["SO_LUONG_BAN"] = df["SO_LUONG_BAN"].apply(safe_float)
    df["DON_GIA"] = df["DON_GIA"].apply(safe_float)
    df["DOANH_THU"] = df["DOANH_THU"].apply(safe_float)
    df["GIA_VON_HANG_HOA"] = df["GIA_VON_HANG_HOA"].apply(safe_float)
    df["MA_NHAN_VIEN_BAN"] = df["MA_NHAN_VIEN_BAN"].apply(safe_str)
    df["CHI_NHANH"] = df["CHI_NHANH"].apply(safe_str)
    return df