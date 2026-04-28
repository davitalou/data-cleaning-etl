from src.db import get_connection
import pandas as pd

def truncate_table(table_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def insert_kpi(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO KPI_TEMPLATE (NAM, CHI_NHANH, KPI)
        VALUES (:1, :2, :3)
    """
    data = [tuple(x) for x in df[["NAM", "CHI_NHANH", "KPI"]].to_records(index=False)]
    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()
    conn.close()

def merge_khach_hang(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        MERGE INTO KHACH_HANG tgt
        USING (SELECT :1 AS MA_KH, :2 AS KHACH_HANG FROM dual) src
        ON (tgt.MA_KH = src.MA_KH)
        WHEN MATCHED THEN
            UPDATE SET tgt.KHACH_HANG = src.KHACH_HANG
        WHEN NOT MATCHED THEN
            INSERT (MA_KH, KHACH_HANG)
            VALUES (src.MA_KH, src.KHACH_HANG)
    """

    data = [tuple(x) for x in df[["MA_KH", "KHACH_HANG"]].to_records(index=False)]
    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()
    conn.close()

def merge_san_pham(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        MERGE INTO SAN_PHAM tgt
        USING (SELECT :1 AS MA_SAN_PHAM, :2 AS SAN_PHAM, :3 AS NHOM_SAN_PHAM FROM dual) src
        ON (tgt.MA_SAN_PHAM = src.MA_SAN_PHAM)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.SAN_PHAM = src.SAN_PHAM,
                tgt.NHOM_SAN_PHAM = src.NHOM_SAN_PHAM
        WHEN NOT MATCHED THEN
            INSERT (MA_SAN_PHAM, SAN_PHAM, NHOM_SAN_PHAM)
            VALUES (src.MA_SAN_PHAM, src.SAN_PHAM, src.NHOM_SAN_PHAM)
    """

    data = [tuple(x) for x in df[["MA_SAN_PHAM", "SAN_PHAM", "NHOM_SAN_PHAM"]].to_records(index=False)]
    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()
    conn.close()

def merge_nhan_vien(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        MERGE INTO NHAN_VIEN tgt
        USING (SELECT :1 AS MA_NHAN_VIEN_BAN, :2 AS NHAN_VIEN_BAN FROM dual) src
        ON (tgt.MA_NHAN_VIEN_BAN = src.MA_NHAN_VIEN_BAN)
        WHEN MATCHED THEN
            UPDATE SET tgt.NHAN_VIEN_BAN = src.NHAN_VIEN_BAN
        WHEN NOT MATCHED THEN
            INSERT (MA_NHAN_VIEN_BAN, NHAN_VIEN_BAN)
            VALUES (src.MA_NHAN_VIEN_BAN, src.NHAN_VIEN_BAN)
    """

    data = [tuple(x) for x in df[["MA_NHAN_VIEN_BAN", "NHAN_VIEN_BAN"]].to_records(index=False)]
    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()
    conn.close()

def merge_chi_nhanh(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        MERGE INTO CHI_NHANH tgt
        USING (SELECT :1 AS MA_CHI_NHANH, :2 AS TEN_CHI_NHANH, :3 AS TINH_THANH_PHO FROM dual) src
        ON (tgt.MA_CHI_NHANH = src.MA_CHI_NHANH)
        WHEN MATCHED THEN
            UPDATE SET
                tgt.TEN_CHI_NHANH = src.TEN_CHI_NHANH,
                tgt.TINH_THANH_PHO = src.TINH_THANH_PHO
        WHEN NOT MATCHED THEN
            INSERT (MA_CHI_NHANH, TEN_CHI_NHANH, TINH_THANH_PHO)
            VALUES (src.MA_CHI_NHANH, src.TEN_CHI_NHANH, src.TINH_THANH_PHO)
    """

    data = [tuple(x) for x in df[["MA_CHI_NHANH", "TEN_CHI_NHANH", "TINH_THANH_PHO"]].to_records(index=False)]
    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()
    conn.close()

def load_du_lieu_ban_hang(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO DU_LIEU_BAN_HANG (
            NGAY_HACH_TOAN, DON_HANG, MA_KH, MA_SAN_PHAM,
            SO_LUONG_BAN, DON_GIA, DOANH_THU, GIA_VON_HANG_HOA,
            MA_NHAN_VIEN_BAN, CHI_NHANH
        )
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)
    """

    if "NGAY_HACH_TOAN" in df.columns:
        df["NGAY_HACH_TOAN"] = pd.to_datetime(df["NGAY_HACH_TOAN"], errors="coerce")
        df["NGAY_HACH_TOAN"] = df["NGAY_HACH_TOAN"].apply(
            lambda x: x.to_pydatetime() if pd.notna(x) else None
        )

    data = list(df[
        [
            "NGAY_HACH_TOAN", "DON_HANG", "MA_KH", "MA_SAN_PHAM",
            "SO_LUONG_BAN", "DON_GIA", "DOANH_THU", "GIA_VON_HANG_HOA",
            "MA_NHAN_VIEN_BAN", "CHI_NHANH"
        ]
    ].itertuples(index=False, name=None))

    cursor.executemany(sql, data)

    conn.commit()
    cursor.close()
    conn.close()