import pandas as pd

def normalize_column_name(col: str) -> str:
    col = str(col).strip().upper()
    col = col.replace(" ", "_")
    col = col.replace(".", "")
    col = col.replace("-", "_")
    col = col.replace("/", "_")
    col = col.replace("(", "")
    col = col.replace(")", "")
    return col

def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [normalize_column_name(col) for col in df.columns]
    return df

def safe_str(value):
    if pd.isna(value):
        return None
    return str(value).strip()

def safe_float(value):
    if pd.isna(value) or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None

def safe_date(value):
    if pd.isna(value) or value == "":
        return None
    try:
        return pd.to_datetime(value).to_pydatetime()
    except Exception:
        return None