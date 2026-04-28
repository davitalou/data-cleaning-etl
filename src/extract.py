import os
import pandas as pd
from src.config import DATA_DIR, KPI_FILE, MODELING_FILE

def get_kpi_file_path():
    return os.path.join(DATA_DIR, KPI_FILE)

def get_modeling_file_path():
    return os.path.join(DATA_DIR, MODELING_FILE)

def read_kpi_file():
    file_path = get_kpi_file_path()
    return pd.read_excel(file_path)

def read_modeling_sheet(sheet_name: str):
    file_path = get_modeling_file_path()
    return pd.read_excel(file_path, sheet_name=sheet_name)