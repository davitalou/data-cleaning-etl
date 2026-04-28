import os
from dotenv import load_dotenv

load_dotenv()

ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "XEPDB1")

DATA_DIR = os.getenv("DATA_DIR", "./data")
KPI_FILE = os.getenv("KPI_FILE", "KPI_Template.xlsx")
MODELING_FILE = os.getenv("MODELING_FILE", "Modeling_and_DAX.xlsx")