import oracledb
from src.config import ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST, ORACLE_PORT

ORACLE_SID = "orcl"

def get_connection():
    dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SID)
    conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=dsn
    )
    return conn