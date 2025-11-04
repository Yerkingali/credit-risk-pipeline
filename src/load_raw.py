"""
Load RAW layer to Azure SQL from local CSVs.

- Reads config from environment (.env)
- Creates raw tables (NVARCHAR(MAX) columns) based on CSV headers
- Loads data in chunks with fast_executemany
- DWH logic lives in /sql scripts
"""

import os
from pathlib import Path
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text, event

# 1) подхватываем .env (если есть)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# 2) конфиг из переменных окружения
SERVER   = os.getenv("AZURE_SQL_SERVER")
DATABASE = os.getenv("AZURE_SQL_DATABASE")
USERNAME = os.getenv("AZURE_SQL_USERNAME")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
DRIVER   = os.getenv("AZURE_SQL_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))

# какие файлы грузим -> в какие таблицы
FILES = [
    ("application_train.csv",     "raw.application_train"),
    ("installments_payments.csv", "raw.installments_payments"),
]

# 3) подключение к Azure SQL (безопасно через odbc_connect)
odbc_conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER=tcp:{SERVER},1433;"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};PWD={PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)
engine = create_engine("mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_conn_str))

# ускорение массовых вставок
@event.listens_for(engine, "before_cursor_execute")
def _fast_execmany(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        try:
            cursor.fast_executemany = True
        except Exception:
            pass

# 4) вспомогательные функции
def ensure_schema_raw():
    """создаёт схему raw при её отсутствии"""
    with engine.begin() as conn:
        conn.execute(text(
            "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='raw') EXEC('CREATE SCHEMA raw');"
        ))

def ensure_raw_table(csv_path: Path, table_fullname: str):
    """создаёт raw.<table> по заголовку CSV: все колонки NVARCHAR(MAX)"""
    df_head = pd.read_csv(csv_path, nrows=50)
    schema, table = table_fullname.split(".")
    cols = ", ".join(f"[{c}] NVARCHAR(MAX) NULL" for c in df_head.columns)
    sql = f"IF OBJECT_ID('{schema}.{table}','U') IS NULL CREATE TABLE {schema}.[{table}] ({cols});"
    with engine.begin() as conn:
        conn.execute(text(sql))

def resolve_csv(name: str) -> Path:
    """ищет файл в DATA_DIR (учитывает .csv и регистр)"""
    p = DATA_DIR / name
    if p.exists():
        return p
    p2 = DATA_DIR / f"{name}.csv"
    if p2.exists():
        return p2
    for f in DATA_DIR.glob("*.csv"):
        if f.stem.lower() == name.replace(".csv", "").lower():
            return f
    raise FileNotFoundError(f"CSV not found: {name} in {DATA_DIR}")

def load_csv(csv_path: Path, table_fullname: str, chunksize: int = 100_000):
    """порционная загрузка CSV в raw.*; NaN -> NULL"""
    schema, table = table_fullname.split(".")
    print(f"-> Loading {csv_path.name} into {table_fullname} ...")
    try:
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk = chunk.astype(str).where(chunk.notna(), None)
            chunk.to_sql(table, engine, schema=schema, if_exists="append", index=False)
    except Exception as e:
        print(f"!! Load failed on {csv_path.name}: {e}")
        engine.dispose()  # сброс соединения, чтобы не ловить PendingRollback
        raise
    print("   done.")

# 5) точка входа
def main():
    assert SERVER and DATABASE and USERNAME and PASSWORD, "Missing DB env vars."
    assert DATA_DIR.exists(), f"DATA_DIR does not exist: {DATA_DIR}"

    ensure_schema_raw()

    for fname, table in FILES:
        csv = resolve_csv(fname)
        ensure_raw_table(csv, table)
        load_csv(csv, table)

    print("RAW load finished ✅")

if __name__ == "__main__":
    main()
