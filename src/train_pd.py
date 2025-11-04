# src/train_pd.py
import os
import urllib.parse
import numpy as np
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text, event


# --- 1) ENV / подключение к Azure SQL ---
load_dotenv()

server   = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver   = os.getenv("AZURE_SQL_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

print("ENV CHECK:")
print("  AZURE_SQL_SERVER =", server)
print("  AZURE_SQL_DATABASE =", database)

odbc = (
    f"DRIVER={{{driver}}};SERVER=tcp:{server},1433;DATABASE={database};"
    f"UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)
engine = create_engine("mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc))

# --- 2) Забираем витрину из DWH ---
df = pd.read_sql("SELECT * FROM dwh.v_model_dataset", engine)
print(df.shape)
print(df.head())

# --- 3) Обучаем PD-модель (логрегрессия) ---
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix
from sklearn.impute import SimpleImputer
import joblib

target = "default_flag"
y = df[target].astype(int)

# признаки
X = df.drop(columns=["loan_id", "client_id", target]).copy()
# защита от бесконечностей -> в NaN (дальше заимпутим)
X = X.replace([np.inf, -np.inf], np.nan)

num_cols = X.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = [c for c in X.columns if c not in num_cols]

# Пайплайны препроцессинга: импутация + масштабирование/кодирование
numeric_tf = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median")),
    ("scale",   StandardScaler(with_mean=False))
])
categorical_tf = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("ohe",     OneHotEncoder(handle_unknown="ignore"))
])

preprocess = ColumnTransformer([
    ("num", numeric_tf, num_cols),
    ("cat", categorical_tf, cat_cols)
])

model = Pipeline([
    ("prep", preprocess),
    ("clf", LogisticRegression(max_iter=300, class_weight="balanced", random_state=42))
])

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, stratify=y, random_state=42
)
model.fit(X_train, y_train)

# --- 4) Оцениваем качество ---
proba_test = model.predict_proba(X_test)[:, 1]
print("ROC-AUC:", roc_auc_score(y_test, proba_test))
print(classification_report(y_test, (proba_test >= 0.5).astype(int)))
print("Confusion matrix:\n", confusion_matrix(y_test, (proba_test >= 0.5).astype(int)))

# --- 5) Сохраняем модель ---
os.makedirs("models", exist_ok=True)
joblib.dump(model, "models/pd_model.pkl")

# --- 6) Считаем PD по всей выборке и сохраняем CSV ---
df["pd_score"] = model.predict_proba(X)[:, 1]
print("Sample PD scores:")
print(df[["loan_id", "pd_score"]].head())

project_dir = os.path.dirname(os.path.dirname(__file__))   # ../ (в корень репо)
output_path = os.path.join(project_dir, "data", "model_dataset.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False)
print(f"✅ Saved to {output_path}")

# --- 7) Заливаем PD в dwh.stg_pd_scores (для вьюхи dwh.v_pd_scores) ---

# 7.0 Ускоряем executemany для pyodbc
@event.listens_for(engine, "before_cursor_execute")
def _fastexec(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        try:
            cursor.fast_executemany = True
        except Exception:
            pass

# 7.1 TRUNCATE отдельной операцией (AUTOCOMMIT), чтобы не держать длинную транзакцию
with engine.connect() as conn:
    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(text("TRUNCATE TABLE dwh.stg_pd_scores;"))
print("🧹 Truncated dwh.stg_pd_scores")

# 7.2 Вставка по чанкам через сырой pyodbc + fast_executemany
to_upload = (
    df[["loan_id", "pd_score"]]
    .astype({"loan_id": int, "pd_score": float})
)

rows = list(map(tuple, to_upload.itertuples(index=False, name=None)))
BATCH = 50_000
sql_ins = "INSERT INTO dwh.stg_pd_scores (loan_id, pd_score) VALUES (?, ?)"

raw = engine.raw_connection()  # нативный pyodbc-connection
try:
    cur = raw.cursor()
    cur.fast_executemany = True
    total = len(rows)
    for start in range(0, total, BATCH):
        part = rows[start:start+BATCH]
        cur.executemany(sql_ins, part)
        raw.commit()  # коммит каждого чанка
        print(f"⬆️ Uploaded {min(start+BATCH, total):,}/{total:,}")
    cur.close()
    raw.close()
except Exception:
    try:
        raw.rollback()
    except Exception:
        pass
    raise

print("✅ PD scores successfully uploaded to dwh.stg_pd_scores")

# --- 8) Быстрый просмотр топ-скорингов во вьюхе ---
check_df = pd.read_sql(
    "SELECT TOP 10 * FROM dwh.v_pd_scores ORDER BY pd_score DESC",
    engine
)
print(check_df)
