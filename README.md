# 🏦 End-to-End Credit Risk Pipeline (PD Model)

This project demonstrates a complete **end-to-end Credit Risk Pipeline** for credit risk modeling — from raw loan data to Probability of Default (PD) scores integrated into a Azure SQL Data Warehouse.

---

## 🎯 Project Objective

The goal of this project is to simulate a **bank-grade PD scoring process** used in retail credit risk management:

- Integrate application and repayment data into a clean analytical dataset
- Train a statistical model (Logistic Regression) to predict loan default probability
- Evaluate model quality with key metrics (ROC-AUC, precision/recall)
- Generate PD scores and store them back into a **data warehouse (Azure SQL)**
- Create SQL views for seamless use in BI dashboards or downstream analytics

---

## 🧱 Project Architecture
```
credit-risk-pipeline/
│
├── data/
│ ├── application_train.csv
│ ├── installments_payments.csv
│ └── model_dataset.csv ← processed dataset with PD scores
│
├── sql/
│ ├── 01_create_schemas.sql ← schema creation (dwh, staging)
│ ├── 02_build_dwh.sql ← data warehouse tables & joins
│ └── 03_views_pd.sql ← PD view + staging table
│
├── src/
│ └── train_pd.py ← Python training & scoring pipeline
│
├── .env ← credentials (Azure SQL)
├── requirements.txt ← Python dependencies
└── README.md

---

## ⚙️ Data Flow Overview

1. **SQL scripts** prepare raw data and create warehouse schemas:
   - `v_model_dataset` – feature store for model training  
   - `stg_pd_scores` – staging table for PD predictions  
   - `v_pd_scores` – final analytical view (merged features + scores)

2. **Python pipeline (`src/train_pd.py`)**:
   - Loads dataset from SQL  
   - Handles missing values and categorical encoding  
   - Trains Logistic Regression (`sklearn`)  
   - Evaluates model performance  
   - Writes PD scores back to SQL

---

## 📊 Model Evaluation

| Metric | Value |
|--------|-------|
| **ROC-AUC** | 0.63 |
| **Accuracy** | 0.59 |
| **Recall (Default=1)** | 0.60 |
| **Precision (Default=1)** | 0.11 |
| **Weighted F1** | 0.68 |

**Confusion Matrix:**
[[41596 29076]
[ 2459 3747]]


➡️ Interpretation:  
The model identifies risky loans with ~60% recall at early stage — suitable for use as a **screening PD model** before more complex segmentation (LGD/EAD modeling).

---

## 💾 SQL Integration Example

After running the Python pipeline, the view `dwh.v_pd_scores` contains:

| loan_id | client_id | gender | income | pd_score | default_flag |
|----------|------------|---------|---------|-----------|---------------|
| 100002 | 100002 | M | 202500 | 0.639 | 1 |
| 100003 | 100003 | F | 270000 | 0.226 | 0 |
| 100004 | 100004 | M | 67500  | 0.551 | 0 |

---

## 🧩 Technologies Used

| Category | Tools |
|-----------|-------|
| **Programming** | Python 3.11, pandas, scikit-learn |
| **Database** | Azure SQL, SQLAlchemy, pyodbc |
| **Modeling** | Logistic Regression (PD scoring) |
| **Preprocessing** | Imputation, OneHotEncoding, Standard Scaling |
| **Deployment** | SQL integration, DWH views for BI tools (Power BI, Tableau) |

---

## 📈 Next Steps

- Add **LGD / EAD** modeling modules  
- Experiment with **Gradient Boosting** for improved AUC  
- Add **Power BI dashboard** for credit portfolio risk monitoring  
- Deploy as **scheduled job in Azure Data Factory**

---

## 👤 Author

**Erkingali Aldybayev**  
Data Analyst / Risk Modeler    
💼 Focus: Credit Risk, Marketing Analytics, Data Pipelines  
🔗 [LinkedIn Profile](https://www.linkedin.com/in/yerkingali-aldybayev-8473611b8/)  
