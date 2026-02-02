# 🏗️ Python Data Warehouse (Medallion Architecture)

A **production‑style Python Data Warehouse** implementing the **Medallion Architecture (Raw → Silver → Gold)** with **class‑based pipelines**, **config‑driven processing**, **logging**, and **batch automation**.

This project is designed to look and behave like a **real industry data platform**, suitable for **portfolio, internships, and interviews**.

---

## 📁 Recommended GitHub Folder Structure

```text
PYTHON_DATA_WAREHOUSE/
│
├── data/                         # Data layers (ignored in GitHub)
│   ├── raw/                      # Raw source data
│   │   ├── crm/
│   │   └── erp/
│   ├── silver/                   # Cleaned & standardized data
│   │   ├── crm/
│   │   └── erp/
│   └── gold/                     # Analytics-ready data
│
├── logs/                         # Execution & quality logs
│   ├── silver.log
│   ├── gold.log
│   └── test_silver.log
│
├── notebooks/                    # EDA & experimentation
│   ├── exploration.ipynb
│   └── scratch.ipynb
│
├── scripts/                      # One‑click execution
│   ├── run_dw.bat                # Run full pipeline
│   ├── run_silver.bat            # Run Silver layer
│   └── run_gold.bat              # Run Gold layer
│
├── src/                          # Core source code
│   ├── common/                   # Shared utilities
│   │   ├── logger.py
│   │   ├── file_utils.py
│   │   └── constants.py
│   │
│   ├── silver/                   # Silver layer
│   │   ├── base/
│   │   │   └── base_silver_pipeline.py
│   │   ├── config/
│   │   │   ├── crm.yaml
│   │   │   └── erp.yaml
│   │   ├── crm/
│   │   │   ├── customers.py
│   │   │   ├── orders.py
│   │   │   ├── order_items.py
│   │   │   ├── kitchen_logs.py
│   │   │   └── customer_reviews.py
│   │   ├── erp/
│   │   │   ├── suppliers.py
│   │   │   ├── ingredients.py
│   │   │   ├── inventory.py
│   │   │   ├── employees.py
│   │   │   ├── delivery_partners.py
│   │   │   └── restaurants.py
│   │   └── main.py               # Silver orchestration
│   │
│   ├── gold/                     # Gold layer
│   │   ├── base/
│   │   │   └── base_gold_pipeline.py
│   │   ├── config/
│   │   │   ├── dim.yaml
│   │   │   └── fact.yaml
│   │   ├── dim/
│   │   │   ├── dim_customers.py
│   │   │   ├── dim_date.py
│   │   │   ├── dim_restaurants.py
│   │   │   └── dim_payment_mode.py
│   │   ├── fact/
│   │   │   └── fact_sales.py
│   │   └── main.py               # Gold orchestration
│
├── tests/                        # Unit & quality tests
│   ├── test_silver_quality.py
│   └── test_gold_logic.py
│
├── .gitignore
├── requirements.txt
├── README.md
└── LICENSE
```

---

## 🧠 Architecture Overview

## 📂 Data Folder Structure (Detailed)

```text
data/
├── raw/                          # Source data (as-is)
│   ├── crm/                      # Customer-facing systems
│   │   ├── customers.csv.gz
│   │   ├── orders.csv.gz
│   │   ├── order_items.csv.gz
│   │   ├── kitchen_logs.csv.gz
│   │   └── customer_reviews.csv.gz
│   │
│   └── erp/                      # Backend / operations systems
│       ├── suppliers.csv.gz
│       ├── supplier_ingredients.csv.gz
│       ├── ingredients.csv.gz
│       ├── inventory.csv.gz
│       ├── employees.csv.gz
│       ├── delivery_partners.csv.gz
│       ├── restaurants.csv.gz
│       ├── menu_items.csv.gz
│       └── recipe.csv.gz
│
├── silver/                       # Cleaned & standardized (Parquet)
│   ├── crm/
│   │   ├── customers.parquet
│   │   ├── orders.parquet
│   │   ├── order_items.parquet
│   │   ├── kitchen_logs.parquet
│   │   └── customer_reviews.parquet
│   │
│   └── erp/
│       ├── suppliers.parquet
│       ├── supplier_ingredients.parquet
│       ├── ingredients.parquet
│       ├── inventory.parquet
│       ├── employees.parquet
│       ├── delivery_partners.parquet
│       ├── restaurants.parquet
│       ├── menu_items.parquet
│       └── recipe.parquet
│
└── gold/                         # Analytics-ready (Star Schema)
    ├── dim_customers.parquet
    ├── dim_date.parquet
    ├── dim_restaurants.parquet
    ├── dim_employees.parquet
    ├── dim_delivery_partners.parquet
    ├── dim_menu_items.parquet
    ├── dim_payment_mode.parquet
    ├── dim_order_status.parquet
    └── fact_sales.parquet
```

### Why this design works
- Raw data remains **immutable (बदला नहीं जाता)**
- Silver layer ensures **clean, typed, deduplicated** data
- Gold layer follows **Star Schema** for BI tools

---



### 🔹 Raw Layer
- Stores **as‑received source data**
- No transformations
- Acts as **system of record**

### 🔸 Silver Layer
- Data **cleaning (सफाई)**
- Standardization (data types, formats)
- Deduplication & validation
- **Class‑based pipelines with inheritance**

### 🟡 Gold Layer
- Business‑ready **fact & dimension tables**
- Star schema modeling
- Optimized for **analytics & BI**

---

## ⚙️ Key Features

- ✅ Medallion Architecture (industry standard)
- ✅ Class‑based pipeline design
- ✅ Config‑driven processing (YAML)
- ✅ Centralized logging (no print spam)
- ✅ Data quality checks
- ✅ Batch automation using `.bat` files
- ✅ Scalable & interview‑ready design

---

## 🧩 Base Pipeline Design

### Silver Pipeline
```python
class BaseSilverPipeline(ABC):
    @abstractmethod
    def extract(self): ...

    @abstractmethod
    def clean(self, df): ...

    @abstractmethod
    def validate(self, df): ...

    @abstractmethod
    def load(self, df): ...
```

Each table pipeline:
- Inherits from `BaseSilverPipeline`
- Implements **only table‑specific logic**

Same concept applies to **Gold pipelines**.

---

## 🗂️ Config‑Driven Processing (YAML)

Example: `crm.yaml`
```yaml
customers:
  primary_key: customer_id
  required_columns:
    - customer_id
    - name
    - phone
```

Benefits:
- No hardcoding ❌
- Easy schema change ✔️
- Production‑like design ✔️

---

## 🧪 Data Quality Checks

- Null percentage
- Duplicate primary keys
- Invalid phone/email
- Row count validation

Sample log:
```text
08:55:56 | INFO | SILVER | CUSTOMERS | rows=100 | nulls=2 | dup_customer_id=0 | invalid_phone=98
```

---

## ▶️ How to Run

### Run complete warehouse
```bat
scripts\run_dw.bat
```

### Run only Silver layer
```bat
scripts\run_silver.bat
```

### Run only Gold layer
```bat
scripts\run_gold.bat
```

---

## 📊 Analytics Output (Gold)

- `dim_customers`
- `dim_date`
- `dim_restaurants`
- `fact_sales`

Ready for:
- Power BI
- Tableau
- SQL Analytics

---

## 🛠️ Tech Stack

- Python (Pandas, NumPy)
- YAML (configuration)
- Logging module
- Batch scripting (.bat)
- MySQL / CSV (storage)

---

## 👤 Author

**Nishant Singh**  
Data Analytics


