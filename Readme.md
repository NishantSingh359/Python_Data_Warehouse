# 🏗️ Python Data Warehouse (Medallion Architecture)

A **Python Data Warehouse** implementing the **Medallion Architecture (Raw → Silver → Gold)** with **class‑based pipelines**, **config‑driven processing**, **logging**, and **batch automation**.
---

## 📁 Folder Structure

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
domain: CRM
tables:
  customers:
    raw_path: data/raw/crm/customers.csv.gz
    silver_path: data/silver/crm/customers.parquet
    dq_threshold: 5
```

Benefits:
- No hardcoding ❌
- Easy changes ✔️
- Production‑like design ✔️

---

## 🧪 Data Quality Checks

- Null percentage
- Duplicate primary keys
- Invalid phone/email
- Row count validation

Sample log:
```text
16:53:57 | WARNING | SILVER | ERP | QUALITY | restaurants | restaurant_type | invalid
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







