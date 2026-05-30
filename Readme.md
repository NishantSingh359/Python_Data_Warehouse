# 🏗️ Food Delivery Data Warehouse

A **Food Delivery Data Warehouse** implementing the **Medallion Architecture (Raw → Silver → Gold)** with **class‑based pipelines**, **config‑driven processing**, **logging**, and **batch automation**.
---

## 🧠 Architecture Overview

### 🔹 Raw Layer
- Stores **as‑received source data**
- No transformations
- Acts as **system of record**

### 🔸 Silver Layer
- Data **cleaning**
- Standardization (data types, formats)
- Deduplication & validation
- **Class‑based pipelines with inheritance**

### 🟡 Gold Layer
- Business‑ready **fact & dimension tables**
- Star schema modeling
- Optimized for **analytics & BI**

---

## 🎯 ARCHITECTURE DIAGRAM

```
            ┌──────────────┐
            │     RAW      │
            │   (Dirty)    │
            └──────┬───────┘
                   │
                   ▼
            ┌──────────────┐
            │    SILVER    │
            │  (Cleaned)   │
            └──────┬───────┘
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
 ┌──────────┐ ┌──────────┐ ┌──────────┐
 │  SALES   │ │LOGISTICS │ │   OPS    │  
 │   MART   │ │   MART   │ │   MART   │
 └──────────┘ └──────────┘ └──────────┘
```

## 📊 Data Marts
### 1️⃣ Sales Mart

Focus: Revenue & transactions

* fact_sales
* dim_customer
* dim_restaurant
* dim_item
* dim_date

---

### 2️⃣ Logistics Mart

Focus: Delivery performance

* fact_delivery
* dim_delivery_partner
* dim_date
* dim_restaurant

---

### 3️⃣ Ops Mart

Focus: Kitchen operations

* fact_kitchen
* dim_chef
* dim_item
* dim_restaurant
* dim_date


--- 

## ⚙️ Key Features

- ✅ Medallion Architecture
- ✅ Class‑based pipeline design
- ✅ End-to-end ETL pipeline
- ✅ Config‑driven processing (YAML)
- ✅ Centralized logging
- ✅ Star schema modeling
- ✅ Multiple data marts
- ✅ Batch automation using `.bat` files

---

## Ready for:
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

## 📌 Dataset

Raw data files are not included in this repository due to size constraints.

### Download Dataset

| Source | Link |
|--------|------|
| 🔗 CRM Dataset | [Restaurant CRM Raw Dataset](https://www.kaggle.com/datasets/nishantsinghpro/restaurant-crm-raw-dataset) |
| 🔗 ERP Dataset | [Restaurant ERP Raw Dataset](https://www.kaggle.com/datasets/nishantsinghpro/restaurant-erp-raw-dataset) |

--- 


## 👤 Author

**Nishant Singh**  







