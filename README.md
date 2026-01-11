# End-to-End Data Engineering Pipeline: MySQL → GCS → BigQuery

## 📌 Project Overview
This project demonstrates the design and implementation of an end-to-end data engineering pipeline that extracts transactional data from a MySQL database, processes it using modern data engineering best practices, and delivers analytics-ready datasets for business intelligence and reporting in Looker Studio.

The pipeline follows the **Medallion Architecture (Bronze, Silver, Gold)** to ensure data reliability, scalability, and analytical value. All workflows are orchestrated using **Apache Airflow**, containerized with **Docker**, and transformations are handled using **dbt**.

---

## ❓ Problem Statement
Organizations often struggle to:
- Integrate data from operational databases into analytics platforms
- Ensure data quality and consistency across pipelines
- Scale transformations and analytics as data volume grows
- Provide trustworthy, business-ready datasets for reporting

This project addresses these challenges by building a robust, automated, and scalable data pipeline from source systems to analytics dashboards.

---

## 🎯 Objectives
- Extract structured data from a MySQL transactional database  
- Store raw data in a cloud-based data lake (Google Cloud Storage)  
- Load and transform data into an analytics-ready data warehouse (BigQuery)  
- Apply data modeling best practices using the Medallion Architecture  
- Orchestrate the entire workflow with Apache Airflow  
- Enable business insights through Looker Studio dashboards  

---

## 🛠️ Solution Approach

### 1. Data Extraction
- Source: **MySQL database**
- Data includes:
  - Orders
  - Customers
  - Products
  - Shippers
  - Orders Details
  - Categories (Product related)
- Before extraction, the pipeline checks existing records in **BigQuery**
- **JSON files are only created in GCS if new or updated records exist** compared to what is already stored in BigQuery using updated_at column
- This ensures:
  - Incremental ingestion
  - Reduced storage costs
  - Faster pipeline execution
  - No duplicate data processing

### 2. Data Lake Storage (Landing Zone)
- Raw incremental data is stored in **Google Cloud Storage (GCS)** as JSON files
- Data remains in its original structure
- Enables replayability and auditability

### 3. Data Warehouse Loading
- JSON files are loaded from GCS into **BigQuery** (**Bronze**)
- Tables are partitioned and structured for efficient querying

### 4. Data Transformation with dbt
Transformations are implemented using **dbt**, following the Medallion Architecture:
- **Silver**: Cleaned, standardized, and deduplicated data  
- **Gold**: Business-focused fact and dimension tables optimized for analytics  

### 5. Orchestration & Deployment
- **Apache Airflow** orchestrates ingestion, loading, and transformation workflows  
- **Docker** is used to containerize Airflow, ensuring consistent local and cloud execution  

### 6. Analytics & Visualization
- Final Gold-layer datasets are connected to **Looker Studio**
- Dashboards provide actionable business insights

---

## 🏗️ Architecture

<img width="1357" height="782" alt="arquitetura" src="https://github.com/user-attachments/assets/3d277ef0-1cb6-41d7-8e3a-36041a40ab23" />

---

## 📊 Key Results
- Fully automated, end-to-end data pipeline  
- Clean separation of raw, refined, and business-ready data  
- Scalable and maintainable transformation logic using dbt  
- Reliable analytics-ready datasets for reporting  
- Interactive dashboards built in Looker Studio delivering:
  - Product performance metrics  

---

## 🚀 Tech Stack
- **Database**: MySQL  
- **Data Lake**: Google Cloud Storage (GCS)  
- **Data Warehouse**: BigQuery  
- **Transformations**: dbt  
- **Orchestration**: Apache Airflow  
- **Containerization**: Docker  
- **Visualization**: Looker Studio  

---

## 📈 Future Improvements
- Implement data quality tests and alerts in dbt  
- Add CI/CD for dbt models  
- Expand dashboards with advanced KPIs

<br>
### Figures

#### Figure 1: Main Workflow Orchestration
<img width="1877" height="907" alt="Dags" src="https://github.com/user-attachments/assets/19a750aa-2d0b-48de-a019-71b9f3e8037a" />

#### Figure 2: Airflow DAG in Execution (skip load and load data shown)
<img width="1888" height="898" alt="Exemplo Dag Categories" src="https://github.com/user-attachments/assets/7911acc6-4466-43a0-a420-362e4e31cee9" />

#### Figure 3: Data Lake
<img width="1902" height="803" alt="Data Lake" src="https://github.com/user-attachments/assets/8225f48e-195c-4952-8cba-2eee04818cf7" />

#### Figure 4: Data Warehouse
<img width="1895" height="796" alt="DataWarehouse Bigquery" src="https://github.com/user-attachments/assets/1b7ab71d-3cff-477b-b1e9-c3445cb6e440" />

#### Figure 4: Visualization Example
<img width="673" height="388" alt="Looker studio exemplo de visualizaçao" src="https://github.com/user-attachments/assets/6dc8be49-e460-4b3d-9ece-c12c59ccc65c" />

