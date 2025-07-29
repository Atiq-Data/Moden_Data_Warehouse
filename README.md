# Modern_Data_Warehouse

Welcome to the **Modern Data Warehouse Project**! Here you’ll find a full-stack demonstration of building a modern data warehouse, implementing robust ETL pipelines, and crafting insightful analytics. This portfolio-grade solution highlights industry best practices in data engineering and data analytics.
## 🏗️ Medollian Data Warehouse Structure
Our warehouse design follows the **Medollian Architecture**, which organizes data into three progressive layers:
**Bronze (Raw)**  
  Ingests source data exactly as it arrives (CSV files, database exports, etc.). Keeps a faithful archive of raw records.
**Silver (Cleansed & Standardized)**  
  Applies data validation, cleansing, and normalization steps. Ensures consistency and prepares data for analytics use.
**Gold (Business-Ready)**  
Transforms Silver data into star schema models (fact and dimension tables) tailored for reporting and BI tools.
This layering ensures traceability from raw to refined data, simplifies debugging, and scales gracefully as data volume grows.
## 📖 Project Highlights
**Data Architecture**  
  Architected with Medollian layers to enforce clear separation of concerns and maintain auditability.
**ETL Pipelines**  
  Automated scripts to extract CSV datasets, clean & transform records, and load into SQL Server.
**Data Modeling**  
  Star schema design with fact tables for events (sales, transactions) and dimension tables for entities (customers, products).
## 🎯 Who Should Explore This Repo?
Aspiring **Data Engineers** and **Architects**  
SQL/BI developers building enterprise-grade warehouses  
Analysts eager to learn end-to-end data workflows  
Students and professionals crafting portfolio projects
## 🛠️ Tools & Resources
**Datasets**: CSV files for ERP and CRM exports  
**SQL Server Express**: Local data store  
**SQL Server Management Studio (SSMS)**: Interactive query and management  
**Draw.io**: Architecture & data flow diagrams  
**Notion**: Project planning templates
## 📂 Repository Structure

```
Modern_data_warehouse_project/
│
├── datasets/           # Source CSVs (ERP, CRM)
├── docs/               # Draw.io files & documentation
│   ├── data_architecture.drawio
│   ├── data_flow.drawio
│   └── data_catalog.md
├── scripts/            # ETL & orchestration scripts
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── tests/              # Data quality and integration tests
├── README.md           # This file
└── LICENSE             # MIT License details
```
## 🛡️ License
This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for full details.  
## About Me
I am a Business Intelligence Developer passionate about translating complex datasets into clear, action-oriented insights that drive strategic decision-making. With strong skills in SQL, ETL pipeline design, and modern BI tools, I empower teams to harness data effectively and unlock business value.
