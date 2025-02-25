
# Data Lakehouse and Analytics Project

Welcome to the **Global Auto X Project** repository! 🚀  
This project builds a modern data lakehouse using Azure Databricks to consolidate car sales data from GitHub, enabling analytical reporting and business insights through Power BI. The pipeline processes data through Bronze, Silver, and Gold layers in ADLS Gen2, leveraging Azure Data Factory (ADF) for automation and PySpark with Delta Lake for transformations.

---
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Data Architecture](docs/Data_Architecture.drawio.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from Azure SQL DB to ADLS Gen2.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ELT Pipelines**: Extracting, loading and transforming data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating Power BI reports and dashboards for actionable insights.


---

## 🚀 Project Requirements

### Building the Data Lakehouse (Data Engineering)

#### Objective
A national car dealership company, "GLobal Auto X," wants to analyze sales performance across branches, dealers, and car models to optimize resource allocation and identify growth opportunities. Key challenges include:

1. **Branch Performance:** Compare branches by revenue, units sold, and revenue per unit (efficiency).
2. **Model Popularity:** Identify top-selling car model categories and their contribution to revenue.
3. **Dealer Efficiency:** Analyze dealers with the highest revenue per unit to reward top performers.
4. **Correlations:** Explore relationships between branch, model, and dealer performance.

#### Specifications
- **Data Source**: Import data from GitHub into Azure SQL DB.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Scope**: Focus on the implementing incremental load.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.
---
