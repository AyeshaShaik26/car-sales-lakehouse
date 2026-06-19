# Car Sales Lakehouse

## Project Overview

Built an end-to-end Azure Data Engineering solution to ingest, transform, govern, and model car sales data using Azure Data Factory, Azure SQL Database, Azure Data Lake Storage Gen2, Azure Databricks, Delta Lake, and Unity Catalog.

The solution implements a Medallion Architecture (Bronze, Silver, Gold) and transforms operational data into analytics-ready dimensional models for reporting and business analysis.

---

## Business Problem

Car sales information is generated from multiple operational systems and external APIs. Business users require a centralized and trusted data platform to analyze vehicle sales performance, dealer performance, branch performance, and customer purchasing trends.

This project addresses the challenge by building a scalable lakehouse architecture that consolidates, cleanses, and models data for analytical consumption.

---

## Solution Architecture

REST API

↓

Azure Data Factory (Dynamic Parameterized Ingestion)

↓

Azure SQL Database (Source Preparation Layer)

↓

Azure Data Lake Storage Gen2 - Bronze Layer

↓

Azure Databricks - Silver Layer

↓

Azure Databricks - Gold Layer

↓

Star Schema Data Model

---

## Technologies Used

* Azure Data Factory
* Azure SQL Database
* Azure Data Lake Storage Gen2
* Azure Databricks
* Delta Lake
* Unity Catalog
* PySpark
* SQL
* GitHub

---

## Key Features

### Dynamic API Ingestion

Implemented parameterized Azure Data Factory pipelines to dynamically ingest data from REST APIs.

### Source Preparation Layer

Loaded API data into Azure SQL Database for source preparation and standardization before data lake ingestion.

### Medallion Architecture

Implemented Bronze, Silver, and Gold layers using Delta Lake.

### Incremental Processing

Designed incremental data loading logic to process only newly arrived records.

### Data Cleansing and Transformation

Performed data standardization, cleansing, and business transformations in the Silver layer.

### Data Quality Checks

Implemented validation checks including:

* Null checks
* Duplicate checks
* Record count validation
* Schema validation

### Dimensional Modeling

Created a Star Schema model in the Gold layer.

Dimension Tables:

* DimModel
* DimBranch
* DimDealer
* DimDate

Fact Tables:

* FactSales

### Unity Catalog Implementation

Configured Unity Catalog for:

* Centralized metadata management
* Data lineage tracking
* Data governance
* Access control

### Security

Implemented secure access using Managed Identity authentication instead of hard-coded credentials.

---

## Data Flow

1. Extract data from REST API using Azure Data Factory.
2. Load data into Azure SQL Database for source preparation.
3. Ingest prepared data into ADLS Bronze layer.
4. Transform and cleanse data in Databricks Silver layer.
5. Create analytical dimensional models in Databricks Gold layer.
6. Store all assets within Unity Catalog for governance and lineage tracking.

---

## Data Volume

Approximately 50,000 records processed through the pipeline.

---

## Future Enhancements

* SCD Type 2 Implementation
* dbt Integration
* CI/CD using Azure DevOps
* Power BI Reporting Layer
* Automated Monitoring and Alerting
