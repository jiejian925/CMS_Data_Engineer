# CMS Open Payments Research Data Pipeline & Analysis

## Project Overview

This project implements an end-to-end data engineering pipeline to analyze research-related payments recorded in the CMS (Centers for Medicare & Medicaid Services) Open Payments dataset. The goal is to provide insights into the financial relationships between healthcare manufacturers/GPOs and teaching hospitals/research entities from 2017 to 2023.

## 1. Problem Description

**Problem:** The CMS Open Payments program collects and makes publicly available extensive data on financial transactions between drug and medical device companies (manufacturers/GPOs) and healthcare providers (physicians, teaching hospitals). While this data promotes transparency, its raw format is large, complex, and challenging to analyze directly, especially for specific areas like research payments over multiple years. Understanding the scale, trends, and distribution of these research payments requires significant data processing and structuring.

**Solution:** This project addresses this challenge by building an automated data pipeline that:
1.  **Ingests** research payment data spanning multiple years (2017-2023).
2.  **Stores** the raw data efficiently in a data lake.
3.  **Loads** the data into a structured data warehouse.
4.  **Transforms** the data for analytical use, focusing on key entities like manufacturers and hospitals.
5.  **Visualizes** key metrics and trends through an interactive dashboard.

The project provides a clear, aggregated view of research funding flows, enabling easier exploration of total spending and transaction volumes by manufacturers and recipient hospitals/research entities over time.

## 2. Pipeline Details

### 1. Data Ingestion (Batch)
- **Extraction & Upload:** An Airflow DAG extracts the CMS research payments dataset and uploads it to a designated GCS bucket.
- **Batch Mode:** The pipeline runs in batch mode, orchestrated by Airflow on a GCP Cloud VM, ensuring scalability and reliability.

### 2. Data Warehouse (BigQuery)
- **Loading:** Data is transferred from the GCS data lake to BigQuery.
- **Optimization:**
  - **Partitioning:** Tables are partitioned by date to improve query efficiency.
  - **Clustering:** Key columns (manufacturer and hospital) are used for clustering to accelerate analytical queries.

### 3. Data Transformation (dbt Cloud)
- **Transformations:** dbt Cloud models transform the raw data into:
  - A fact table (`fact_rsrch`) capturing research payments.
  - Analytical tables that further prepare the data for visualization.
- **Modularity:** Each transformation is version-controlled and testable.

### 4. Dashboard Visualization (Looker Studio)
- **Dashboard Composition:** The dashboard includes:
  - **Categorical Tile:** Graph showing the distribution of research payment spend across manufacturers or hospitals.
  - **Temporal Tile:** Graph showing the trend of research payment records over time.
- **User-Focused:** The dashboard is designed with clear titles, references, and legends for ease of interpretation.
