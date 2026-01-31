# 📊 ITJOBANALYSE: Data Lakehouse & Job Demand Prediction System

![Project Status](https://img.shields.io/badge/Status-Active-brightgreen)
![Docker](https://img.shields.io/badge/Containerized-Docker-blue)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

## 📖 Introduction

**ITJOBANALYSE** is a modern Data Lakehouse project designed to collect, process, and analyze IT recruitment data in Vietnam (sourced from TopDev, VietnamWorks). By leveraging the **Medallion Architecture**, the system transforms raw data into actionable insights.

A key feature of this project is the integration of the **Facebook Prophet** machine learning algorithm to forecast future recruitment trends, helping decision-makers understand the labor market demand.


## 🏗️ System Architecture

The entire system is deployed on a **Docker** infrastructure, ensuring reproducibility and scalability. It integrates **Apache Iceberg** to bring ACID transactions to the Data Lake.
<img width="940" height="468" alt="image" src="https://github.com/user-attachments/assets/65859fef-a3dd-46f4-9e5a-f4d196353d71" />

🛠️ Tech Stack

Component,Technology,Role
Storage,Hadoop HDFS,Distributed physical storage for raw and processed data.
Table Format,Apache Iceberg,"Provides ACID transactions, time travel, and schema evolution."
Processing,Apache Spark,Heavy ETL processing and Machine Learning model training.
Orchestration,Apache Airflow,Workflow management and scheduling of data pipelines.
Query Engine,Trino,High-performance distributed SQL query engine for interactive analytics.
Visualization,Apache Superset,Business Intelligence (BI) tool for dashboards and reporting.
Metadata,Hive Metastore,Central repository for metadata management.
Algorithm,Prophet,Time-series forecasting for job demand prediction.

🔄 Data Pipeline (Medallion Architecture)
The data flows through a structured Bronze-Silver-Gold pipeline managed by Apache Iceberg:

Bronze Layer (Raw): Ingests raw recruitment data (CSV, JSON, Scraped Data) into HDFS.

Silver Layer (Cleansed): Spark cleans, deduplicates, and validates data. Stored as Iceberg tables.

Gold Layer (Aggregated): Business-level aggregations ready for analysis (e.g., "Jobs by Industry," "Salary Trends").

Prediction Layer: Spark ML runs the Prophet algorithm on historical data to generate future demand forecasts, storing results back into the Gold layer.

🤖 Machine Learning: Job Demand Prediction
We utilize Facebook Prophet, a robust additive model for time series forecasting, to predict labor market trends.

Input: Historical job posting counts, seasonal trends, and industry categories.

Process: The model is trained periodically via Airflow DAGs using PySpark.

Output: Forecasted number of job openings for the next [X] months.

Visualization: Predicted trends are visualized in Superset alongside historical data.

🚀 Getting Started
Prerequisites
Docker & Docker Compose (allocated at least 8GB RAM).

Git.

Installation
Clone the repository:

Bash

git clone https://github.com/your-username/job-demand-datalakehouse.git

cd job-demand-datalakehouse

Start the environment:

Bash
docker-compose up -d

Access the interfaces:

Airflow: http://localhost:8080 (User/Pass: admin/admin)

Superset: http://localhost:8088 (User/Pass: admin/admin)

Trino: http://localhost:8081

Spark Master: http://localhost:8090

📊 Dashboard & Visualization
Describes the insights provided by Superset:

Market Overview: Total active jobs, distribution by industry.

Geographic Analysis: Heatmaps of job demand by region/city.

Forecasting: Line charts showing Historical Data vs. Prophet Predictions.
<img width="940" height="429" alt="image" src="https://github.com/user-attachments/assets/fb452d5f-27ed-4bdc-b231-a431d2534310" />

📂 Project Structure

├── dags/                 # Airflow DAGs (ETL & ML pipelines)

├── data/                 # Sample raw data

├── docker/               # Dockerfile and configuration for services

├── notebooks/            # Jupyter Notebooks for EDA and Model Prototyping

├── scripts/              # Python/Spark scripts for processing

├── docker-compose.yaml   # Container orchestration

└── README.md             # Project documentation


📝 License
This project is created for educational purposes within the "specialized essay" course.
