# 🚀 Sales ETL Pipeline
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/)
[![Airflow 2.10.3](https://img.shields.io/badge/Airflow-2.10.3-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![AWS Redshift](https://img.shields.io/badge/AWS_Redshift-Serverless-FF9900?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/redshift/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

An automated, containerized ETL pipeline that orchestrates the extraction of monthly retail sales data from local CSV files into **Amazon Redshift Serverless** via **AWS S3**.

---

## 🚀 Overview
This project implements a production-grade ETL workflow managed by **Apache Airflow**. It automates the ingestion of raw data, performs cleaning and aggregation using **Pandas**, and securely loads the final dataset into a cloud data warehouse.

### ✨ Key Features
* **Automated Extraction:** Monthly scheduled ingestion of raw local CSV datasets.
* **Data Transformation:** Automated cleaning and revenue aggregation using Python and Pandas.
* **Cloud Integration:** Secure, IAM-based loading into **Redshift Serverless** using Boto3 for dynamic credentials.
* **Idempotency:** Utilizes `TRUNCATE` operations to ensure reruns do not create duplicate records.

---

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow 2.10.3 (Dockerized)
* **Language:** Python 3.10
* **Cloud Infrastructure:** AWS S3, Amazon Redshift Serverless
* **Environment:** Docker & Docker Compose

---

## 📁 Project Structure
```text
SALES_ETL/
├── dags/                # Airflow DAG definition (sales_analytics_dag.py)
├── scripts/             # ETL logic (ingest_to_s3.py, transform_logic.py)
├── data/                # Data staging (raw vs processed)
├── tests/               # PyTest suite for data validation
├── docker-compose.yaml  # Container orchestration
└── .env                 # AWS & Redshift credentials (not tracked)