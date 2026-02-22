# Enterprise Retail Lakehouse Batch Platform (GCP)

## 📌 Project Overview

This project demonstrates a fully automated Bronze–Silver–Gold Lakehouse data pipeline built on Google Cloud Platform (GCP).

It simulates an enterprise retail batch analytics platform using:

- Google Cloud Storage (GCS)
- Dataproc (Apache Spark)
- Cloud Composer (Airflow)
- Parquet-based Lakehouse architecture
- SCD Type-2 implementation in Spark

---

## 🏗 Architecture Overview

Bronze (Raw)
- Raw CSV files stored in GCS
- Partitioned by load_date
- Immutable storage

Silver
- Cleaned & structured data stored as Parquet in GCS
- Schema enforcement
- Metadata enrichment

Gold
- Business-ready Parquet datasets
- SCD Type-2 implementation for dimensions
- Fact tables generated via Spark

Orchestration
- Airflow DAG using Cloud Composer
- Dataproc Spark job triggered automatically

---

## 🧰 Tech Stack

- Google Cloud Storage (GCS)
- Google Cloud Dataproc (Spark)
- Google Cloud Composer (Airflow)
- Parquet
- PySpark

---

## 🚀 Features Implemented

- Batch ingestion pipeline
- Bronze → Silver → Gold transformation
- SCD Type-2 in Lakehouse
- Defensive coding for missing files
- Airflow orchestration
- Parquet-based storage architecture

---

## 📂 Repository Structure
