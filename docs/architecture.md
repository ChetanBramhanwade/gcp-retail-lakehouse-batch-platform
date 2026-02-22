# 🏗 Architecture Documentation

## Architecture Type

Lakehouse-based Batch Data Platform

---

## High-Level Flow

Raw (GCS)
   ↓
Dataproc (Spark)
   ↓
Silver (Parquet in GCS)
   ↓
Gold (Parquet in GCS with SCD2)
   ↓
(Optional) External Query Engine

---

## Layer Breakdown

### 🥉 Bronze Layer

- Location: GCS raw bucket
- Format: CSV
- Partitioning: load_date
- Rules:
  - Immutable
  - No transformations
  - System of record

---

### 🥈 Silver Layer

- Location: GCS curated bucket (silver/)
- Format: Parquet
- Transformations:
  - Schema enforcement
  - Metadata enrichment
  - Type casting

---

### 🥇 Gold Layer

- Location: GCS curated bucket (gold/)
- Format: Parquet
- Transformations:
  - Business modeling
  - SCD Type-2 implementation
  - Fact table creation

---

## Orchestration

Airflow DAG performs:

1. Submit Dataproc Spark job
2. Execute Bronze → Silver logic
3. Execute Silver → Gold logic
4. Write Parquet outputs

---

## Design Principles

- Layered architecture
- Separation of concerns
- Idempotent batch processing
- Defensive coding
- Cloud-native orchestration
