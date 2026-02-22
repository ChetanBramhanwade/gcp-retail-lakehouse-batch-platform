# ⚙ Setup Guide (Rebuild Instructions)

Follow these steps to recreate the entire project from scratch.

---

## 1️⃣ Create GCP Project

Create a new GCP project:
- Example: retail-batch-dev

Enable APIs:
- Dataproc API
- Cloud Composer API
- Cloud Storage API
- BigQuery API (optional)

---

## 2️⃣ Create GCS Buckets

Create three buckets:

1. retail-chetan-dev-raw
2. retail-chetan-dev-curated
3. retail-chetan-dev-temp

---

## 3️⃣ Create Dataproc Cluster

- Name: retail-dev-cluster
- Region: us-central1
- Machine type: e2-standard-2 (or similar)

---

## 4️⃣ Create Cloud Composer Environment

- Name: retail-dev-composer
- Region: us-central1
- Small preset for development

---

## 5️⃣ Upload Spark Script

Upload:

spark_jobs/lakehouse_pipeline.py

To:

gs://retail-chetan-dev-temp/scripts/

---

## 6️⃣ Upload Airflow DAG

Upload:

airflow_dags/retail_lakehouse_pipeline.py

To Composer DAG folder.

Wait 1–2 minutes for DAG to appear.

---

## 7️⃣ Upload Sample Raw Data

Upload CSV files to:

gs://retail-chetan-dev-raw/customers/load_date=YYYY-MM-DD/

---

## 8️⃣ Trigger DAG

In Airflow UI:
- Turn DAG ON
- Click "Trigger DAG"

Verify green execution.

---

Total rebuild time: ~1–2 hours.
