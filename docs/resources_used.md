# ☁ GCP Resources Used

## Project

- retail-batch-dev

---

## Cloud Storage Buckets

- retail-chetan-dev-raw
- retail-chetan-dev-curated
- retail-chetan-dev-temp

---

## Dataproc

- Cluster name: retail-dev-cluster
- Region: us-central1
- Spark version: Default image

---

## Cloud Composer

- Environment: retail-dev-composer
- Region: us-central1

---

## IAM / Roles

Default Compute Service Account roles:
- Storage Admin
- Dataproc Editor
- Composer User

---

## Estimated Cost Drivers

- Dataproc cluster runtime
- Composer environment
- GCS storage

Recommendation:
Delete cluster and Composer when not in use.
