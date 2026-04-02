# GCP Mapping

The project is implemented locally but structured to map directly to common Google Cloud
data engineering services.

| Local Component | GCP Equivalent | Notes |
| --- | --- | --- |
| Kafka | Pub/Sub | Replace the Kafka producer and Structured Streaming source with Pub/Sub connectors |
| PySpark jobs | Dataproc | Batch and streaming Spark code can run on Dataproc clusters or serverless Spark |
| Parquet data lake | Google Cloud Storage | Bronze/Silver/Gold paths map to GCS buckets and prefixes |
| PostgreSQL analytics layer | BigQuery | Curated marts become BigQuery tables or views |
| Airflow | Cloud Composer | DAG shape and orchestration intent remain the same |

This v1 repository documents the cloud mapping only. It does not include deployment
automation or IaC.

