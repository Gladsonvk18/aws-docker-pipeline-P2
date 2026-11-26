# Resilient AWS ETL Pipeline: EC2 Docker, RDS & Glue Fallback

A fault-tolerant, containerized ETL pipeline built on AWS (`ap-southeast-2`) using Python, Docker, Amazon S3, Amazon RDS (MySQL), AWS Glue Data Catalog, and Amazon Athena.

## Architecture Overview

1. **Extract (Amazon S3 -> EC2 / Docker):** A Python ETL container (`script.py`) running on EC2 authenticates via an attached IAM Role and extracts `s3bucket.csv` from Amazon S3.
2. **Primary Load (EC2 / Docker -> Amazon RDS):** Connects to Amazon RDS (MySQL) over port `3306` and upserts records into `ingestion_db.users`.
3. **Fault-Tolerant Fallback (EC2 / Docker -> AWS Glue):** If RDS is unreachable, the `except` block registers an external catalog table (`users_fallback`) in AWS Glue Data Catalog (`gladson_pipeline_2_glue_db`) mapped to the S3 data location.
4. **Serverless Verification (AWS Glue + S3 -> Amazon Athena):** Amazon Athena queries `users_fallback` directly via SQL against S3, writing query outputs to a separate S3 bucket.

## Tech Stack

- **Compute & Containerization:** Amazon EC2 (Ubuntu), Docker, Python 3.12 (`boto3`, `mysql-connector-python`)
- **Storage & Databases:** Amazon S3, Amazon RDS (MySQL)
- **Catalog & Serverless Analytics:** AWS Glue Data Catalog, Amazon Athena
- **Security:** EC2 IAM Instance Profile (`gladson-pipeline-2-role`) and runtime `.env` injection

## Quick Start

```bash
# 1. Build the Docker image
sudo docker build -t gladson-pipeline:latest .

# 2. Run primary pipeline (loads to RDS MySQL)
sudo docker run --rm --env-file .env gladson-pipeline:latest

# 3. Simulate RDS failure (triggers AWS Glue fallback)
sudo docker run --rm --env-file .env -e RDS_HOST="invalid-host.local" gladson-pipeline:latest
```

### Verify in Amazon Athena

```sql
SELECT * FROM "gladson_pipeline_2_glue_db"."users_fallback" LIMIT 10;
```
