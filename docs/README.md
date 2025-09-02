🍽️ Restaurant Analytics Pipeline

An end-to-end AWS data engineering pipeline designed to transform raw restaurant transaction data into business-ready insights for customer analytics, churn prediction, loyalty tracking, and revenue optimization.

This project demonstrates the Medallion architecture (Bronze → Silver → Gold), leveraging RDS, S3, Glue, DMS, Lambda, EventBridge, and Streamlit for orchestration, transformation, and serving dashboards.

🚀 Architecture

<!-- replace with your actual diagram -->

Flow:

SQL Server RDS (source system) →

DMS → S3 Bronze (raw Parquet) →

Glue ETL → Silver (cleaned, conformed) →

Glue ETL → Gold (business marts) →

Athena/Lambda APIs or Streamlit dashboards

🛠️ Tech Stack

Data Source: RDS (SQL Server Express)

Storage: S3 (Bronze, Silver, Gold, Logs) with KMS encryption

Ingestion: AWS DMS (batch load)

Transformations: AWS Glue (PySpark)

Orchestration: EventBridge + CloudWatch (scheduling & monitoring)

Serving Layer: Athena + Lambda API / Streamlit Dashboards

Infra & Security: IAM roles, Secrets Manager, KMS

CI/CD: GitHub Actions (infra + code deployment)

📊 Pipeline Steps
1. Source: RDS (SQL Server Express)

Free-tier RDS instance with restaurant data.

Tables populated from CSVs.

Basic integrity & row-count checks.

2. Storage: S3 Medallion Buckets

s3://<project>-bronze → Raw ingested data.

s3://<project>-silver → Cleaned & standardized.

s3://<project>-gold → Analytics-ready marts.

Encryption with SSE-S3 / KMS.

3. IAM & Secrets

Secrets Manager stores RDS credentials.

IAM roles for DMS, Glue, Lambda, and logging.

4. Ingestion: AWS DMS

Replication task moves data → Bronze S3 (Parquet).

Daily batch load (Express edition).

5. Transformation: Glue ETL (PySpark)

Bronze → Silver: Type casting, deduplication, FK checks, date alignment.

Silver → Gold: Business-ready marts and metrics.

6. Gold Data Model

fact_orders → Revenue, discounts.

fact_customer_daily_ltv → LTV by customer/date.

dim_customer, dim_date, dim_location.

Marts for churn, loyalty, pricing, location ranking.

7. Orchestration & Monitoring

EventBridge schedules daily workflows.

CloudWatch Logs + Alarms for DMS, Glue, Lambda.

SNS alerts on failure.

8. Serving Layer

Option A: Athena + Lambda + API Gateway (JSON API).

Option B: Streamlit dashboards directly reading Gold.

9. Dashboards (Streamlit)

CLV – Customer lifetime value trends.

RFM Segmentation – Recency, Frequency, Monetary analysis.

Churn Risk – Indicators for at-risk customers.

Sales Trends – Time-grain analysis (D/W/M).

Loyalty Impact – Loyalty vs non-loyalty performance.

Pricing/Discounts – Effectiveness of promos & discounts.

📈 Example Dashboard

<img width="1005" height="404" alt="Screenshot 2025-08-28 at 8 13 21 PM" src="https://github.com/user-attachments/assets/23f10c4f-52aa-4337-86cd-4f457ac348de" />



🔔 Monitoring & CI/CD

CloudWatch – Logs, metrics, alarms.

GitHub Actions – Deploys Glue jobs, Lambda functions, and infra templates.

🧭 Business Value

This pipeline enables restaurant operators to:

Measure customer LTV and segment with RFM.

Identify churn risks and loyalty program impact.

Track sales trends across time and locations.

Optimize pricing & discounting strategies.

📹 Demo Video

👉 Demo Walkthrough Video
 <!-- add Loom/YouTube link later -->

🧑‍💻 Author

Eitan Lupovitch
Data Engineer | Analytics Consultant | Streamlit Builder
LinkedIn
 | GitHub
