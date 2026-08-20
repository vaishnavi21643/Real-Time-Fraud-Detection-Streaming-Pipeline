# FinGuard — Real-Time Fraud Detection Streaming Pipeline

An end-to-end streaming data pipeline that ingests live financial transactions, enriches them with customer and fraud-watchlist data, detects suspicious activity, and triggers real-time alerts — built on **Databricks**, **Confluent Kafka**, **Spark Structured Streaming**, and **PostgreSQL**.

## Problem Statement

Traditional fraud detection relies on nightly batch jobs, which means fraudulent transactions can clear before they're ever flagged. FinGuard solves this by processing transactions **as they happen**, enriching them in-flight, and sending alerts within seconds of a suspicious event.

## Architecture

```
                     ┌─────────────────────┐
                     │  Confluent Kafka     │
                     │  (transaction topic) │
                     └──────────┬───────────┘
                                │
                                ▼
   ┌───────────────────────────────────────────────────┐
   │              Databricks + Lakeflow (SDP)           │
   │                                                     │
   │   Bronze  ──►  Silver  ──►  Gold                    │
   │   (raw)      (cleaned,     (aggregated,             │
   │               joined)       alert-ready)            │
   └───────────────────────────────────────────────────┘
        ▲                  ▲                  │
        │                  │                  ▼
 ┌─────────────┐   ┌───────────────┐   ┌──────────────┐
 │ PostgreSQL  │   │  JSON files   │   │  Email alert  │
 │ (customers) │   │ (Auto Loader) │   │  (Gmail SMTP) │
 │ via Lakeflow│   │  watchlist    │   └──────────────┘
 │  Connect    │   └───────────────┘          │
 └─────────────┘                              ▼
                                      ┌──────────────────┐
                                      │ Databricks        │
                                      │ Dashboard (SQL)   │
                                      └──────────────────┘
```

**Flow:**
1. A Kafka producer streams synthetic transaction events to a Confluent Kafka topic.
2. Spark Structured Streaming (via Lakeflow Declarative Pipelines) consumes the topic into a **Bronze** Delta table.
3. Customer master data is ingested from PostgreSQL via Lakeflow Connect; the fraud watchlist is streamed in as JSON files via Auto Loader.
4. Transactions are enriched in **Silver** using a stream-static join (customer data) and a stream-stream join (fraud watchlist), with watermarking to bound state and handle late-arriving events.
5. Matches against fraud rules trigger a real-time email alert and flow into **Gold** aggregate tables.
6. A Databricks SQL Dashboard refreshes near-real-time to visualize transaction and fraud metrics.
7. The full pipeline is scheduled and monitored with Lakeflow Jobs.

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming ingestion | Confluent Kafka |
| Stream processing | Spark Structured Streaming, Lakeflow Declarative Pipelines |
| Storage | Delta Lake (medallion architecture: Bronze / Silver / Gold) |
| Batch/reference data | PostgreSQL, Lakeflow Connect |
| File streaming | Auto Loader |
| Alerting | Gmail SMTP (email notifications) |
| Analytics / BI | Databricks SQL, Databricks Dashboards |
| Orchestration | Lakeflow Jobs |
| Languages | Python, SQL |
| Platform | Databricks (Free Edition) |

## Key Concepts Implemented

- Kafka producer/consumer architecture and topic partitioning
- Spark Structured Streaming trigger types (once, fixed-interval, continuous)
- Stateless vs. stateful stream processing
- Watermarking for late-arriving data
- Stream-static and stream-stream joins
- Tumbling / sliding window aggregations
- Secret scope management for secure credential handling

## Project Structure

```
finguard_streaming_project/
├── producer/              # Kafka producer script(s)
├── pipelines/              # Lakeflow Declarative Pipeline definitions
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── postgres/               # PostgreSQL setup / schema scripts
├── alerts/                 # Email alert logic (Gmail SMTP)
├── dashboards/              # Databricks SQL dashboard definitions
├── jobs/                    # Lakeflow Job orchestration configs
└── README.md
```

> Adjust this structure to match your actual repo layout.

## Setup

### Prerequisites
- Databricks workspace (Free Edition works)
- Confluent Kafka cluster (Confluent Cloud free tier is sufficient)
- PostgreSQL instance with customer data
- Gmail account with an App Password for SMTP alerts

### Steps
1. **Kafka setup** — create a Confluent Kafka cluster and topic for transactions; note the bootstrap server, API key, and secret.
2. **Secret scope** — store Kafka and SMTP credentials in a Databricks secret scope so they aren't hardcoded.
3. **Kafka producer** — run the producer script to start streaming synthetic transaction events to the topic.
4. **Bronze ingestion** — deploy the Lakeflow Declarative Pipeline to consume the Kafka topic into a Bronze Delta table.
5. **PostgreSQL ingestion** — connect PostgreSQL via Lakeflow Connect to bring in historical customer data.
6. **Silver enrichment** — run the stream-static and stream-stream join pipelines to enrich and join transactions with customer and watchlist data.
7. **Auto Loader** — point Auto Loader at the JSON file source for the fraud watchlist / reference files.
8. **Alerts** — configure Gmail SMTP credentials and enable the real-time alert step in the Silver/Gold pipeline.
9. **Dashboard** — build a Databricks SQL dashboard on the Gold tables with near-real-time refresh.
10. **Orchestration** — wire everything together with a Lakeflow Job for scheduled, end-to-end execution.

## Outcome

- Reduced fraud detection latency from a nightly batch cycle to near real-time (seconds).
- Built a fully functional streaming pipeline across Kafka, Spark Structured Streaming, and Databricks Lakeflow.
- Hands-on implementation of production streaming patterns: watermarking, stateful joins, checkpointing, and job orchestration.

## Acknowledgements

Built following the hands-on tutorial *"End-to-End Streaming Data Pipeline with Databricks, Kafka, and Spark"* by Narender Kumar.