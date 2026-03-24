# 🌍 Real-Time Global Event & Geopolitical Sentiment Pipeline

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

## 📌 Project Overview
An end-to-end, fault-tolerant streaming data pipeline that ingests live global news events, processes them through a decoupled Lakehouse architecture, and visualizes geopolitical sentiment in real-time. 

**[Insert your dashboard.png screenshot here. To do this in GitHub, just drag and drop the image file into the edit window, and it will generate a link for you!]**

## 🏗️ System Architecture

```mermaid
graph TD
    subgraph Data Ingestion
        A[GDELT Live Firehose] -->|HTTP Requests| B(Python Producer Script)
    end
    
    subgraph Message Broker
        B -->|mTLS Authenticated Push| C{Aiven Kafka Cluster}
    end
    
    subgraph Stream Processing
        C -->|AvailableNow Trigger| D[Databricks Serverless]
        D -->|PySpark Transformation| E[Unity Catalog Volume]
    end
    
    subgraph Data Lakehouse & BI
        E -->|Managed Checkpointing| F[(Delta Table: gdelt_live_events)]
        F -->|DirectQuery| G[Power BI Command Center]
    end

    classDef default fill:#1A1A1A,stroke:#4CAF50,stroke-width:2px,color:#fff;
    classDef database fill:#2C3E50,stroke:#3498DB,stroke-width:2px,color:#fff;
    class F database;
🚀 The Business Problem & Solution (STAR Method)
Situation: Relying on static daily data dumps means business decisions are always 24 hours behind. Analysts needed a way to monitor global events and geopolitical sentiment shifts (Goldstein Scale) in real-time without manual intervention.

Task: Architect an automated, fault-tolerant streaming pipeline capable of securely ingesting a continuous firehose of unstructured global data, cleaning it, and landing it in a centralized enterprise data warehouse.

Action: * Built a Kafka Producer in Python to fetch the live stream and push it to an Aiven Kafka cluster.

Engineered a PySpark Structured Streaming consumer on Databricks Serverless.

Optimized cloud compute costs by implementing an AvailableNow micro-batch trigger rather than an expensive 24/7 continuous stream.

Bypassed legacy DBFS storage, routing the stream directly into a secure, Unity Catalog-managed Delta Table with explicit checkpointing.

Result: Reduced data latency from 24 hours to under 15 minutes. The pipeline runs completely hands-off, automatically managing state and fault tolerance. The data lands in an ACID-compliant Delta table, natively supporting Power BI via DirectQuery, allowing stakeholders to instantly visualize global sentiment shifts the moment they happen.

🧠 Engineering Challenges Overcome
Serverless Security Constraints: Databricks Shared Access mode blocks direct file path reading for mTLS certificates. Engineered a workaround by using Python to read the certificates into memory and passed them to Spark as raw PEM strings.

Unity Catalog Migration: Encountered deprecated DBFS storage protocols. Refactored Spark SQL to dynamically provision and write to secure Unity Catalog Volumes, aligning the pipeline with modern enterprise security standards.

Fault Tolerance: Enforced explicit checkpointing for PySpark structured streaming so that if the Serverless cluster spins down, the Kafka consumer knows exactly which offset to resume from without duplicating data.

⚙️ How to Run Locally
Clone the repository:

Bash
git clone [https://github.com/Sumit-Pathrabe/gdelt-streaming-pipeline.git](https://github.com/Sumit-Pathrabe/gdelt-streaming-pipeline.git)
Set up the virtual environment & dependencies:

Bash
python -m venv venv
venv\Scripts\activate
python -m pip install -r requirements.txt
Add your Aiven Kafka Certificates: Place your ca.pem, service.cert, and service.key in a /certs directory. Update the .env file with your Kafka URI.

Start the Ingestion Engine:

Bash
python ingest_gdelt.py
Trigger the Lakehouse Micro-batch: Run the spark_consumer.py notebook in your Databricks workspace to pull the queue into the Delta Table.

Architected and developed by Sumit Pathrabe.