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