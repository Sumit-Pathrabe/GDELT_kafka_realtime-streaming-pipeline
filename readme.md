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

Set up the virtual environment & dependencies:

Add your Aiven Kafka Certificates: Place your ca.pem, service.cert, and service.key in a /certs directory. Update the .env file with your Kafka URI.

Start the Ingestion Engine:

Trigger the Lakehouse Micro-batch: Run the spark_consumer.py notebook in your Databricks workspace to pull the queue into the Delta Table.

Architected and developed by Sumit Pathrabe.