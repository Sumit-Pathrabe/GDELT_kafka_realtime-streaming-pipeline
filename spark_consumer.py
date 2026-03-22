from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Define the Workspace Path for your Certificates
cert_path = "/Workspace/Users/YOUR_EMAIL_HERE/kafka_certs/"

# 2. Read Certificates into Memory (Bypasses Serverless hard drive restrictions)
with open(cert_path + "ca.pem", "r") as f:
    ca_pem = f.read()

with open(cert_path + "service.cert", "r") as f:
    service_cert = f.read()

with open(cert_path + "service.key", "r") as f:
    service_key = f.read()

# 3. Secure Kafka Configuration (Using your fixed Aiven URI)
kafka_options = {
    "kafka.bootstrap.servers": "your-aiven-uri:port", # Ensure this is your full Aiven URL
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.type": "PEM",
    "kafka.ssl.truststore.certificates": ca_pem,
    "kafka.ssl.keystore.type": "PEM",
    "kafka.ssl.keystore.certificate.chain": service_cert,
    "kafka.ssl.keystore.key": service_key,
    "subscribe": "gdelt-events",
    "startingOffsets": "earliest"
}

# 4. Read the Raw Stream
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# 5. Parse the JSON Payload
json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_payload")

schema = StructType([
    StructField("GlobalEventID", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("EventCode", StringType(), True),
    StructField("GoldsteinScale", DoubleType(), True),
    StructField("SourceURL", StringType(), True)
])

parsed_stream = json_stream.withColumn("data", from_json(col("json_payload"), schema)).select("data.*")

# 6. Transformation
clean_stream = parsed_stream \
    .withColumn("EventDate", to_date(col("Date"), "yyyyMMdd")) \
    .filter(col("Actor1Name") != "UNKNOWN")

# 7. Unity Catalog & Delta Lake Sink
print("Configuring Unity Catalog Volume for secure checkpointing...")
spark.sql("CREATE VOLUME IF NOT EXISTS default.streaming_checkpoints")

current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
checkpoint_path = f"/Volumes/{current_catalog}/default/streaming_checkpoints/gdelt_stream"

print(f"Starting Delta Micro-Batch... draining Kafka queue into default.gdelt_live_events")

query = clean_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable("default.gdelt_live_events")

query.awaitTermination()
print("Batch complete! Data is securely in the Delta Lakehouse.")