import requests
import zipfile
import io
import pandas as pd
import json
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load environment variables
load_dotenv()

GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
KAFKA_TOPIC = "gdelt-events"

# Configure the Kafka Producer for Aiven (mTLS)
# Configure the Kafka Producer for Aiven (mTLS) using your new .env variables
producer_config = {
    'bootstrap.servers': os.getenv('KAFKA_SERVICE_URI'),
    'security.protocol': 'SSL',
    'ssl.ca.location': os.getenv('KAFKA_CA_PATH'),
    'ssl.certificate.location': os.getenv('KAFKA_CERT_PATH'),
    'ssl.key.location': os.getenv('KAFKA_KEY_PATH'),
    'client.id': 'gdelt-python-producer'
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def get_latest_gdelt_url():
    print("Fetching latest GDELT update URL...")
    response = requests.get(GDELT_LAST_UPDATE_URL)
    response.raise_for_status()
    first_line = response.text.split('\n')[0]
    return first_line.split(' ')[2]

def download_and_extract_data(zip_url):
    print("Downloading and unpacking data in memory...")
    response = requests.get(zip_url)
    response.raise_for_status()
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        csv_filename = z.namelist()[0]
        columns_to_keep = {
            0: 'GlobalEventID', 1: 'Date', 6: 'Actor1Name', 
            16: 'Actor2Name', 26: 'EventCode', 34: 'GoldsteinScale', 60: 'SourceURL'
        }
        with z.open(csv_filename) as f:
            df = pd.read_csv(f, sep='\t', header=None, usecols=columns_to_keep.keys())
            df.rename(columns=columns_to_keep, inplace=True)
            df.fillna("UNKNOWN", inplace=True)
            return df

def stream_to_kafka(df):
    print(f"Streaming {len(df)} records to Aiven Kafka topic '{KAFKA_TOPIC}'...")
    
    for index, row in df.iterrows():
        record_value = json.dumps(row.to_dict())
        producer.produce(
            topic=KAFKA_TOPIC,
            key=str(row['GlobalEventID']), 
            value=record_value,
            callback=delivery_report
        )
        producer.poll(0)
        
    producer.flush()
    print("All records successfully pushed to Aiven Kafka!")

if __name__ == "__main__":
    try:
        url = get_latest_gdelt_url()
        df = download_and_extract_data(url)
        stream_to_kafka(df)
    except Exception as e:
        print(f"Pipeline failed: {e}")