import requests
import zipfile
import io
import pandas as pd

# GDELT v2 updates a text file every 15 minutes with the latest zip URLs
GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

def get_latest_gdelt_url():
    print("Fetching latest GDELT update URL...")
    response = requests.get(GDELT_LAST_UPDATE_URL)
    response.raise_for_status()
    
    # The file has 3 lines. We want the first line (the primary export data)
    # Format is: size hash URL
    first_line = response.text.split('\n')[0]
    export_url = first_line.split(' ')[2]
    
    print(f"Targeting Live Data URL: {export_url}")
    return export_url

def download_and_extract_data(zip_url):
    print("Downloading and unpacking data in memory...")
    response = requests.get(zip_url)
    response.raise_for_status()
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        csv_filename = z.namelist()[0]
        
        # GDELT files lack headers. We map out only the essential indices we need.
        # 0: ID, 1: Date, 6: Actor1, 16: Actor2, 26: EventCode, 34: Sentiment/Scale, 60: News URL
        columns_to_keep = {
            0: 'GlobalEventID', 
            1: 'Date', 
            6: 'Actor1Name', 
            16: 'Actor2Name', 
            26: 'EventCode', 
            34: 'GoldsteinScale', 
            60: 'SourceURL'
        }
        
        with z.open(csv_filename) as f:
            # Read directly from the unpacked memory stream
            df = pd.read_csv(f, sep='\t', header=None, usecols=columns_to_keep.keys())
            df.rename(columns=columns_to_keep, inplace=True)
            
            return df

if __name__ == "__main__":
    try:
        url = get_latest_gdelt_url()
        df = download_and_extract_data(url)
        
        print("\nSUCCESS! Ingestion complete. Here is a snapshot of global events right now:\n")
        print(df.head())
        print(f"\nTotal live events fetched in this 15-minute batch: {len(df)}")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")