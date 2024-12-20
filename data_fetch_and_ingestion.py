import pandas as pd
import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from pymongo import MongoClient

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection_db"] 
collection = db["transactions_data"]

# Kaggle Dataset Details
kaggle_dataset = "computingvictor/transactions-fraud-datasets"  # Dataset path on Kaggle
kaggle_file = "transactions_data.csv"  # File within the dataset
local_zip_file = "transactions_data.zip"  # Name for the downloaded zip file
local_csv_file = "transactions_data.csv"  # Name for the extracted CSV file
subset_csv_file = "transactions_data_subset.csv"  # Path to save the reduced subset

# Step 1: Download Kaggle Data
api = KaggleApi()
api.authenticate()

if not os.path.exists(local_zip_file):
    print("Downloading dataset from Kaggle...")
    api.dataset_download_file(kaggle_dataset, kaggle_file, path=".")
    print("Download complete.")

# Step 2: Unzip the File
if not os.path.exists(local_csv_file):
    print("Extracting CSV from the zip file...")
    with zipfile.ZipFile(local_zip_file, 'r') as zip_ref:
        zip_ref.extractall()
    print("Extraction complete.")

# Step 3: Load Kaggle Data
if not os.path.exists(local_csv_file):
    raise FileNotFoundError(f"Extracted CSV file not found: {local_csv_file}")

print("Loading Kaggle CSV file...")
data = pd.read_csv(local_csv_file)

# Step 4: Reduce Dataset Size
print("Reducing dataset to 120,000 rows...")
data_subset = data.head(120000)  # Keep only the top 120,000 rows

# Save the reduced subset to a new CSV file
print(f"Saving reduced dataset to {subset_csv_file}...")
data_subset.to_csv(subset_csv_file, index=False)

# Step 5: Load Reduced Data into MongoDB
print("Loading reduced CSV file for ingestion...")
data_to_ingest = pd.read_csv(subset_csv_file)

# Convert the DataFrame to a list of dictionaries
data_dict = data_to_ingest.to_dict(orient="records")

# Insert data into MongoDB
print("Inserting data into MongoDB...")
collection.insert_many(data_dict)

# Verify insertion
print("Data successfully ingested!")
print("Total documents in collection:", collection.count_documents({}))
