import pandas as pd
from pymongo import MongoClient

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection_db"] 
collection = db["transactions_data"]

# Path to the CSV file
csv_file = "/Users/mfs/transactions_data_subset.csv" 

# Load the CSV file into a Pandas DataFrame
print("Loading CSV file...")
data = pd.read_csv(csv_file)

# Convert the DataFrame to a list of dictionaries
data_dict = data.to_dict(orient="records")

# Insert data into MongoDB
print("Inserting data into MongoDB...")
collection.insert_many(data_dict)

# Verify insertion
print("Data successfully ingested!")
print("Total documents in collection:", collection.count_documents({}))
