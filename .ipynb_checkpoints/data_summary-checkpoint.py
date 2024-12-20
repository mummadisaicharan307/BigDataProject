import pandas as pd
from pymongo import MongoClient

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")  # MongoDB connection string
db = client["fraud_detection_db"]  # Database name
collection = db["transactions_data"]  # Collection name

# Fetch all rows and columns for a sample size
print("Fetching first 5 rows...")
sample_data = list(collection.find({}, {"_id": 0}).limit(5))  # Exclude MongoDB '_id'

# Convert sample to DataFrame
df_sample = pd.DataFrame(sample_data)

# Display sample data
print("Sample Data (First 5 Rows):")
print(df_sample)

# Fetch total number of rows
total_rows = collection.count_documents({})
print("\nTotal Rows in Collection:", total_rows)

# Fetch total number of columns
total_columns = len(df_sample.columns)
print("Total Columns in Collection:", total_columns)

# Display column names
print("\nColumn Names:")
print(df_sample.columns.tolist())

