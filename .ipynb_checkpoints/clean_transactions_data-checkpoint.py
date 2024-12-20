import pandas as pd
from pymongo import MongoClient

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection_db"]
collection = db["transactions_data"]
cleaned_collection = db["transactions_data_cleaned"]

# Fetch All Data from MongoDB
print("Fetching all data from MongoDB...")
data = pd.DataFrame(list(collection.find({}, {"_id": 0})))  # Exclude MongoDB '_id'

# Data Cleaning Function
def clean_data(df):
    # Drop rows with missing critical fields
    df = df.dropna(subset=["id", "amount", "date", "client_id", "merchant_id"])

    # Convert 'amount' to numeric
    df["amount"] = pd.to_numeric(df["amount"].str.replace("$", "", regex=True), errors="coerce")
    df = df.dropna(subset=["amount"])

    # Convert 'date' to datetime format
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # Fill missing values in 'merchant_state' and 'zip' with 'unknown'
    df["merchant_state"] = df["merchant_state"].fillna("unknown")
    df["zip"] = df["zip"].fillna("unknown")

    # Standardize text fields
    text_columns = ["use_chip", "merchant_city", "merchant_state"]
    for col in text_columns:
        df[col] = df[col].astype(str).str.lower().str.strip()

    # Drop the 'errors' column
    if "errors" in df.columns:
        df = df.drop(columns=["errors"])

    # Remove duplicates
    df = df.drop_duplicates()

    return df

# Clean the Data
print("Cleaning the data...")
cleaned_data = clean_data(data)

# Insert Cleaned Data Back into MongoDB
print("Inserting cleaned data into MongoDB...")
cleaned_collection.delete_many({})  # Clear previous data if any
cleaned_collection.insert_many(cleaned_data.to_dict(orient="records"))

# Verify
print("Data cleaning complete!")
print("Total documents in cleaned collection:", cleaned_collection.count_documents({}))
