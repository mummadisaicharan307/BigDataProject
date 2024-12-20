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

def clean_data(df):
    print("Initial DataFrame shape:", df.shape)

    # Drop rows with missing critical fields
    df = df.dropna(subset=["id", "amount", "date", "client_id", "merchant_id"])
    print("After dropping rows with missing critical fields:", df.shape)

    # Handle and convert 'amount' column
    df["amount"] = df["amount"].str.replace(r"[^0-9.]", "", regex=True)  # Remove non-numeric characters
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])
    print("After converting 'amount' and dropping NaNs:", df.shape)

    # Convert 'date' to datetime format
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    print("After converting 'date' and dropping NaNs:", df.shape)

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
    print("After dropping 'errors' column:", df.shape)

    # Remove duplicates
    df = df.drop_duplicates()
    print("After removing duplicates:", df.shape)

    return df




# Clean the Data
print("Cleaning the data...")
cleaned_data = clean_data(data)

print("Cleaned data:")
print(cleaned_data.head())  # Show the first few rows
print("Shape of cleaned_data:", cleaned_data.shape)


if not cleaned_data.empty:
    cleaned_collection.delete_many({})  # Clear previous data if any
    cleaned_collection.insert_many(cleaned_data.to_dict(orient="records"))
    print("Data inserted successfully!")
else:
    print("No data to insert. cleaned_data is empty.")


# Insert Cleaned Data Back into MongoDB
print("Inserting cleaned data into MongoDB...")
if not cleaned_data.empty:  # Check if DataFrame is not empty
    cleaned_collection.delete_many({})  # Clear previous data if any
    records = cleaned_data.to_dict(orient="records")
    cleaned_collection.insert_many(records)
    print("Data inserted successfully!")
    print("Total documents in cleaned collection:", cleaned_collection.count_documents({}))
else:
    print("No data to insert. Cleaned DataFrame is empty.")


# Verify
print("Data cleaning complete!")
print("Total documents in cleaned collection:", cleaned_collection.count_documents({}))
