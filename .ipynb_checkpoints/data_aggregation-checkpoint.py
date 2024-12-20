import pandas as pd
from pymongo import MongoClient

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection_db"]
collection = db["transactions_data_cleaned"]

# Fetch cleaned data into a Pandas DataFrame
print("Fetching cleaned data from MongoDB...")
data = pd.DataFrame(list(collection.find({}, {"_id": 0})))

# Ensure 'date' is in datetime format
data["date"] = pd.to_datetime(data["date"], errors="coerce")

# Perform Aggregations
print("Performing data aggregations...")

# 1. Top 10 Merchants by Total Amount
top_merchants = data.groupby("merchant_id").agg(
    total_amount=("amount", "sum"),
    total_transactions=("id", "count")
).sort_values(by="total_amount", ascending=False).head(10).reset_index()

# 2. Top 10 Clients by Total Spending
top_clients = data.groupby("client_id").agg(
    total_amount=("amount", "sum"),
    total_transactions=("id", "count")
).sort_values(by="total_amount", ascending=False).head(10).reset_index()

# 3. Transaction Volume by Merchant State
transactions_by_state = data.groupby("merchant_state").agg(
    total_transactions=("id", "count"),
    total_amount=("amount", "sum")
).sort_values(by="total_transactions", ascending=False).reset_index()

# 4. Daily Transaction Trends
daily_trends = data.groupby(data["date"].dt.date).agg(
    total_amount=("amount", "sum"),
    total_transactions=("id", "count")
).reset_index()
daily_trends.rename(columns={"date": "day"}, inplace=True)

# 5. Average Transaction Amount by Merchant City
avg_transaction_by_city = data.groupby("merchant_city").agg(
    avg_amount=("amount", "mean"),
    total_transactions=("id", "count")
).sort_values(by="avg_amount", ascending=False).reset_index()

# Export Aggregated Data to CSV
print("Exporting aggregated data to CSV files...")

top_merchants.to_csv("/Users/mfs/Downloads/agg_top_merchants.csv", index=False)
top_clients.to_csv("/Users/mfs/Downloads/agg_top_clients.csv", index=False)
transactions_by_state.to_csv("/Users/mfs/Downloads/agg_transactions_by_state.csv", index=False)
daily_trends.to_csv("/Users/mfs/Downloads/agg_daily_trends.csv", index=False)
avg_transaction_by_city.to_csv("/Users/mfs/Downloads/agg_avg_transaction_by_city.csv", index=False)

print("Data aggregation complete! CSV files exported:")
print("- agg_top_merchants.csv")
print("- agg_top_clients.csv")
print("- agg_transactions_by_state.csv")
print("- agg_daily_trends.csv")
print("- agg_avg_transaction_by_city.csv")
