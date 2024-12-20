import pandas as pd
import matplotlib.pyplot as plt

# Load aggregated data
top_merchants = pd.read_csv("/Users/saicharan/Downloads/bigdatadataset/agg_top_merchants.csv")
top_clients = pd.read_csv("/Users/saicharan/Downloads/bigdatadataset/agg_top_clients.csv")
transactions_by_state = pd.read_csv("/Users/saicharan/Downloads/bigdatadataset/agg_transactions_by_state.csv")
daily_trends = pd.read_csv("/Users/saicharan/Downloads/bigdatadataset/agg_daily_trends.csv")
avg_transaction_by_city = pd.read_csv("/Users/saicharan/Downloads/bigdatadataset/agg_avg_transaction_by_city.csv")

# Visualization 1: Top 10 Merchants by Total Amount
plt.figure(figsize=(10, 6))
plt.bar(top_merchants["merchant_id"].astype(str), top_merchants["total_amount"], color="skyblue")
plt.title("Top 10 Merchants by Total Amount")
plt.xlabel("Merchant ID")
plt.ylabel("Total Amount ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("/Users/saicharan/Downloads/bigdatadataset/top_10_merchants.png")
plt.show()

# Visualization 2: Top 10 Clients by Total Spending
plt.figure(figsize=(10, 6))
plt.bar(top_clients["client_id"].astype(str), top_clients["total_amount"], color="orange")
plt.title("Top 10 Clients by Total Spending")
plt.xlabel("Client ID")
plt.ylabel("Total Amount ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("/Users/mfs/Downloads/top_10_clients.png")
plt.show()

# Visualization 3: Transaction Volume by State
plt.figure(figsize=(10, 6))
plt.barh(transactions_by_state["merchant_state"], transactions_by_state["total_transactions"], color="green")
plt.title("Transaction Volume by State")
plt.xlabel("Total Transactions")
plt.ylabel("Merchant State")
plt.tight_layout()
plt.savefig("//Users/saicharan/Downloads/bigdatadataset/transactions_by_state.png")
plt.show()

# Visualization 4: Daily Transaction Trends
plt.figure(figsize=(12, 6))
plt.plot(daily_trends["day"], daily_trends["total_amount"], marker="o", linestyle="-", color="purple")
plt.title("Daily Transaction Trends")
plt.xlabel("Date")
plt.ylabel("Total Amount ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("/Users/saicharan/Downloads/bigdatadataset/daily_trends.png")
plt.show()

# Visualization 5: Average Transaction Amount by Merchant City
plt.figure(figsize=(12, 6))
top_cities = avg_transaction_by_city.head(10)  # Top 10 cities for clarity
plt.bar(top_cities["merchant_city"], top_cities["avg_amount"], color="red")
plt.title("Average Transaction Amount by Merchant City (Top 10)")
plt.xlabel("Merchant City")
plt.ylabel("Average Amount ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("/Users/saicharan/Downloads/bigdatadataset/avg_transaction_by_city.png")
plt.show()

print("Visualizations complete! Charts saved as PNG files.")
