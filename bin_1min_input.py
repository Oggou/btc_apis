import requests
import pandas as pd
import time
from datetime import datetime

# Binance API endpoint
url = "https://api.binance.com/api/v3/klines"

# Input date and time in the format 'YYYY-MM-DD HH:MM:SS'
start_date = input("Enter the start date (YYYY-MM-DD): ")
start_time = input("Enter the start time (HH:MM:SS): ")

# Convert start date and time to a timestamp in milliseconds
start_datetime = f"{start_date} {start_time}"
try:
    start_timestamp = int(datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
except ValueError as e:
    print(f"Error: {e}")
    exit()

# Parameters for Binance API request
params = {
    "symbol": "BTCUSDT",
    "interval": "1m",  # 1-minute interval
    "limit": 1000,     # Maximum number of rows per request
    "startTime": start_timestamp,
}

# Fetch data from Binance
def fetch_data(params):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        exit()

# Save data to CSV
def save_to_csv(data, file_name="btc_usdt_data_12-01-23.csv"):
    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ]
    df = pd.DataFrame(data, columns=columns)
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    df.to_csv(file_name, mode='a', index=False, header=False)
    print(f"Saved {len(data)} rows to {file_name}")

# Main function
def main():
    print(f"Fetching data starting from {start_datetime}...")
    data = fetch_data(params)
    if data:
        save_to_csv(data)
        print(f"Data fetching complete. Retrieved {len(data)} rows.")
    else:
        print("No data returned.")

if __name__ == "__main__":
    main()
