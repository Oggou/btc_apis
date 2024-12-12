import requests
import pandas as pd
import time

# Binance API Endpoint
url = "https://api.binance.com/api/v3/klines"

# Parameters for BTC/USDT 1-minute interval
params = {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "limit": 1000,  # Max allowed per request
}

# Fetch Data from Binance API
def fetch_data(start_time=None):
    if start_time:
        params["startTime"] = start_time
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise error for bad status codes
    return response.json()

# Save Data to CSV
def save_to_csv(data, file_name="btc_usdt_1m.csv"):
    columns = [
        "open_time", "open", "high", "low", "close", "volume", 
        "close_time", "quote_asset_volume", "number_of_trades", 
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ]
    df = pd.DataFrame(data, columns=columns)
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    df.to_csv(file_name, mode='a', index=False, header=False)

# Main Function
def main():
    start_time = None
    file_name = "btc_usdt_1m_12_11_24.csv"
    
    while True:
        try:
            data = fetch_data(start_time)
            if not data:
                print("No more data to fetch.")
                break
            
            save_to_csv(data, file_name)
            start_time = data[-1][6] + 1  # Use last close_time as next startTime
            
            print(f"Fetched {len(data)} rows. Sleeping to avoid rate limits...")
            time.sleep(1)  # Adjust based on usage
        except Exception as e:
            print(f"Error: {e}")
            break

if __name__ == "__main__":
    main()
