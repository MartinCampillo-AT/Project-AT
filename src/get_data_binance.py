import pandas as pd
import requests
import time
import os
from datetime import datetime

def get_binance_data(symbol, interval, start_str, end_str):
    """
    Downloads historical k-lines (candles) from Binance API using pagination.
    
    Parameters:
    symbol (str): Trading pair, e.g., 'BTCUSDT'.
    interval (str): Timeframe -> '1m', '5m', '1h', '1d'.
    start_str (str): Start Date -> 'YYYY-MM-DD'.
    end_str (str): End Date -> 'YYYY-MM-DD'.
    """
    
    base_url = "https://api.binance.com/api/v3/klines"
    
    # Convert dates to milliseconds timestamp (Binance standard)
    start_ts = int(datetime.strptime(start_str, "%Y-%m-%d").timestamp() * 1000)
    end_ts = int(datetime.strptime(end_str, "%Y-%m-%d").timestamp() * 1000)
    
    data = []
    current_ts = start_ts
    
    print(f"Starting download for {symbol} ({interval}) from {start_str}...")

    # LOOP: Pagination to overcome the 1000 candle limit
    while current_ts < end_ts:
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': current_ts,
            'endTime': end_ts,
            'limit': 1000
        }
        
        try:
            response = requests.get(base_url, params=params)
            temp_data = response.json()
            
            if not temp_data:
                break
                
            data.extend(temp_data)
            
            # Update pointer: Last candle time + 1ms
            last_close_time = temp_data[-1][6]
            current_ts = last_close_time + 1
            
            # Visual feedback
            current_date = datetime.fromtimestamp(current_ts / 1000).strftime('%Y-%m-%d')
            print(f"   ⏳ Fetched up to {current_date}", end='\r')
            
            # Respect API weight limits
            time.sleep(0.1)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            break

    print(f"\nDownload complete. Processing {len(data)} rows...")
    
    # Convert raw list to DataFrame
    # Binance columns: Open Time, Open, High, Low, Close, Volume, ...
    df = pd.DataFrame(data)
    df = df.iloc[:, :6] # We only need the first 6 columns
    df.columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    
    # Type conversion (Strings -> Floats)
    numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, axis=1)
    
    # Time conversion (Ms -> Datetime)
    df['Date'] = pd.to_datetime(df['Open Time'], unit='ms')
    df.set_index('Date', inplace=True)
    df.drop(columns=['Open Time'], inplace=True)
    
    # Save Logic
    if not os.path.exists('data'):
        os.makedirs('data')
        
    filename = f"data/{symbol}_{interval}_binance.csv"
    df.to_csv(filename)
    print(f"💾 Saved to {filename}")
    
    return df

if __name__ == "__main__":
    get_binance_data("BTCUSDT", "1h", "2020-01-01", "2023-12-31")