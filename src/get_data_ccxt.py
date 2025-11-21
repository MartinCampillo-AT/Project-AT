import ccxt
import pandas as pd
import os
import time
from datetime import datetime

def download_crypto_data(exchange_id, symbol, timeframe, since_str, limit=1000):
    """
    Downloads historical OHLCV data using the CCXT library with pagination.
    
    Parameters:
    exchange_id (str): Name of the exchange (e.g., 'binance', 'kraken').
    symbol (str): Trading pair in CCXT format (e.g., 'BTC/USDT').
    timeframe (str): '1m', '1h', '1d'.
    since_str (str): Start date in 'YYYY-MM-DD'.
    """
    
    # 1. Initialize the exchange dynamically
    # getattr looks for the class name inside the ccxt module
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class()
    
    # Convert start date to milliseconds (CCXT handles this helper)
    since_ts = exchange.parse8601(f"{since_str}T00:00:00Z")
    
    all_candles = []
    current_since = since_ts
    
    print(f"⬇️ Downloading {symbol} from {exchange_id} starting {since_str}...")

    while True:
        try:
            # Fetch the data (Open, High, Low, Close, Volume)
            candles = exchange.fetch_ohlcv(symbol, timeframe, since=current_since, limit=limit)
            
            if not candles:
                break
            
            all_candles.extend(candles)
            
            # Update pointer: Time of the last candle + 1 timeframe duration
            # But safely, we just take the last timestamp + 1ms
            last_timestamp = candles[-1][0]
            current_since = last_timestamp + 1
            
            # Feedback
            last_date_str = exchange.iso8601(last_timestamp)
            print(f"   ⏳ Fetched up to {last_date_str}", end='\r')
            
            # Break if we reached current time (conceptually)
            # In a real rigorous script, we would check against an 'end_date'
            if len(candles) < limit:
                break
                
            # Sleep to respect rate limits (CCXT has a built-in rate limiter property, 
            # but a manual sleep is safer for beginners)
            time.sleep(exchange.rateLimit / 1000) 
            
        except ccxt.NetworkError as e:
            print(f"❌ Network Error: {e}")
            time.sleep(5) # Retry logic could go here
        except ccxt.ExchangeError as e:
            print(f"❌ Exchange Error: {e}")
            break

    print(f"\n✅ Download complete. Rows: {len(all_candles)}")

    # 2. Process Data to DataFrame
    df = pd.DataFrame(all_candles, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    
    # Convert Timestamp to Date
    df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
    df.set_index('Date', inplace=True)
    df.drop(columns=['Timestamp'], inplace=True)
    
    # 3. Save to CSV
    if not os.path.exists('data'):
        os.makedirs('data')
        
    # Note: We replace '/' with '_' in the filename because '/' breaks file paths
    safe_symbol = symbol.replace('/', '')
    filename = f"data/{exchange_id}_{safe_symbol}_{timeframe}.csv"
    
    df.to_csv(filename)
    print(f"💾 Saved to {filename}")
    
    return df

if __name__ == "__main__":
    # Test 1: Binance (The standard)
    download_crypto_data('binance', 'BTC/USDT', '1h', '2023-01-01')
    
    # Test 2: Kraken (To prove it works on other exchanges)
    download_crypto_data('kraken', 'ETH/USD', '1d', '2023-01-01')