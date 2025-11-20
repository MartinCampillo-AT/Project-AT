import yfinance as yf
import pandas as pd
import os

def download_data(ticker, start_date, end_date, interval='1d'):
    """
    Downloads historical market data for a given ticker symbol and saves it to CSV.

    Parameters:
    ticker (str): The ticker symbol of the stock.
    start_date (str): The start date for the data in 'YYYY-MM-DD' format.
    end_date (str): The end date for the data in 'YYYY-MM-DD' format.
    interval (str): The data interval (e.g., '1d', '1h', '5m').

    Returns:
    pd.DataFrame: A DataFrame containing the historical market data.
    """
    print(f" Downloading data for {ticker} from {start_date} to {end_date} with interval {interval}...")
    
    try:
        # Auto_adjust=True is important for accurate backtesting (accounts for dividends/splits)
        data = yf.download(ticker, start=start_date, end=end_date, interval=interval, auto_adjust=True)
        
        if data.empty:
            raise ValueError(f"No data found for {ticker} with parameters provided.")
            
        # --- SAVING LOGIC (NEW) ---
        # Create data folder if it doesn't exist
        if not os.path.exists('data'):
            os.makedirs('data')

        # Define filename (e.g., data/AAPL_1d.csv)
        filename = f"data/{ticker}_{interval}.csv"
        data.to_csv(filename)
        print(f"Data saved successfully to {filename}")
        
        return data

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return pd.DataFrame()
'''
# This block executes only when running the script directly
if __name__ == "__main__":
    # Test with Apple (Daily)
    df = download_data("AAPL", "2020-01-01", "2023-12-31", interval="1d")
    
    # Print first rows to verify
    if not df.empty:
        print(df.head())
'''
bitcoin = download_data("BTC-USD", "2024-01-01", "2025-06-06", interval="1h")