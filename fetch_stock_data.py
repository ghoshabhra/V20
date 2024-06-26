import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import argparse
import os

def fetch_and_save_stock_data(symbols, n_days, output_dir):
    """
    Fetch stock data for the last n days for given symbols,
    and save each to individual CSV files in the specified output directory.
    
    Parameters:
    - symbols: list of str, the stock symbols.
    - n_days: int, the number of days to go back from today.
    - output_dir: str, path to the directory where CSV files will be saved.
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=n_days)
    
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for symbol in symbols:
        if not symbol.endswith('.NS'):
            symbol += '.NS'
        
        # Fetch the stock data
        stock_data = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
        
        # Create a CSV file path
        csv_file_path = os.path.join(output_dir, f"{symbol.split('.')[0]}_stock_data.csv")
        
        # Save the data to a CSV file
        stock_data.to_csv(csv_file_path)
        print(f"Data for {symbol} fetched and saved to {csv_file_path}.")

def read_symbols_from_csv(file_path):
    """
    Read stock symbols from a CSV file.
    
    Parameters:
    - file_path: str, path to the CSV file.
    
    Returns:
    - list of str, the stock symbols.
    """
    df = pd.read_csv(file_path)
    return df['symbol'].tolist()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE stock data for the last n days and save to CSV.")
    parser.add_argument('n_days', type=int, help="Number of days to go back from today.")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file containing stock symbols.")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: The file {args.csv_file} does not exist.")
    else:
        symbols = read_symbols_from_csv(args.csv_file)
        fetch_and_save_stock_data(symbols, args.n_days, output_dir="Data")
