import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import argparse
import os

# Define a configurable constant for the percentage threshold
PERCENTAGE_THRESHOLD = 20  # Example: 20 means 20%

def fetch_stock_data(symbol, start_date, end_date):
    """
    Fetch stock data for a given symbol between start_date and end_date.
    
    Parameters:
    - symbol: str, the stock symbol.
    - start_date: str, start date in YYYY-MM-DD format.
    - end_date: str, end date in YYYY-MM-DD format.
    
    Returns:
    - DataFrame containing the stock data.
    """
    oldSymbol = symbol
    if not symbol.endswith('.NS'):
        symbol += '.NS'
    stock_data = yf.download(symbol, start=start_date, end=end_date)
    stock_data['Symbol'] = oldSymbol  # Add a column for the stock symbol
    return stock_data

def mark_keywords(stock_data):
    """
    Mark each row as 'green' if open price is less than close price, otherwise 'red'.
    
    Parameters:
    - stock_data: DataFrame containing the stock data.
    
    Returns:
    - DataFrame with an added 'Keyword' column.
    """
    stock_data['Keyword'] = stock_data.apply(lambda row: 'green' if row['Open'] < row['Close'] else 'red', axis=1)
    return stock_data

def assign_groups(stock_data):
    """
    Assign groups to neighboring green and red candles.
    Green groups are named 'green 1', 'green 2', etc., and all red groups are named 'red'.
    
    Parameters:
    - stock_data: DataFrame containing the stock data.
    
    Returns:
    - DataFrame with added 'Group' and 'GroupID' columns.
    """
    stock_data['GroupID'] = (stock_data['Keyword'] != stock_data['Keyword'].shift()).cumsum()
    stock_data['Group'] = stock_data['Keyword'].str.cat(stock_data['GroupID'].astype(str), sep='_')

    return stock_data

def calculate_percentage_differences(stock_data):
    """
    Calculate the percentage difference between the highest High and the lowest Low for each group.
    
    Parameters:
    - stock_data: DataFrame containing the stock data.
    
    Returns:
    - DataFrame with an added 'PercentageDifference' column.
    """
    percentage_diffs = []
    for group_id in stock_data['GroupID'].unique():
        group_data = stock_data[stock_data['GroupID'] == group_id]
        lowest_low = group_data['Low'].min()
        highest_high = group_data['High'].max()
        percentage_diff = ((highest_high - lowest_low) / lowest_low) * 100
        percentage_diffs.extend([percentage_diff] * len(group_data))
    stock_data['PercentageDifference'] = percentage_diffs
    return stock_data

def identify_valid_groups(stock_data):
    """
    Identify valid green groups where the percentage difference between the highest High
    and the lowest Low is at least the specified percentage threshold.
    
    Parameters:
    - stock_data: DataFrame containing the stock data.
    
    Returns:
    - DataFrame with an added 'ValidGroup' column.
    """
    valid_groups = []
    for group_name in stock_data['Group'].unique():
        if 'green' in group_name:
            group_data = stock_data[stock_data['Group'] == group_name]
            if group_data['PercentageDifference'].iloc[0] >= PERCENTAGE_THRESHOLD:
                valid_groups.append(group_name)
    stock_data['ValidGroup'] = stock_data['Group'].apply(lambda x: x if x in valid_groups else '')
    return stock_data

def remove_invalid_rows(stock_data):
    """
    Remove rows that have an empty ValidGroup column.
    
    Parameters:
    - stock_data: DataFrame containing the stock data.
    
    Returns:
    - DataFrame with rows having empty ValidGroup removed.
    """
    return stock_data[stock_data['ValidGroup'] != '']

def transform_valid_groups(stock_data):
    """
    Transform each valid group into a single row with specified columns.
    
    Parameters:
    - stock_data: DataFrame containing the stock data.
    
    Returns:
    - DataFrame with transformed rows.
    """
    transformed_data = []
    for group_name in stock_data['ValidGroup'].unique():
        if group_name:
            group_data = stock_data[stock_data['ValidGroup'] == group_name]
            symbol = group_data['Symbol'].iloc[0]
            lowest_low = group_data['Low'].min()
            highest_high = group_data['High'].max()
            earliest_date = group_data.index.min()
            percentage_diff = group_data['PercentageDifference'].iloc[0]
            transformed_data.append({
                'Symbol': symbol,
                'Lowest Low': lowest_low,
                'Highest High': highest_high,
                'Earliest Date': earliest_date,
                'Percentage Difference': percentage_diff,
                'ValidGroup': group_name
            })
    return pd.DataFrame(transformed_data)

def save_to_csv(stock_data, symbol, output_dir):
    """
    Save the stock data to a CSV file in the specified output directory.
    
    Parameters:
    - stock_data: DataFrame containing the stock data.
    - symbol: str, the stock symbol.
    - output_dir: str, path to the directory where CSV files will be saved.
    """
    csv_file_path = os.path.join(output_dir, f"{symbol.split('.')[0]}_stock_data.csv")
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

def main(n_days, csv_file, output_dir):
    """
    Main function to fetch NSE stock data for the last n days and save to CSV.
    
    Parameters:
    - n_days: int, the number of days to go back from today.
    - csv_file: str, path to the CSV file containing stock symbols.
    - output_dir: str, path to the directory where CSV files will be saved.
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=n_days)
    
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    symbols = read_symbols_from_csv(csv_file)
    combined_data = pd.DataFrame()
    
    for symbol in symbols:
        stock_data = fetch_stock_data(symbol, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        stock_data = mark_keywords(stock_data)
        stock_data = assign_groups(stock_data)
        stock_data = calculate_percentage_differences(stock_data)
        stock_data = identify_valid_groups(stock_data)
        stock_data = remove_invalid_rows(stock_data)
        stock_data = transform_valid_groups(stock_data)
        combined_data = pd.concat([combined_data, stock_data])
    
    save_to_csv(combined_data, "Combined", output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE stock data for the last n days and save to CSV.")
    parser.add_argument('n_days', type=int, help="Number of days to go back from today.")
    parser.add_argument('csv_file', type=str, help="Path to the CSV file containing stock symbols.")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: The file {args.csv_file} does not exist.")
    else:
        main(args.n_days, args.csv_file, output_dir="data")
