#! python3

"""
yfin_csv_autoloader.py: 
Exports ETF price data and organizes it into the format needed by a 
custom (Google) Sheets-based backtester.
"""

import logging
import datetime as dt
import requests
import time
import csv

from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
#logging.disable(logging.CRITICAL)

def clear():
    """Clears the screen."""
    print('\033c\033[3J', end='')

def convert_time(date_str):
    """Converts dates to timestamps."""
    time_tuple = dt.datetime.strptime(date_str, '%m/%d/%Y:%H').timetuple()
    return int(time.mktime(time_tuple))

def form_query(query_ticker, query_start, query_end):
    """Forms Yahoo! query URL for CSVs."""
    yf_query = f'https://query1.finance.yahoo.com/v7/finance/download/{query_ticker}?period1={query_start}&period2={query_end}&interval=1d&events=history&includeAdjustedClose=true'
    return yf_query

def download_csv(packed_params):
    """Downloads, writes, sorts, rewrites CSVs."""
    ticker = packed_params[0]
    query_str = packed_params[1]
    headers = {
        'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36',
        }

    res = requests.get(query_str, headers=headers)
    res.raise_for_status()
    play_file = open(f'{ticker}.csv', 'wb')
    
    for chunk in res.iter_content(chunk_size=None):
        play_file.write(chunk)
    play_file.close()

    with open(f'{ticker}.csv', newline='') as csv_file:
        reader = csv.reader(csv_file)
        sorted_list = sorted(reader, key=lambda x: x[0], reverse = True)

    with open(f'{ticker}.csv', 'w') as csv_out:
        wrtr = csv.writer(csv_out)
        wrtr.writerows(sorted_list)

    print(f'{ticker} created.')

def merge_csv(universe):
    """Merges CSVs."""
    
    datasets = [(ticker + '.csv') for ticker in universe]
    datasets_len_list = []
    dataset_len_min = 0
    buffer_row = []

    # Appends .csv to universe to create datasets list.
    for ticker in universe:
        datasets.append(ticker + '.csv')

    # Gets the minimum number of rows across all CSVs/
    for dataset in datasets:
        open_csv = open(dataset)
        open_reader = csv.reader(open_csv)
        open_data = list(open_reader)
        datasets_len_list.append(len(open_data))
        open_csv.close()
    dataset_len_min = min(datasets_len_list)

    merged_file = open('merged.csv', 'w', newline='')
    merged_writer = csv.writer(merged_file)

    print(f'Minimum rows: {dataset_len_min}')
    for row in range(dataset_len_min):
        buffer_row = []
        for dataset in datasets:
            open_csv = open(dataset)
            open_reader = csv.reader(open_csv)
            open_data = list(open_reader)
            buffer_row.extend(open_data[row] + ['', ''])

        print(f'Writing row {row}')
        merged_writer.writerow(buffer_row)

def main_loop():
    """Calls sub-functions."""
    
    start_date = convert_time('01/29/1993:9')   # MM/DD/YYYY:HOUR (use 9AM)
    end_date = convert_time('11/30/2023:9')     # Update this date.
    universe = [
        'AGG', 'EEM', 'SPY', 'VGK', 'EWJ', 'VNQ', 
        'RWX', 'GLD', 'TLT', 'DBC', 'IEF', 'SHY', 
        'LQD', 'PDBC', 'QQQ', 'IWM',
        ]
    worker_count = len(universe)
    
    queries = [form_query(ticker, start_date, end_date) for ticker in universe]
    packed_params = zip(universe, queries)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        return executor.map(download_csv, packed_params)

    merge_csv(universe)

main_loop()