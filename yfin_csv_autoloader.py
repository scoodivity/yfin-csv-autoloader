#! python3

"""
yfin_csv_autoloader.py: 
Exports ETF price data and organizes it into the format needed by a 
custom (Google) Sheets-based backtester.
Update end date in config.py.
"""

import requests
import datetime as dt
import time
import csv
import logging

from concurrent.futures import ThreadPoolExecutor

import config

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
logging.disable(logging.DEBUG)


def convert_time(date_str):
    """Converts dates to timestamps."""
    time_tuple = dt.datetime.strptime(date_str, '%m/%d/%Y:%H').timetuple()
    return int(time.mktime(time_tuple))


def form_query(query_ticker, query_start, query_end):
    """Forms Yahoo! query URL for CSVs."""
    yf_query = (
        f'https://query1.finance.yahoo.com/v7/finance/download/'
        f'{query_ticker}?period1={query_start}&period2={query_end}&'
        f'interval=1d&events=history&includeAdjustedClose=true'
        )
    return yf_query


def download_csv(params):
    """Downloads CSVs and returns row count."""
    ticker = params[0]
    query_str = params[1]
    header_str = {
        'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 '
        'Safari/537.36',
        }

    response = requests.get(query_str, headers=header_str)
    response.raise_for_status()
    row_count = len(response.text.splitlines())

    with open(f'{ticker}.csv', 'w') as csv_file:
        csv_file.write(response.text)
    
    print(f'{ticker} created.')
    response.close()
    return row_count


def merge_csvs(datasets, max_rows):
    """Merges CSVs."""
    print(f"max_rows: {max_rows}")
    buffer = []

    for dataset in datasets:
        with open(dataset, 'r') as open_csv:
            reader = csv.reader(open_csv)
            reader = sorted(reader, key=lambda x: x[0], reverse=True)
            for i, row in enumerate(reader):
                if i == max_rows:
                    break 
                logging.info(f"writing row: {i + 1} to merged.csv")
                try:
                    buffer[i].extend(row + ['', ''])
                except IndexError:
                    buffer.append([])
                    buffer[i].extend(row + ['', ''])

    with open('merged.csv', 'w') as merged:
        writer = csv.writer(merged)
        writer.writerows(buffer)


def main_loop():
    """Calls sub-functions."""
    universe = [
        'AGG', 'EEM', 'SPY', 'VGK', 'EWJ', 'VNQ', 
        'RWX', 'GLD', 'TLT', 'DBC', 'IEF', 'SHY', 
        'LQD', 'PDBC', 'QQQ', 'IWM',
        ]
    #universe = ['SPY', 'QQQ',]      # Testing only.

    start_date = convert_time('01/29/1993:9') 
    end_date = convert_time(config.end_date)   

    worker_count = len(universe)
    datasets = [(ticker + '.csv') for ticker in universe]    
    queries = [form_query(ticker, start_date, end_date) for ticker in universe]
    
    params = zip(universe, queries)

    row_counts = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        row_counts += executor.map(download_csv, params)

    max_rows = min(row_counts)
    """Sets the maximum number of rows to write (to merged.csv) to equal
    the smallest number of rows across all CSVs. (Bottlenecked by least
    data.)
    """

    merge_csvs(datasets, max_rows)


main_loop()