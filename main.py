import io
import pytz
import time
import requests
import yfinance
#import threading
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor



# getting the tickers
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        res = requests.get(url, headers=headers, timeout=10) # Added a timeout
        res.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Network error while fetching S&P 500 list: {e}")
        # Return a small hardcoded list so you can at least test the rest of your code
        return ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"] 
    tables = pd.read_html(io.StringIO(res.text), attrs={"id": "constituents"})
    
    return list(tables[0].Symbol)

def get_history(ticker, period_start, period_end, granularity="1d", tries = 0):
    try:
        df = yfinance.Ticker(ticker).history(
            start = period_start, 
            end = period_end, 
            interval = granularity, 
            auto_adjust = True
        ).reset_index()
    except Exception as err:
        if tries < 5:
            return get_history(ticker, period_start, period_end, granularity, tries+1)
        return pd.DataFrame()

    df = df.rename(columns={
        "Date": "datetime",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    if df.empty:
        return pd.DataFrame()
    
    df['datetime'] = df['datetime'].dt.tz_convert(pytz.utc)
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.set_index("datetime", drop=True) 

    return df


def get_histories(tickers, period_starts, period_ends, granularity="1d"):
    # Limit to 5-10 workers to avoid getting blocked/timing out
    MAX_WORKERS = 5 
    
    def _helper(args):
        i, ticker, start, end = args
        try:
            # Small delay so we don't hit the server all at once
            time.sleep(i * 0.1) 
            print(f"Fetching {ticker}...")
            
            df = get_history(ticker, start, end, granularity=granularity)
            return df
        except Exception as e:
            # Catching the error here prevents the "Thread Explosion" log mess
            print(f"!!! Error on {ticker}: {e}")
            return None

    # Bundle arguments together
    tasks = list(zip(range(len(tickers)), tickers, period_starts, period_ends))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        dfs = list(executor.map(_helper, tasks))
    tickers = [tickers[i] for i in range(len(tickers)) if not dfs[i].empty]
    dfs = [df for df in dfs if not df.empty]

    return tickers, dfs

def get_ticker_dfs(start, end):
    from utils import load_pickle, save_pickle
    try:
        tickers, ticker_dfs = load_pickle("dataset.obj")
    except Exception as err:
        tickers = get_sp500_tickers()
        starts = [start]*len(tickers)
        ends = [end]*len(tickers)
        tickers, dfs = get_histories(tickers, starts, ends, granularity="1d")
        ticker_dfs = {ticker:df for ticker,df in zip(tickers, dfs)}
        save_pickle("dataset.obj", (tickers, ticker_dfs))
    return tickers, ticker_dfs



period_start = datetime(2010, 1, 1, tzinfo=pytz.utc)
period_end = datetime.now(pytz.utc)
ticker, ticker_dfs = get_ticker_dfs(start=period_start, end=period_end)
print(ticker_dfs)
