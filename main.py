import pandas as pd
import requests
import io
import pytz
from datetime import datetime



# getting the tickers
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/134.0.0.0 Safari/537.36"
    }

    res = requests.get(url, headers=headers)
    res.raise_for_status()

    tables = pd.read_html(io.StringIO(res.text), attrs={"id": "constituents"})
    df = tables[0]

    tickers = list(df.Symbol)

    return tickers

tickers = get_sp500_tickers()





def get_history(ticker, period_start, period_end, granularity="1d"):

    import yfinance

    df = yfinance.Ticker(ticker).history(
        start = period_start, 
        end = period_end, 
        interval = granularity, 
        auto_adjust = True
    ).reset_index()

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




period_start = datetime(2010, 1, 1, tzinfo=pytz.utc)
period_end = datetime(2020, 1, 1, tzinfo=pytz.utc)



for ticker in tickers:
    df = get_history(ticker, period_start, period_end)
    print(ticker, df)