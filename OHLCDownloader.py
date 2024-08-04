import json
import urllib
from datetime import date
from io import StringIO
# import Helper as h
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from pandas.io.json import json_normalize
import yfinance as yf
import Tickers as t

_nse_index_dl_url="https://www.nseindia.com/api/historical/indicesHistory?indexType={0}&from={1}&to={2}"
_nse_cm_dl_url = "https://www.nseindia.com/api/historical/cm/equity?symbol={0}&series=[\"EQ\"]&from={1}&to={2}&csv=true"
# Urls for fetching Data
url_oc = "https://www.nseindia.com/option-chain"
url_bnf = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'
url_nf = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
url_indices = "https://www.nseindia.com/api/allIndices"
url_master = "https://www.nseindia.com/api/master-quote"
url_index = "https://www.nseindia.com/api/option-chain-indices?symbol="
url_equity = "https://www.nseindia.com/api/option-chain-equities?symbol="
url_futures = "https://www.nseindia.com/get-quotes/derivatives?symbol="
url_futures_quote = "https://www.nseindia.com/api/quote-derivative?symbol="


headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'accept-language': 'en,gu;q=0.9,hi;q=0.8',
    'accept-encoding': 'gzip, deflate, br'}


sess = requests.Session()
cookies = dict()


# Local methods
def _set_cookie():
    request = sess.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)

def _get_data(url):
    _set_cookie()
    response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
    if (response.status_code == 401):
        _set_cookie()
        response = sess.get(url_nf, headers=headers, timeout=5, cookies=cookies)
    if (response.status_code == 200):
        return response.text
    return ""

def _set_header():
    global url_bnf
    global url_nf
    global bnf_nearest
    global nf_nearest
    response_text = _get_data(url_indices)
    data = json.loads(response_text)
    for index in data["data"]:
        if index["index"] == "NIFTY 50":
            url_nf = index["last"]
            print("nifty")
        if index["index"] == "NIFTY BANK":
            url_bnf = index["last"]
            print("banknifty")

def _get_date_and_x_months_ago(x=6):
    date_now = date.today().strftime("%d-%m-%Y").upper()
    six_months = (date.today() + relativedelta(months=-x)).strftime("%d-%m-%Y").upper()
    return[date_now,six_months]

def _rename_index_df_columns(df):
    df = df.rename(columns={'EOD_OPEN_INDEX_VAL':'OPEN', 'EOD_HIGH_INDEX_VAL':'HIGH', 'EOD_LOW_INDEX_VAL':'LOW', 'EOD_CLOSE_INDEX_VAL':'CLOSE', 'EOD_TIMESTAMP':'DATE'})
    # df = df.reo)
    df = df[{'DATE','OPEN','HIGH','LOW','CLOSE','TIMESTAMP'}]
    return df


def _process_for_comma(df=None):
    try:
        df['OPEN'] = df['OPEN'].str.replace(',', '')
    except Exception as e: pass
    try:
        df['HIGH'] = df['HIGH'].str.replace(',', '')
    except Exception as e: pass
    try:
        df['LOW'] = df['LOW'].str.replace(',', '')
    except Exception as e: pass
    try:
        df['CLOSE'] = df['CLOSE'].str.replace(',', '')
    except Exception as e: pass

    return df


def _convert_df_ohlc_to_float(df=None):
    try:
        df['OPEN'] = df['OPEN'].astype(float)
    except Exception as e: pass
    try:
        df['HIGH'] = df['HIGH'].astype(float)
    except Exception as e: pass
    try:
        df['LOW'] = df['LOW'].astype(float)
    except Exception as e: pass
    try:
        df['CLOSE'] = df['CLOSE'].astype(float)
    except Exception as e: pass

    return df


def download_ohlc_from_nse(symbol=None, resample_method="W", period="12M"):
    tickers = t.Tickers()
    index_or_stock = tickers.check_index_or_stock(internal_name=symbol)
    dl_code = tickers.get_dl_code(internal_name=symbol)
    if index_or_stock == "INDEX":
        return download_index_ohlc_from_nse(symbol=dl_code, resample_method=resample_method, period=period)
    elif index_or_stock == "STOCK":
        return test_download_stock_ohlc_from_nse(symbol=dl_code, resample_method=resample_method, period=period)


def download_index_ohlc_from_nse(symbol=None, resample_method="W", period="12M"):

    global _nse_index_dl_url
    # _set_header()
    # _set_cookie()
    to_from = _get_date_and_x_months_ago(int(period[:-1]))
    dl_url = _nse_index_dl_url.format(urllib.parse.quote(symbol), to_from[1], to_from[0])
    response = _get_data(dl_url)
    json_data = json.loads(response)
    json_data = json_normalize(json_data['data']['indexCloseOnlineRecords'])
    df_index_ohlc = pd.DataFrame(json_data)
    df_index_ohlc = _rename_index_df_columns(df_index_ohlc)

    df_index_ohlc['DATE'] = pd.to_datetime(df_index_ohlc['DATE'], format="%d-%b-%Y")
    df_index_ohlc.set_index('DATE', inplace=True)
    if resample_method == "W":
        df_index_ohlc = df_index_ohlc.resample('W-Fri').agg({'OPEN': 'first', 'HIGH': 'max', 'LOW': 'min', 'CLOSE': 'last'})
    # IF not end of the week? How to check this.
    df_index_ohlc = df_index_ohlc.sort_index(ascending=False)
    return df_index_ohlc


def test_download_stock_ohlc_from_nse(symbol=None, resample_method="W", period="12M"):
    global _nse_cm_dl_url
    to_from = _get_date_and_x_months_ago(int(period[:-1]))
    dl_url = _nse_cm_dl_url.format(urllib.parse.quote(symbol), to_from[1], to_from[0])
    response = _get_data(dl_url)
    df_stock_ohlc = pd.read_csv(StringIO(response))
    df_stock_ohlc.columns = df_stock_ohlc.columns.str.strip()
    df_stock_ohlc.rename(columns={df_stock_ohlc.columns[0]: 'DATE'}, inplace=True)
    df_stock_ohlc.columns = df_stock_ohlc.columns.str.upper()
    df_stock_ohlc.drop(['SERIES', 'PREV. CLOSE', 'LTP', 'VWAP', '52W H', '52W L', 'VOLUME', 'VALUE', 'NO OF TRADES'], axis=1, inplace=True)
    df_stock_ohlc['TIMESTAMP'] = df_stock_ohlc['DATE']
    df_stock_ohlc['DATE'] = pd.to_datetime(df_stock_ohlc['DATE'], format="%d-%b-%Y")
    df_stock_ohlc.set_index('DATE', inplace=True)
    if resample_method == "W":
        df_stock_ohlc = df_stock_ohlc.resample('W-Fri').agg({'OPEN': 'first', 'HIGH': 'max', 'LOW': 'min', 'CLOSE': 'last'})
    # # IF not end of the week? How to check this.
    df_stock_ohlc = df_stock_ohlc.sort_index(ascending=False)
    df_stock_ohlc = _process_for_comma(df_stock_ohlc)
    df_stock_ohlc = _convert_df_ohlc_to_float(df_stock_ohlc)
    return df_stock_ohlc

def download_index_oc_from_nse():
    return []

def download_stock_oc_from_nse():
    return []

def download_index_ohlc_from_yahoo(symbol=None, interval="1wk", start=None, end=None, period="1y"):
    try:
        yahoo_name = yf.Ticker(symbol)
        # region BLOCK ONE get data from yahoo and modify it as needed
        # fetch data from Yahoo Finance
        # data = ticker.history(start="2008-01-01", end="2009-01-01", interval="1mo", back_adjust=True, auto_adjust=True, actions=False)
        data = yahoo_name.history(period=period, start=start, end=end, interval=interval, back_adjust=False,
                                  auto_adjust=False,
                                  actions=False)
        #         data = ticker.history(start="2000-01-01", interval="1mo", back_adjust=False, auto_adjust=False, actions=False)
        #         datax = ticker.history(start="2020-04-30", interval="1mo", back_adjust=False, auto_adjust=False, actions=False)

        # info = ticker.get_info()
        # fiftyTwoWeekHigh = info['fiftyTwoWeekHigh']

        #        data = ticker.history(period="max", interval="1mo")

        df = pd.DataFrame(data)
        # removes rows with Dividents and Stock splits info
        df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)

        # remove Dividents and Stock splits columns
        # df.drop(columns=["Volume", "Dividends","Stock Splits"], inplace=True)
        df.drop(columns=["Volume"], inplace=True)

        # resample to 2M duration
        #        df = df.resample('2M', loffset = offset).apply(logic)
        #        dfx = df.resample('2M').apply(logic) doesn't work

        # Calculate returns
        # df['Returns'] = df['Close'].pct_change(1)

        # Sort descending
        df.sort_index(ascending=False, inplace=True)
        # endregion
    finally:
        pass

    return df


def download_stock_ohlc_from_yahoo(symbol=None, interval="1wk", start=None, end=None, period="1y"):
    return download_index_ohlc_from_yahoo(symbol=symbol, interval=interval, start=start, end=end, period=period)

def resample_weekly(ohlc_df=None):
    return
