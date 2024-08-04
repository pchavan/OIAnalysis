import json
import urllib
from datetime import date
import OHLCDownloader as dl
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from pandas.io.json import json_normalize

NIFTY = "NIFTY"
nse_index_dl_url="https://www.nseindia.com/api/historical/indicesHistory?indexType={0}&from={1}&to={2}"
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

_rsc_cache = {}
_ohlc_cache = {}

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'accept-language': 'en,gu;q=0.9,hi;q=0.8',
    'accept-encoding': 'gzip, deflate, br'}


sess = requests.Session()
cookies = dict()


# Local methods
def set_cookie():
    request = sess.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)

def get_data(url):
    set_cookie()
    response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
    if (response.status_code == 401):
        set_cookie()
        response = sess.get(url_nf, headers=headers, timeout=5, cookies=cookies)
    if (response.status_code == 200):
        return response.text
    return ""

def set_header():
    global url_bnf
    global url_nf
    global bnf_nearest
    global nf_nearest
    response_text = get_data(url_indices)
    data = json.loads(response_text)
    for index in data["data"]:
        if index["index"] == "NIFTY 50":
            url_nf = index["last"]
            print("nifty")
        if index["index"] == "NIFTY BANK":
            url_bnf = index["last"]
            print("banknifty")



def get_yahoo_symbol(symbol=None):
    special_symbols = {'NIFTY': '^NSEI', 'BANKNIFTY': '^NSEBANK', 'CNXAUTO': '^CNXAUTO', 'CNXCOMMODITIES': '^CNXCMDT',
                       'CNXCONSUMPTION': '^CNXCONSUM', 'CNXENERGY': '^CNXENERGY', 'CNXFINANCE': 'NIFTY_FIN_SERVICE.NS',
                       'CNXFMCG': '^CNXFMCG', 'CNXINFRA': '^CNXINFRA', 'CNXIT': '^CNXIT',
                       'CNXMEDIA': '^CNXMEDIA', 'CNXMETAL': '^CNXMETAL', 'CNXPHARMA': '^CNXPHARMA', 'CNXPSE': '^CNXPSE',
                       'CNXPSUBANK': '^CNXPSUBANK', 'CNXREALITY': '^CNXREALTY', 'CNXSERVICE': '^CNXSERVICE',
                       'NIFTYPVTBANK': 'NIFTY_PVT_BANK.NS', 'DUMMYSANOF': 'SANOFI.NS', 'XXX': '^XXX',
                       'XXX': '^XXX'}

    if symbol == None:
        return None

    result = special_symbols.get(symbol)
    if result == None:
        return symbol + '.NS'
    else:
        return result


def get_index_nse_url(index_name):
    global nse_index_dl_url
    tickers = pd.read_excel("./Excels/Tickers.xlsx", sheet_name='NSE_INDEXES')
    dl_name = tickers.loc[(tickers["INTERNAL_NAME"] == index_name), ["DL_CODE"]]
    to_from = get_date_and_x_months_ago(6)
    dl_url = nse_index_dl_url.format(urllib.parse.quote(dl_name.iloc[0][0]),to_from[1], to_from[0])
    # print(dl_url)
    return dl_url


def get_index_data_from_nse(index_name=None):
    if index_name==None: index_name = NIFTY
    url = get_index_nse_url(index_name)

    response = get_data(url)
    json_data = json.loads(response)
    json_data = json_normalize(json_data['data']['indexCloseOnlineRecords'])
    df = pd.DataFrame(json_data)
    return df
    # print(response.text)

def rename_index_df_columns(df):
    df = df.rename(columns={'EOD_OPEN_INDEX_VAL':'OPEN', 'EOD_HIGH_INDEX_VAL':'HIGH', 'EOD_LOW_INDEX_VAL':'LOW', 'EOD_CLOSE_INDEX_VAL':'CLOSE', 'EOD_TIMESTAMP':'DATE'})
    # df = df.reo)
    df = df[{'DATE','OPEN','HIGH','LOW','CLOSE','TIMESTAMP'}]
    return df

def get_date_and_x_months_ago(x=6):
    date_now = date.today().strftime("%d-%m-%Y").upper()
    six_months = (date.today() + relativedelta(months=-x)).strftime("%d-%m-%Y").upper()
    return[date_now,six_months]

def get_all_nse_indexes():
    tickers = pd.read_excel("./Excels/Tickers.xlsx", sheet_name='NSE_INDEX_N_STOCKS')
    indexes = tickers.loc[(tickers["TYPE"] == 'INDEX'), ["INTERNAL_NAME"]]
    return indexes

def check_index_or_stock(internal_name="NIFTY"):

    tickers = pd.read_excel("./Excels/Tickers.xlsx", sheet_name='NSE_INDEX_N_STOCKS')
    indexes = tickers.loc[(tickers["INTERNAL_NAME"] == internal_name)]

    return indexes.iloc[0]["TYPE"]


def _check_rsc_cache(symbol=None):
    global _rsc_cache
    if None == symbol:
        return None

    for key, values in _rsc_cache.items():
        if key == symbol:
            return values
    return None


def _get_rsc_for_symbol(symbol_name = None, symbol_df=None, compare_df=None):
    global _rsc_cache
    # if None == symbol_name or None == symbol_df or None == compare_df:
    #     return None
    rsc = _check_rsc_cache(symbol=symbol_name)
    if rsc is not None:
        return rsc

    s_i = 1
    len = 13
    multiplier = 109
    rsc0 = (((symbol_df['CLOSE'][s_i] / symbol_df['CLOSE'][s_i + len]) / (
            compare_df['CLOSE'][s_i] / compare_df['CLOSE'][s_i + len])) - 1) * multiplier
    rsc1 = (((symbol_df['CLOSE'][s_i + 1] / symbol_df['CLOSE'][s_i + len + 1]) / (
            compare_df['CLOSE'][s_i + 1] / compare_df['CLOSE'][s_i + len + 1])) - 1) * multiplier
    rsc2 = (((symbol_df['CLOSE'][s_i + 2] / symbol_df['CLOSE'][s_i + len + 2]) / (
            compare_df['CLOSE'][s_i + 2] / compare_df['CLOSE'][s_i + len + 2])) - 1) * multiplier

    rsc = [rsc0, rsc1, rsc2]
    _rsc_cache[symbol_name] = rsc
    return rsc


def test_index_dl_endtoend():
    df_indexes = get_all_nse_indexes()
    df_nifty = []
    s_i = 1
    len = 13
    for index in df_indexes['INTERNAL_NAME']:
        print('\n', index)
        try:
            # df_index_data = get_index_data_from_nse(index)
            df_index_data = get_index_data_from_nse(index)
            df_index_data = rename_index_df_columns(df_index_data)
            df_index_data['DATE']=pd.to_datetime(df_index_data['DATE'],format="%d-%b-%Y")
            df_index_data.set_index('DATE', inplace=True)
            df_index_data = df_index_data.resample('W-Fri').agg({'OPEN': 'first','HIGH': 'max','LOW': 'min','CLOSE': 'last'})
            # IF not end of the week? How to check this.
            df_index_data = df_index_data.sort_index(ascending=False)
            if index == 'NIFTY': df_nifty = df_index_data
            # print(df_index_data.head())
            if index != 'NIFTY':
                rsc = _get_rsc_for_symbol(symbol_name=index, symbol_df=df_index_data, compare_df=df_nifty)

                rsc0 = (((df_index_data['CLOSE'][s_i] / df_index_data['CLOSE'][s_i + len]) / (
                            df_nifty['CLOSE'][s_i] / df_nifty['CLOSE'][s_i + len])) - 1) * 109
                rsc1 = (((df_index_data['CLOSE'][s_i + 1] / df_index_data['CLOSE'][s_i + len + 1]) / (
                            df_nifty['CLOSE'][s_i + 1] / df_nifty['CLOSE'][s_i + len + 1])) - 1) * 109
                rsc2 = (((df_index_data['CLOSE'][s_i + 2] / df_index_data['CLOSE'][s_i + len + 2]) / (
                            df_nifty['CLOSE'][s_i + 2] / df_nifty['CLOSE'][s_i + len + 2])) - 1) * 109
                rsc3 = ''
                # if index != 'NIFTY_MIDSML_HLTH':
                #     rsc3 = (((df_index_data['CLOSE'][s_i + 3] / df_index_data['CLOSE'][s_i + len + 3]) / (
                #                 df_nifty['CLOSE'][s_i + 3] / df_nifty['CLOSE'][s_i + len + 3])) - 1) * 109
                print(rsc0, ' ', rsc1, ' ', rsc2, ' ', rsc3)
        except Exception as e:
            print(e)
