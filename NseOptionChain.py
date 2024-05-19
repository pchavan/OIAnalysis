# Libraries
import datetime
import json
import math
import sys
import urllib

import pandas as pd
import requests
import win32com.client
from line_profiler_pycharm import profile
from pandas.io.json import json_normalize


# Python program to print
# colored text and background
def strRed(skk):         return "\033[91m {}\033[00m".format(skk)
def strGreen(skk):       return "\033[92m {}\033[00m".format(skk)
def strYellow(skk):      return "\033[93m {}\033[00m".format(skk)
def strLightPurple(skk): return "\033[94m {}\033[00m".format(skk)
def strPurple(skk):      return "\033[95m {}\033[00m".format(skk)
def strCyan(skk):        return "\033[96m {}\033[00m".format(skk)
def strLightGray(skk):   return "\033[97m {}\033[00m".format(skk)
def strBlack(skk):       return "\033[98m {}\033[00m".format(skk)
def strBold(skk):        return "\033[1m {}\033[0m".format(skk)


# Method to get nearest strikes
def round_nearest(x, num=50): return int(math.ceil(float(x) / num) * num)
def nearest_strike_bnf(x): return round_nearest(x, 100)
def nearest_strike_nf(x): return round_nearest(x, 50)
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

indexes = ["NIFTY", "BANKNIFTY"]
nifty_50 = ['NIFTY', 'ADANIENT', 'ADANIPORTS', 'APOLLOHOSP', 'ASIANPAINT', 'AXISBANK', 'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV',
            'BPCL', 'BHARTIARTL', 'BRITANNIA', 'CIPLA', 'COALINDIA', 'DIVISLAB', 'DRREDDY', 'EICHERMOT', 'GRASIM',
            'HCLTECH', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDUNILVR', 'ICICIBANK', 'ITC', 'INDUSINDBK',
            'INFY', 'JSWSTEEL', 'KOTAKBANK', 'LTIM', 'LT', 'M&M', 'MARUTI', 'NTPC', 'NESTLEIND', 'ONGC', 'POWERGRID',
            'RELIANCE', 'SBILIFE', 'SHRIRAMFIN', 'SBIN', 'SUNPHARMA', 'TCS', 'TATACONSUM', 'TATAMOTORS', 'TATASTEEL',
            'TECHM', 'TITAN', 'ULTRACEMCO', 'WIPRO']
banknifty = ['BANKNIFTY', 'AXISBANK', 'HDFCBANK', 'ICICIBANK', 'INDUSINDBK', 'KOTAKBANK', 'SBIN', 'PNB', 'BANKBARODA', 'AUBANK', 'IDFCFIRSTB', 'FEDERALBNK', 'BANDHANBNK']
list_of_dfs = []
# df_all = pd.DataFrame()
# Headers
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'accept-language': 'en,gu;q=0.9,hi;q=0.8',
    'accept-encoding': 'gzip, deflate, br'}

bhavcopy_col_names = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                      'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']

dict_bhavcopy_ce = {'CE.identifier': 'INSTRUMENT', 'CE.underlying': 'SYMBOL', 'CE.expiryDate': 'EXPIRY_DT',
                    'CE.strikePrice': 'STRIKE_PR', 'OPTION_TYP': 'OPTION_TYP', 'OPEN': 'OPEN', 'HIGH': 'HIGH',
                    'LOW': 'LOW', 'CE.lastPrice': 'CLOSE', 'SETTLE_PR': 'SETTLE_PR',
                    'CE.totalTradedVolume': 'CONTRACTS', 'VAL_INLAKH': 'VAL_INLAKH', 'CE.openInterest': 'OPEN_INT',
                    'CE.changeinOpenInterest': 'CHG_IN_OI', 'TIMESTAMP': 'TIMESTAMP'}

dict_bhavcopy_pe = {'PE.identifier': 'INSTRUMENT', 'PE.underlying': 'SYMBOL', 'PE.expiryDate': 'EXPIRY_DT',
                    'PE.strikePrice': 'STRIKE_PR', 'OPTION_TYP': 'OPTION_TYP', 'OPEN': 'OPEN', 'HIGH': 'HIGH',
                    'LOW': 'LOW', 'PE.lastPrice': 'CLOSE', 'SETTLE_PR': 'SETTLE_PR',
                    'PE.totalTradedVolume': 'CONTRACTS', 'VAL_INLAKH': 'VAL_INLAKH', 'PE.openInterest': 'OPEN_INT',
                    'PE.changeinOpenInterest': 'CHG_IN_OI', 'TIMESTAMP': 'TIMESTAMP'}

df_bhavcopy = pd.DataFrame(columns=bhavcopy_col_names)

instrument_types = ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']
timestamp = datetime.date.today().strftime("%d-%b-%Y").upper()

sess = requests.Session()
cookies = dict()


# Local methods
@profile
def set_cookie():
    request = sess.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)

@profile
def get_data(url):
    set_cookie()
    response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
    if (response.status_code == 401):
        set_cookie()
        response = sess.get(url_nf, headers=headers, timeout=5, cookies=cookies)
    if (response.status_code == 200):
        return response.text
    return ""

@profile
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
    bnf_nearest = nearest_strike_bnf(url_bnf)
    nf_nearest = nearest_strike_nf(url_nf)


# Showing Header in structured format with Last Price and Nearest Strike
@profile
def print_header(index="", ul=0, nearest=0):
    print(
        strPurple(index.ljust(12, " ") + " => ") + strLightPurple(" Last Price: ") + strBold(str(ul)) + strLightPurple(
            " Nearest Strike: ") + strBold(str(nearest)))


def print_hr():
    print(strYellow("|".rjust(70, "-")))


# Fetching CE and PE data based on Nearest Expiry Date
def print_oi(num, step, nearest, url):
    strike = nearest - (step * num)
    start_strike = nearest - (step * num)
    response_text = get_data(url)
    data = json.loads(response_text)
    currExpiryDate = data["records"]["expiryDates"][0]
    for item in data['records']['data']:
        if item["expiryDate"] == currExpiryDate:
            if item["strikePrice"] == strike and item["strikePrice"] < start_strike + (step * num * 2):
                # print(strCyan(str(item["strikePrice"])) + strGreen(" CE ") + "[ " + strBold(str(item["CE"]["openInterest"]).rjust(10," ")) + " ]" + strRed(" PE ")+"[ " + strBold(str(item["PE"]["openInterest"]).rjust(10," ")) + " ]")
                print(data["records"]["expiryDates"][0] + " " + str(item["strikePrice"]) + " CE " + "[ " + strBold(
                    str(item["CE"]["openInterest"]).rjust(10, " ")) + " ]" + " PE " + "[ " + strBold(
                    str(item["PE"]["openInterest"]).rjust(10, " ")) + " ]")
                strike = strike + step


# Finding highest Open Interest of People's in CE based on CE data
def highest_oi_CE(num, step, nearest, url):
    strike = nearest - (step * num)
    start_strike = nearest - (step * num)
    response_text = get_data(url)
    data = json.loads(response_text)
    currExpiryDate = data["records"]["expiryDates"][0]
    max_oi = 0
    max_oi_strike = 0
    for item in data['records']['data']:
        if item["expiryDate"] == currExpiryDate:
            if item["strikePrice"] == strike and item["strikePrice"] < start_strike + (step * num * 2):
                if item["CE"]["openInterest"] > max_oi:
                    max_oi = item["CE"]["openInterest"]
                    max_oi_strike = item["strikePrice"]
                strike = strike + step
    return max_oi_strike


# Finding highest Open Interest of People's in PE based on PE data
def highest_oi_PE(num, step, nearest, url):
    strike = nearest - (step * num)
    start_strike = nearest - (step * num)
    response_text = get_data(url)
    data = json.loads(response_text)
    currExpiryDate = data["records"]["expiryDates"][0]
    max_oi = 0
    max_oi_strike = 0
    for item in data['records']['data']:
        if item["expiryDate"] == currExpiryDate:
            if item["strikePrice"] == strike and item["strikePrice"] < start_strike + (step * num * 2):
                if item["PE"]["openInterest"] > max_oi:
                    max_oi = item["PE"]["openInterest"]
                    max_oi_strike = item["strikePrice"]
                strike = strike + step
    return max_oi_strike

@profile
def download_multiple_symbols_option_chain_and_futures(df_symbols, index=True):
    global df_bhavcopy
    df_bhavcopy2 = pd.DataFrame(columns=bhavcopy_col_names)

    url = url_index
    if index == False:
        url = url_equity
    index = 0
    for row in df_symbols.iterrows():
        index = index + 1
        if index > 200:
            break
        scrip_name = row[1][0]
        print(scrip_name)
        try:
            # FUTURES DATA
            # api_req = req.get('https://www.nseindia.com/api/quote-derivative?symbol=NIFTY', headers=headers).json()
            data = []
            api_req = json.loads(get_data(url_futures_quote + urllib.parse.quote(scrip_name)))
            # x = api_req.get('stocks')
            # TODO use json_normalize(api_req['stocks']) and vectorization and remove the for loop
            normalized = json_normalize(api_req['stocks'])
            normalized['openInterest'] = normalized['marketDeptOrderBook.tradeInfo.openInterest'] * normalized['marketDeptOrderBook.tradeInfo.marketLot']
            normalized['changeinOpenInterest'] = normalized['marketDeptOrderBook.tradeInfo.changeinOpenInterest'] * normalized['marketDeptOrderBook.tradeInfo.marketLot']

            # normalized = normalized.drop(
            #     columns=['volumeFreezeQuantity', 'metadata.prevClose', 'metadata.change', 'metadata.pChange',
            #              'metadata.numberOfContractsTraded', 'metadata.totalTurnover',
            #              'marketDeptOrderBook.totalBuyQuantity', 'marketDeptOrderBook.totalSellQuantity',
            #              'marketDeptOrderBook.bid', 'marketDeptOrderBook.ask',
            #              'marketDeptOrderBook.carryOfCost.price.bestBuy',
            #              'marketDeptOrderBook.carryOfCost.price.bestSell',
            #              'marketDeptOrderBook.carryOfCost.price.lastPrice',
            #              'marketDeptOrderBook.carryOfCost.carry.bestBuy',
            #              'marketDeptOrderBook.carryOfCost.carry.bestSell',
            #              'marketDeptOrderBook.carryOfCost.carry.lastPrice', 'marketDeptOrderBook.tradeInfo.vmap',
            #              'marketDeptOrderBook.tradeInfo.premiumTurnover',
            #              'marketDeptOrderBook.tradeInfo.pchangeinOpenInterest',
            #              'marketDeptOrderBook.tradeInfo.marketLot', 'marketDeptOrderBook.otherInfo.settlementPrice',
            #              'marketDeptOrderBook.otherInfo.dailyvolatility',
            #              'marketDeptOrderBook.otherInfo.annualisedVolatility',
            #              'marketDeptOrderBook.otherInfo.impliedVolatility',
            #              'marketDeptOrderBook.otherInfo.clientWisePositionLimits',
            #              'marketDeptOrderBook.otherInfo.marketWidePositionLimits'])
            # normalized.loc[(normalized["metadata.optionType"] == "-"), "metadata.optionType"] = "XX"

            df_bhavcopy2['INSTRUMENT'] = normalized['metadata.identifier'].str.slice(stop=6)
            df_bhavcopy2['SYMBOL'] = scrip_name
            df_bhavcopy2['EXPIRY_DT'] = normalized['metadata.expiryDate']
            df_bhavcopy2['STRIKE_PR'] = normalized['metadata.strikePrice']
            df_bhavcopy2['OPTION_TYP'] = normalized['metadata.optionType']
            df_bhavcopy2['OPEN'] = normalized['metadata.openPrice']
            df_bhavcopy2['HIGH'] = normalized['metadata.highPrice']
            df_bhavcopy2['LOW'] = normalized['metadata.lowPrice']
            df_bhavcopy2['CLOSE'] = normalized['metadata.lastPrice']
            df_bhavcopy2['SETTLE_PR'] = normalized['metadata.lastPrice']
            df_bhavcopy2['CONTRACTS'] = normalized['marketDeptOrderBook.tradeInfo.tradedVolume']
            df_bhavcopy2['VAL_INLAKH'] = normalized['marketDeptOrderBook.tradeInfo.value']
            df_bhavcopy2['OPEN_INT'] = normalized['openInterest']
            df_bhavcopy2['CHG_IN_OI'] = normalized['changeinOpenInterest']
            df_bhavcopy2['TIMESTAMP'] = timestamp
            df_bhavcopy2.loc[(df_bhavcopy2["OPTION_TYP"] == "-"), ["OPTION_TYP"]] = "XX"
            df_bhavcopy2.loc[(df_bhavcopy2["OPTION_TYP"] == "Call"), ["OPTION_TYP"]] = "CE"
            df_bhavcopy2.loc[(df_bhavcopy2["OPTION_TYP"] == "Put"), ["OPTION_TYP"]] = "PE"


            # for item in api_req['stocks']:
            #     if (item['metadata']['instrumentType'] == 'Stock Futures') or (item['metadata']['instrumentType'] == 'Index Futures'):
            #         data.append([
            #             item['metadata']['identifier'][:6],
            #             scrip_name,
            #             item['metadata']['expiryDate'],
            #             "0", "XX",
            #             item['metadata']['openPrice'],
            #             item['metadata']['highPrice'],
            #             item['metadata']['lowPrice'],
            #             item['metadata']['lastPrice'],
            #             item['metadata']['lastPrice'],
            #             item['marketDeptOrderBook']['tradeInfo']['tradedVolume'],
            #             item['marketDeptOrderBook']['tradeInfo']['value'],
            #             item['marketDeptOrderBook']['tradeInfo']['openInterest'],
            #             item['marketDeptOrderBook']['tradeInfo']['changeinOpenInterest'],
            #             timestamp
            #         ])
            #     if (item['metadata']['instrumentType'] == 'Index Options') or (item['metadata']['instrumentType'] == 'Stock Options'):
            #         optionType = "AA"
            #         if (item['metadata']['optionType'] == "Call"):
            #             optionType = "CE"
            #         else:
            #             if (item['metadata']['optionType'] == "Put"):
            #                 optionType = "PE"
            #         data.append([
            #             item['metadata']['identifier'][:6],
            #             scrip_name,
            #             item['metadata']['expiryDate'],
            #             item['metadata']['strikePrice'],
            #             optionType,
            #             item['metadata']['openPrice'],
            #             item['metadata']['highPrice'],
            #             item['metadata']['lowPrice'],
            #             item['metadata']['lastPrice'],
            #             item['metadata']['lastPrice'],
            #             item['marketDeptOrderBook']['tradeInfo']['tradedVolume'],
            #             item['marketDeptOrderBook']['tradeInfo']['value'],
            #             item['marketDeptOrderBook']['tradeInfo']['openInterest'],
            #             item['marketDeptOrderBook']['tradeInfo']['changeinOpenInterest'],
            #             timestamp
            #         ])

            # df_temp_futures = pd.DataFrame(data, columns=bhavcopy_col_names)
            # df_temp_futures.sort_values(by=['INSTRUMENT','EXPIRY_DT', 'STRIKE_PR','OPTION_TYP'], ascending = [True, True, True, True], inplace=True)
            # OPTIONS CHAIN
            # response_text = get_data(url + urllib.parse.quote(scrip_name))
            # data = json.loads(response_text)
            # data2 = data['records']['data']
            # # imp
            # # df_transpose = json_normalize(data['records']['data']).T.transpose()
            # # # df_transpose.to_csv(scrip_name+".csv")
            # # df_all = df_all.append(df_transpose)
            # # imp end
            # df_temp = json_normalize(data2).T.transpose()
            #
            # df_temp_ce = df_temp[[col for col in df_temp if col.startswith('CE')]]
            # df_temp_ce = df_temp_ce[df_temp_ce['CE.strikePrice'].notna()]
            # df_temp_ce['CE.identifier'] = df_temp_ce['CE.identifier'].str[:6]
            # df_temp_ce['OPEN'] = df_temp_ce['CE.lastPrice'] - df_temp_ce['CE.change']
            # df_temp_ce['HIGH'] = df_temp_ce['CE.lastPrice']
            # df_temp_ce['LOW'] = df_temp_ce['CE.lastPrice']
            # df_temp_ce['SETTLE_PR'] = df_temp_ce['CE.lastPrice']
            # df_temp_ce['VAL_INLAKH'] = 0
            # df_temp_ce['TIMESTAMP'] = timestamp
            # df_temp_ce = df_temp_ce.drop(
            #     columns=['CE.pchangeinOpenInterest', 'CE.change', 'CE.pChange', 'CE.impliedVolatility',
            #              'CE.totalBuyQuantity', 'CE.totalSellQuantity', 'CE.bidQty',
            #              'CE.bidprice', 'CE.askQty', 'CE.askPrice', 'CE.underlyingValue'])
            # df_temp_ce['OPTION_TYP'] = 'CE'
            # df_temp_ce.rename(columns=dict_bhavcopy_ce, inplace=True)
            # df_temp_ce = df_temp_ce[bhavcopy_col_names]
            # # df_temp_ce = df_temp_ce.sort_values(['EXPIRY_DT', 'STRIKE_PR'], ascending=[False, False])
            #
            # df_temp_pe = df_temp[[col for col in df_temp if col.startswith('PE')]]
            # df_temp_pe = df_temp_pe[df_temp_pe['PE.strikePrice'].notna()]
            # df_temp_pe['PE.identifier'] = df_temp_pe['PE.identifier'].str[:6]
            # df_temp_pe['OPEN'] = df_temp_pe['PE.lastPrice'] - df_temp_pe['PE.change']
            # df_temp_pe['HIGH'] = df_temp_pe['PE.lastPrice']
            # df_temp_pe['LOW'] = df_temp_pe['PE.lastPrice']
            # df_temp_pe['SETTLE_PR'] = df_temp_pe['PE.lastPrice']
            # df_temp_pe['VAL_INLAKH'] = 0
            # df_temp_pe['TIMESTAMP'] = timestamp
            # df_temp_pe = df_temp_pe.drop(
            #     columns=['PE.pchangeinOpenInterest', 'PE.change', 'PE.pChange', 'PE.impliedVolatility',
            #              'PE.totalBuyQuantity', 'PE.totalSellQuantity', 'PE.bidQty',
            #              'PE.bidprice', 'PE.askQty', 'PE.askPrice', 'PE.underlyingValue'])
            # df_temp_pe['OPTION_TYP'] = 'PE'
            # df_temp_pe.rename(columns=dict_bhavcopy_pe, inplace=True)
            # df_temp_pe = df_temp_pe[bhavcopy_col_names]
            # # df_temp_pe.sort_values(['EXPIRY_DT', 'STRIKE_PR'], inplace=True)

            # df_bhavcopy = df_bhavcopy.append([df_temp_futures,df_temp_ce,df_temp_pe])
            df_bhavcopy = df_bhavcopy.append(df_bhavcopy2)
            # df_bhavcopy = df_bhavcopy2
            print(df_bhavcopy.head())

        except Exception as e:
            print(e)
            continue


set_header()
print('\033c')
# print_hr()
# print_header("Nifty",nf_ul,nf_nearest)
# print_hr()
# print_oi(10,50,nf_nearest,url_nf)
# print_hr()
# print_header("Bank Nifty",bnf_ul,bnf_nearest)
# print_hr()
# print_oi(10,100,bnf_nearest,url_bnf)
# print_hr()
#
# # Finding Highest OI in Call Option In Nifty
# nf_highestoi_CE = highest_oi_CE(10,50,nf_nearest,url_nf)
#
# # Finding Highet OI in Put Option In Nifty
# nf_highestoi_PE = highest_oi_PE(10,50,nf_nearest,url_nf)
#
# # Finding Highest OI in Call Option In Bank Nifty
# bnf_highestoi_CE = highest_oi_CE(10,100,bnf_nearest,url_bnf)
#
# # Finding Highest OI in Put Option In Bank Nifty
# bnf_highestoi_PE = highest_oi_PE(10,100,bnf_nearest,url_bnf)
#
#
# print(strCyan(str("Major Support in Nifty:")) + str(nf_highestoi_CE))
# print(strCyan(str("Major Resistance in Nifty:")) + str(nf_highestoi_PE))
# print(strPurple(str("Major Support in Bank Nifty:")) + str(bnf_highestoi_CE))
# print(strPurple(str("Major Resistance in Bank Nifty:")) + str(bnf_highestoi_PE))

# response_text = get_data(url_nf)
# # print (response_text)
# data = json.loads(response_text)
# data2 = data['records']['data']
# df_transpose = json_normalize(data['records']['data']).T.transpose()
# print(df_transpose.head())
# df_transpose.to_csv("csvfile.csv")

# master_index_list = json.loads(get_data(url_indices))
param = "all"
if (len(sys.argv) > 1):
    param = sys.argv[1]

param = param.lower()

def switch(param):
    if param == 'all':
        print ("Downloading all index and stock F&O data...")
        master_index_df = pd.DataFrame(indexes)
        download_multiple_symbols_option_chain_and_futures(master_index_df)

        master_stock_list = json.loads(get_data(url_master))
        master_stock_df = pd.DataFrame(master_stock_list)
        download_multiple_symbols_option_chain_and_futures(master_stock_df, False)
    elif param == "index":
        print ("Downloading all index F&O data...")
        master_index_df = pd.DataFrame(indexes)
        download_multiple_symbols_option_chain_and_futures(master_index_df)
    elif param == "stocks":
        print ("Downloading all stock F&O data...")
        master_stock_list = json.loads(get_data(url_master))
        master_stock_df = pd.DataFrame(master_stock_list)
        download_multiple_symbols_option_chain_and_futures(master_stock_df, False)
    elif param == "nifty":
        print ("Downloading NIFTY50 F&O data...")
        df_nifty_50 = pd.DataFrame(nifty_50)
        download_multiple_symbols_option_chain_and_futures(df_nifty_50, False)
    elif param == "banknifty":
        print ("Downloading BANKNIFTY F&O data...")
        df_banknifty = pd.DataFrame(banknifty)
        download_multiple_symbols_option_chain_and_futures(df_banknifty, False)

def process_excels():
    xl = win32com.client.Dispatch("Excel.Application")  # instantiate excel app

    wb = xl.Workbooks.Open(r'C:\CondaPrograms\Python\OIAnalysis\Excels\CEPEv1.1.xlsm')
    xl.Application.Run('CEPEv1.1.xlsm!modProcess.ClearHistory')
    xl.Application.Run('CEPEv1.1.xlsm!modProcess.ProcessBHAVFromFile')
    wb.Save()

    wb = xl.Workbooks.Open(r'C:\CondaPrograms\Python\OIAnalysis\Excels\OptionsAnalyticsScanner.xlsm')
    xl.Application.Run('OptionsAnalyticsScanner.xlsm!modProcess.ProcessBHAVFromFile')
    wb.Save()
    # xl.Application.Quit()

print (datetime.datetime.now().strftime("%H:%M"))
switch(param)
df_bhavcopy.to_csv("df_bhavcopy.csv", index=False)
process_excels()
print ("Done.")

