# Libraries
import datetime
import json
import math
import urllib

import pandas as pd
import requests
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
def round_nearest(x,num=50): return int(math.ceil(float(x)/num)*num)
def nearest_strike_bnf(x): return round_nearest(x,100)
def nearest_strike_nf(x): return round_nearest(x,50)

# Urls for fetching Data
url_oc      = "https://www.nseindia.com/option-chain"
url_bnf     = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'
url_nf      = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
url_indices = "https://www.nseindia.com/api/allIndices"
url_master  = "https://www.nseindia.com/api/master-quote"
url_index   = "https://www.nseindia.com/api/option-chain-indices?symbol="
url_equity  = "https://www.nseindia.com/api/option-chain-equities?symbol="

indexes = ["NIFTY", "BANKNIFTY"]
list_of_dfs = []
df_all = pd.DataFrame()
# Headers
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br'}

bhavcopy_col_names = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                      'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']

dict_bhavcopy_ce = {'CE.identifier': 'INSTRUMENT', 'CE.underlying': 'SYMBOL', 'CE.expiryDate': 'EXPIRY_DT',
                    'CE.strikePrice': 'STRIKE_PR', 'OPTION_TYP': 'OPTION_TYP', 'OPEN': 'OPEN', 'HIGH': 'HIGH',
                    'LOW': 'LOW', 'CE.lastPrice': 'CLOSE', 'SETTLE_PR': 'SETTLE_PR',
                    'CE.totalTradedVolume': 'CONTRACTS', 'VAL_INLAKH': 'VAL_INLAKH', 'CE.openInterest': 'OPEN_INT',
                    'CE.changeinOpenInterest': 'CHG_IN_OI', 'TIMESTAMP': 'TIMESTAMP'}
df_bhavcopy = pd.DataFrame(columns=bhavcopy_col_names)

instrument_types = ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']
timestamp = datetime.date.today().strftime("%d-%b-%Y").upper()

sess = requests.Session()
cookies = dict()

# Local methods
def set_cookie():
    request = sess.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)

def get_data(url):
    set_cookie()
    response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
    if(response.status_code==401):
        set_cookie()
        response = sess.get(url_nf, headers=headers, timeout=5, cookies=cookies)
    if(response.status_code==200):
        return response.text
    return ""

def set_header():
    global bnf_ul
    global nf_ul
    global bnf_nearest
    global nf_nearest
    response_text = get_data(url_indices)
    data = json.loads(response_text)
    for index in data["data"]:
        if index["index"]=="NIFTY 50":
            nf_ul = index["last"]
            print("nifty")
        if index["index"]=="NIFTY BANK":
            bnf_ul = index["last"]
            print("banknifty")
    bnf_nearest=nearest_strike_bnf(bnf_ul)
    nf_nearest=nearest_strike_nf(nf_ul)

# Showing Header in structured format with Last Price and Nearest Strike

def print_header(index="",ul=0,nearest=0):
    print(strPurple( index.ljust(12," ") + " => ")+ strLightPurple(" Last Price: ") + strBold(str(ul)) + strLightPurple(" Nearest Strike: ") + strBold(str(nearest)))

def print_hr():
    print(strYellow("|".rjust(70,"-")))

# Fetching CE and PE data based on Nearest Expiry Date
def print_oi(num,step,nearest,url):
    strike = nearest - (step*num)
    start_strike = nearest - (step*num)
    response_text = get_data(url)
    data = json.loads(response_text)
    currExpiryDate = data["records"]["expiryDates"][0]
    for item in data['records']['data']:
        if item["expiryDate"] == currExpiryDate:
            if item["strikePrice"] == strike and item["strikePrice"] < start_strike+(step*num*2):
                #print(strCyan(str(item["strikePrice"])) + strGreen(" CE ") + "[ " + strBold(str(item["CE"]["openInterest"]).rjust(10," ")) + " ]" + strRed(" PE ")+"[ " + strBold(str(item["PE"]["openInterest"]).rjust(10," ")) + " ]")
                print(data["records"]["expiryDates"][0] + " " + str(item["strikePrice"]) + " CE " + "[ " + strBold(str(item["CE"]["openInterest"]).rjust(10," ")) + " ]" + " PE " + "[ " + strBold(str(item["PE"]["openInterest"]).rjust(10," ")) + " ]")
                strike = strike + step

# Finding highest Open Interest of People's in CE based on CE data
def highest_oi_CE(num,step,nearest,url):
    strike = nearest - (step*num)
    start_strike = nearest - (step*num)
    response_text = get_data(url)
    data = json.loads(response_text)
    currExpiryDate = data["records"]["expiryDates"][0]
    max_oi = 0
    max_oi_strike = 0
    for item in data['records']['data']:
        if item["expiryDate"] == currExpiryDate:
            if item["strikePrice"] == strike and item["strikePrice"] < start_strike+(step*num*2):
                if item["CE"]["openInterest"] > max_oi:
                    max_oi = item["CE"]["openInterest"]
                    max_oi_strike = item["strikePrice"]
                strike = strike + step
    return max_oi_strike

# Finding highest Open Interest of People's in PE based on PE data
def highest_oi_PE(num,step,nearest,url):
    strike = nearest - (step*num)
    start_strike = nearest - (step*num)
    response_text = get_data(url)
    data = json.loads(response_text)
    currExpiryDate = data["records"]["expiryDates"][0]
    max_oi = 0
    max_oi_strike = 0
    for item in data['records']['data']:
        if item["expiryDate"] == currExpiryDate:
            if item["strikePrice"] == strike and item["strikePrice"] < start_strike+(step*num*2):
                if item["PE"]["openInterest"] > max_oi:
                    max_oi = item["PE"]["openInterest"]
                    max_oi_strike = item["strikePrice"]
                strike = strike + step
    return max_oi_strike

def download_multiple_symbols_option_chain(df_symbols, index=True):
    global df_all
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
            response_text = get_data(url+urllib.parse.quote(scrip_name))
            data = json.loads(response_text)
            data2 = data['records']['data']
            # imp
            # df_transpose = json_normalize(data['records']['data']).T.transpose()
            # # df_transpose.to_csv(scrip_name+".csv")
            # df_all = df_all.append(df_transpose)
            # imp end
            df_temp = json_normalize(data2).T.transpose()
            df_temp_ce = df_temp[[col for col in df_temp if col.startswith('CE')]]
            df_temp_ce = df_temp_ce[df_temp_ce['CE.strikePrice'].notna()]
            df_temp_ce['CE.identifier'] = df_temp_ce['CE.identifier'].str[:6]
            df_temp_ce['OPEN'] = df_temp_ce['CE.lastPrice']-df_temp_ce['CE.change']
            df_temp_ce['HIGH'] = df_temp_ce['CE.lastPrice']
            df_temp_ce['LOW'] = df_temp_ce['CE.lastPrice']
            df_temp_ce['SETTLE_PR'] = df_temp_ce['CE.lastPrice']
            df_temp_ce['VAL_INLAKH'] = 0
            df_temp_ce['TIMESTAMP'] = timestamp
            df_temp_ce = df_temp_ce.drop(columns=['CE.pchangeinOpenInterest','CE.change', 'CE.pChange','CE.impliedVolatility', 'CE.totalBuyQuantity', 'CE.totalSellQuantity', 'CE.bidQty',
                                     'CE.bidprice', 'CE.askQty', 'CE.askPrice', 'CE.underlyingValue',])

            df_temp_ce.rename(columns=dict_bhavcopy_ce, inplace=True)


            df_temp_ce['OPTION_TYP'] = 'CE'
            df_temp_pe = df_temp[[col for col in df_temp if col.startswith('PE')]]
            df_temp_pe = df_temp_pe[df_temp_pe['PE.strikePrice'].notna()]
            df_temp_pe = df_temp_pe.drop(columns=['PE.impliedVolatility', 'PE.totalBuyQuantity', 'PE.totalSellQuantity', 'PE.bidQty',
                                     'PE.bidprice', 'PE.askQty', 'PE.askPrice', 'PE.underlyingValue'])
            print (df_temp.head())

        except Exception as e:
            print (e)
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

response_text = get_data(url_nf)
# print (response_text)
data = json.loads(response_text)
data2 = data['records']['data']
df_transpose = json_normalize(data['records']['data']).T.transpose()
print(df_transpose.head())
df_transpose.to_csv("csvfile.csv")

# master_index_list = json.loads(get_data(url_indices))
master_index_df = pd.DataFrame(indexes)
download_multiple_symbols_option_chain(master_index_df)


# master_stock_list = json.loads(get_data(url_master))
# master_stock_df = pd.DataFrame(master_stock_list)
# download_multiple_symbols_option_chain(master_stock_df, False)

df_all.to_csv("all.csv")
# print (response_text)
