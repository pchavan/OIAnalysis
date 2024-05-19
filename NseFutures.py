import requests
import pandas as pd
from pandas.io.json import json_normalize
import json
data=[]

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',}
with requests.session() as req:
    symbol_name = "NIFTY"
    req.get('https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY',headers = headers)

    api_req=req.get('https://www.nseindia.com/api/quote-derivative?symbol=NIFTY',headers = headers).json()
    for item in api_req['stocks']:
        if (item['metadata']['instrumentType'] == 'Stock Futures') or (item['metadata']['instrumentType'] == 'Index Futures'):
            data.append([
                item['metadata']['identifier'][:6],
                symbol_name,
                item['metadata']['expiryDate'],
                "0","XX",
                item['metadata']['openPrice'],
                item['metadata']['highPrice'],
                item['metadata']['lowPrice'],
                item['metadata']['lastPrice'],
                item['metadata']['lastPrice'],
                item['marketDeptOrderBook']['tradeInfo']['tradedVolume'],
                item['marketDeptOrderBook']['tradeInfo']['value'],
                item['marketDeptOrderBook']['tradeInfo']['openInterest'],
                item['marketDeptOrderBook']['tradeInfo']['changeinOpenInterest'],
                "TIMESTAMP"
            ])


bhavcopy_col_names = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                      'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH', 'OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']
df = pd.DataFrame(data,columns=bhavcopy_col_names)
print(df)
df.to_csv('info.csv',index = False)