import requests
import pandas as pd
from pandas.io.json import json_normalize
import json
data=[]

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',}
with requests.session() as req:
    req.get('https://www.nseindia.com/get-quotes/derivatives?symbol=WIPRO',headers = headers)

    api_req=req.get('https://www.nseindia.com/api/quote-derivative?symbol=WIPRO',headers = headers).json()
    for item in api_req['stocks']:
        if item['metadata']['instrumentType'] == 'Stock Futures':
            data.append([
                item['metadata']['instrumentType'],
                item['metadata']['openPrice'],
                item['metadata']['identifier'],
                item['metadata']['expiryDate'],
                item['metadata']['openPrice'],
                item['metadata']['highPrice'],
                item['metadata']['lowPrice'],
                item['metadata']['lastPrice'],
                item['marketDeptOrderBook']['tradeInfo']['openInterest'],
                item['marketDeptOrderBook']['tradeInfo']['changeinOpenInterest']
            ])


cols=['instrumentType','openPrice','identifier','expiryDate','openPrice','highPrice','lowPrice','lastPrice','openInterest','changeinOpenInterest']

df = pd.DataFrame(data, columns=cols)
print(df)
#df.to_csv('info.csv',index = False)