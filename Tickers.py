# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 12:07:17 2019

@author: Prakash
"""
import Constants as c
import pandas as pd

_excel_tickers = "./Excels/Tickers.xlsx"


class Tickers:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Tickers, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.tickers = pd.read_excel(_excel_tickers, sheet_name='NSE_INDEX_N_STOCKS')
        self.index_stock_mapping = pd.read_excel(_excel_tickers, sheet_name='INDEX_STOCK_MAPPING')

    def get_tickers(self):
        return self.tickers

    # def get_tickers_as_list(self, indexOnly=False):
    #
    #     if indexOnly:
    #         return self.tickers.to_dict('records')[:3]
    #
    #     return self.tickers.to_dict('records')

    def get_all_nse_indexes(self):
        indexes = self.tickers.loc[(self.tickers["TYPE"] == 'INDEX'), ["INTERNAL_NAME"]]
        return indexes

    def check_index_or_stock(self, internal_name=None):
        indexes = self.tickers.loc[(self.tickers["INTERNAL_NAME"] == internal_name)]

        return indexes.iloc[0]["TYPE"]

    def get_index_components(self, internal_name=None):
        exchange_code = self.get_exchange_code(internal_name=internal_name)
        index_components = self.index_stock_mapping.loc[(self.index_stock_mapping["INDEX_CODE"] == exchange_code), ['STOCK_CODE']]
        return index_components

    def get_exchange_code(self, internal_name=None):
        return self.tickers.loc[(self.tickers["INTERNAL_NAME"] == internal_name)].iloc[0]["EXCHANGE_CODE"]

    def get_internal_name(self, exchange_code=None):
        return self.tickers.loc[(self.tickers["EXCHANGE_CODE"] == exchange_code)].iloc[0]["INTERNAL_NAME"]

    def get_dl_code(self, internal_name=None):
        return self.tickers.loc[(self.tickers["INTERNAL_NAME"] == internal_name)].iloc[0]["EXCHANGE_CM_DL_CODE"]


# tickersobj = Tickers()
#
# print(type(tickersobj.get_tickers()))
#
# print(type(tickersobj.get_tickers_as_list()))
#
# # print(type(tickers_multi_symbol))
#
# for tickerSymbolArray in tickersobj.get_tickers_as_list():
#     symbol_name = tickerSymbolArray["SYMBOL"]
#     nse_name = tickerSymbolArray['NSE']
#     print (symbol_name + " " + nse_name)
