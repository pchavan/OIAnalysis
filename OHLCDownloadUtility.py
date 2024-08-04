import traceback

import pandas as pd
import requests
import yfinance as yf
import Constants as c

error_file = open(c.error_file_name_stock_ohlc, 'w')


class OHLCDownloadUtility():

    def __init__(self, symbol):
        self.data = None
        self.symbol = symbol
        self.yahoo_name = yf.Ticker(symbol)
        return

    def download_from_yahoo(self, interval="1wk", start=None, end=None, period="1y"):
        try:
            yahoo_name = yf.Ticker(self.symbol)
            # region BLOCK ONE get data from yahoo and modify it as needed
            # fetch data from Yahoo Finance
            # data = ticker.history(start="2008-01-01", end="2009-01-01", interval="1mo", back_adjust=True, auto_adjust=True, actions=False)
            data = yahoo_name.history(period=period, start=start, end=end, interval=interval, back_adjust=False, auto_adjust=False,
                                      actions=False)
            self.data = data
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

    def get_info(self):
        try:
            self.info = self.yahoo_name.info
            return self.info
        except Exception as e:
            traceback.print_exc(file=error_file)
            return ""

    def get_info_sector(self):
        try:
            self.get_info()
            return self.info.get("sector")
        except Exception as e:
            traceback.print_exc(file=error_file)
            return ""



