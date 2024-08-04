import datetime


current_date = datetime.datetime.today().strftime(' %d-%b-%Y %H-%M-%S')

error_file_name = "./error.txt"
error_file_name_stock_ohlc = './StockData_ERROR.txt'

excel_tickers = "./../StockData/Tickers/Tickers.xlsx"

tradingview_chart_link = "https://www.tradingview.com/chart/?symbol=NSE:{0}&interval=1D"

screener_link = "https://www.screener.in/company/{0}/consolidated/#top"

money_control_tag_link = "https://www.moneycontrol.com/news/tags/{0}.html"

nse_lot_size_link = "https://www1.nseindia.com/content/fo/fo_mktlots.csv"
