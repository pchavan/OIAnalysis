# import MyLogger as ml
import datetime

import pandas as pd

import OHLCDownloader as dl
import Tickers as t
from openpyxl import load_workbook
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.styles import colors
import Constants as c

_rsc_cache = {}
_ohlc_cache = {}


def test_tickers():
    tickers = t.Tickers()
    tickers2 = t.Tickers()
    print(tickers is tickers2)
    print(tickers.get_index_components(internal_name="IT").values)
    # ml.test.debug(tickers.get_index_components(internal_name="IT").values)


def _check_rsc_cache(symbol=None):
    global _rsc_cache
    if None == symbol:
        return None

    for key, values in _rsc_cache.items():
        if key == symbol:
            return values
    return None


def _get_rsc_for_symbol(symbol_name=None, symbol_df=None, compare_df=None):
    global _rsc_cache
    pd.options.display.float_format = '{:.2f}'.format
    # if None == symbol_name or None == symbol_df or None == compare_df:
    #     return None
    # rsc = _check_rsc_cache(symbol=symbol_name)
    # if rsc is not None:
    #     return rsc
    day_no = datetime.datetime.today().weekday()
    s_i = 1
    len = 13
    multiplier = 100

    if day_no > 4: s_i = 0  # On weekdays, an extra row for unfinished week is also returned

    rsc0 = (((symbol_df['CLOSE'][s_i] / symbol_df['CLOSE'][s_i + len]) / (
            compare_df['CLOSE'][s_i] / compare_df['CLOSE'][s_i + len])) - 1) * multiplier
    rsc1 = (((symbol_df['CLOSE'][s_i + 1] / symbol_df['CLOSE'][s_i + len + 1]) / (
            compare_df['CLOSE'][s_i + 1] / compare_df['CLOSE'][s_i + len + 1])) - 1) * multiplier
    rsc2 = (((symbol_df['CLOSE'][s_i + 2] / symbol_df['CLOSE'][s_i + len + 2]) / (
            compare_df['CLOSE'][s_i + 2] / compare_df['CLOSE'][s_i + len + 2])) - 1) * multiplier

    rsc = [rsc0, rsc1, rsc2]
    # _rsc_cache[symbol_name] = rsc
    # ml.test.debug(rsc)
    return rsc


def add_tv_links(df, symbol):
    # '=HYPERLINK("#{}!A1", "{}")'.format(sheet, sheet)
    df.insert(6, "Tradingview",
              '=HYPERLINK("{}", "{}")'.format(c.tradingview_chart_link.format(symbol),
                                              symbol))
    return df

def index_dl_endtoend():
    tickers = t.Tickers()
    final_df = pd.DataFrame(columns=['Name', 'Type', 'Index Name', 'RSC1', 'RSC2', 'RSC3', 'Tradingview'])
    # with open("./csvoutput.csv", "wb") as csv_file:
    #     writer = csv.writer(csv_file, delimiter=',')

    df_indexes = tickers.get_all_nse_indexes()
    df_nifty = []
    s_i = 1
    len = 13
    for index in df_indexes['INTERNAL_NAME']:
        # print('\n==>', index)
        # ml.test.info('\n==>', index)
        try:
            # df_index_data = get_index_data_from_nse(index)
            # df_index_data = get_index_data_from_nse(index)
            df_index_data = dl.download_ohlc_from_nse(index)
            # df_index_data = rename_index_df_columns(df_index_data)
            # df_index_data['DATE']=pd.to_datetime(df_index_data['DATE'],format="%d-%b-%Y")
            # df_index_data.set_index('DATE', inplace=True)
            # df_index_data = df_index_data.resample('W-Fri').agg({'OPEN': 'first','HIGH': 'max','LOW': 'min','CLOSE': 'last'})
            # # IF not end of the week? How to check this.
            # df_index_data = df_index_data.sort_index(ascending=False)
            if index == 'NIFTY':
                df_nifty = df_index_data
                # writer.writerow(index)
            # print(df_index_data.head())
            if index != 'NIFTY':
                rsc = _get_rsc_for_symbol(symbol_name=index, symbol_df=df_index_data, compare_df=df_nifty)
                # if (rsc[0] < rsc [1]) or (rsc[1] < rsc[2]): continue
                my_formatted_list = ['%.2f' % elem for elem in rsc]
                # print(my_formatted_list)
                # ml.test.info(my_formatted_list)
                print('\n', f"{index:<20}", '   \t', my_formatted_list)
                # row = '\n', f"{index:<20}", '   \t', my_formatted_list
                new_row = pd.DataFrame([{'Name': index, "Type": "INDEX", 'Index Name': index, "RSC1": rsc[0],
                                         "RSC2": rsc[1], "RSC3": rsc[2]}])
                new_row = add_tv_links(new_row, index)

                final_df = pd.concat([final_df, new_row])
                stocks_in_index = tickers.get_index_components(internal_name=index)

                for stock in stocks_in_index['STOCK_CODE']:
                    # print(stock)
                    df_stock_data = dl.download_ohlc_from_nse(stock)
                    rsc_stock = _get_rsc_for_symbol(symbol_name=stock, symbol_df=df_stock_data, compare_df=df_index_data)
                    # if (rsc_stock[0] < rsc_stock[1]) or (rsc_stock[1] < rsc_stock[2]): continue

                    my_formatted_list = ['%.2f' % elem for elem in rsc_stock]
                    # my_formatted_list = str(my_formatted_list)[1:-1].replace("'","")
                    # print('\n','{:>20}'.format(stock), my_formatted_list)
                    # print('\n\t|->',f"{stock:<20}", f"{my_formatted_list:<20}")
                    print('\n\t|->',f"{stock:<20}", my_formatted_list)
                    new_row = pd.DataFrame([{'Name': stock, "Type": "STOCK", 'Index Name': index, "RSC1": rsc_stock[0], "RSC2": rsc_stock[1], "RSC3": rsc_stock[2]}])
                    new_row = add_tv_links(new_row, stock)
                    final_df = pd.concat([final_df, new_row])

                    # row = '\n\t|->',f"{stock:<20}", my_formatted_list
                    # writer.writerow(row)
                    # print(*my_formatted_list, sep=", ")
                    # print({stock:<15}{my_formatted_list:>20}")
                    # print(tabulate(my_formatted_list, tablefmt='fancygrid'))
                    # ml.test.info(my_formatted_list)

            print('\n==========================================')
            # ml.test.info('\n==========================================')
        except Exception as e:
            print(e)

    final_df.to_csv("./output.csv")
    final_df.to_excel("./output.xlsx", index=False)


def format_output_excel():
    # Load the Excel workbook
    workbook = load_workbook('./output.xlsx')

    # Select the desired sheet by name or index
    sheet = workbook['Sheet1']  # Replace 'Sheet1' with the actual sheet name

    rule = ColorScaleRule(start_type='num', start_value=0, start_color='FFFFFF',
                          mid_type='num', mid_value=1, mid_color='FF000000',
                          end_type='num', end_value=2, end_color='FF808080')
    rule = ColorScaleRule(start_type='min', start_color=colors.COLOR_INDEX[2], mid_type='num', mid_value=0,
                          mid_color=colors.WHITE, end_type='max', end_color=colors.COLOR_INDEX[3])
    # Starting at A2 skips the header row
    sheet.conditional_formatting.add("D2:F9999", rule)

    # Save the modified workbook with conditional formatting
    workbook.save('output_formatted.xlsx')

index_dl_endtoend()

format_output_excel()
