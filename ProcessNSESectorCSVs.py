# Create a mapping of NSE INDEX NAME and CODE in excel
# Folder location read all csv files
# Process Names of the file
# read file into df and get the stock codes
# add in a dict
from io import StringIO
from pathlib import Path
from tabulate import tabulate
import pandas as pd
import pickle
import json
import csv
import OHLCDownloadUtility as ohlcdownloader
import Helper as h

ip_folder_location = "D:/Users/Prakash/Downloads/Delete Later/NSE Sectoral Indices/"
index_name_code_mapping = []
index_code_stock_code_mapping_dict = {}
stock_code_set = set()
sector_index_list = []

def read_index_name_code_mapping():
    global index_name_code_mapping
    global index_code_stock_code_mapping_dict
    index_name_code_mapping = pd.read_csv('./Excels/NSE_Index_Name_Code_Mapping.csv', dtype=str)
    # index_name_code_mapping_dict = index_name_code_mapping.to_dict()
    print(index_name_code_mapping.head())


def read_index_components_details():
    global index_code_stock_code_mapping_dict
    global stock_code_set
    global sector_index_list
    for p in Path(ip_folder_location).glob('*.csv'):
        # print(f"{p.name}:\n{p.read_text()}\n")
        # print(f"{p.name}")
        # print(f"{p.name[9:p.name.__len__()-8].strip('_').upper()}\n{p.read_text()}\n")
        sector_name = p.name[9:p.name.__len__() - 8].strip('_').upper()

        sector_code_name = index_name_code_mapping.loc[index_name_code_mapping['Name'] == sector_name, 'Code'].values.item()
        index_components = pd.read_table(StringIO(p.read_text()), sep=",")
        index_components_list = index_components['Symbol'].tolist()
        sector_index_list.append(sector_code_name)
        index_code_stock_code_mapping_dict[sector_code_name] = index_components_list
        stock_code_set.update(index_components_list)
        # stock_code_set.update({sector_code_name})

    t_nifty_dict = {'NIFTY': sector_index_list}
    index_code_stock_code_mapping_dict = {**t_nifty_dict, **index_code_stock_code_mapping_dict}
    with open('./Excels/index_code_stock_code_mapping_dict.csv', "w") as fp:
        json.dump(index_code_stock_code_mapping_dict, fp, indent=4)  # encode dict into JSON

    # pd.DataFrame(index_code_stock_code_mapping_dict).to_csv('./Excels/index_code_stock_code_mapping_dict.csv', index=False)
    # with open('./Excels/index_code_stock_code_mapping_dict.csv', 'wb') as fp:
    #     pickle.dump(index_code_stock_code_mapping_dict, fp)
    #     print('index_code_stock_code_mapping_dict saved successfully to file')

def download_key_value_pair_ohlc_data():
    global stock_code_set
    for sym in stock_code_set:
        dl = ohlcdownloader.OHLCDownloadUtility(symbol=h.get_yahoo_symbol(symbol=sym))
        df = dl.download_from_yahoo()
        # df_2 = pd.DataFrame()
        if df.shape[0] == 54:
            df_one = df.iloc[0]
            df_one["Open"] = df.iloc[1]["Open"]
            df_one["High"] = max(df.iloc[0]["High"], df.iloc[1]["High"])
            df_one["Low"] = min(df.iloc[0]["Low"], df.iloc[1]["Low"])
            df_one["Close"] = df.iloc[0]["Close"]
            df_one["Adj Close"] = df.iloc[0]["Adj Close"]
            df.drop(index=df.index[0], inplace=True)
            df.iloc[0] = df_one
            # df.drop(df.index[0], inplace=True)
            # print (df_one.head())
        print (sym)
        print(df.head())

def universal_set_of_symbols():
    # NIFTY_CONSR_DURBL, NIFTY_HEALTHCARE, NIFTY_MIDSML_HLTH, NIFTY_OIL_AND_GAS, CNXPHARMA

    global stock_code_set
    stock_code_set = sorted(stock_code_set)
    # with open('./Excels/stock_code_set.csv', "w") as fp:
    #     json.dump(stock_code_set, fp)  # encode dict into JSON
    with open('./Excels/stock_code_set.csv', 'w', newline='') as fp:
        writer = csv.writer(fp)
        for row in stock_code_set:
            writer.writerow([row])

def main():

    print("Hello World!")
    read_index_name_code_mapping()
    read_index_components_details()
    universal_set_of_symbols()
    df = pd.DataFrame(stock_code_set)
    print(df.T.T[0])
    # h.get_index_data_from_nse()
    # print(list(dict.fromkeys(stock_code_set)))
    # print(tabulate(index_code_stock_code_mapping_dict))
    # download_key_value_pair_ohlc_data()
    print("done")


if __name__ == "__main__":
    main()
