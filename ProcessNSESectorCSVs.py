# Create a mapping of NSE INDEX NAME and CODE in excel
# Folder location read all csv files
# Process Names of the file
# read file into df and get the stock codes
# add in a dict
from io import StringIO
from pathlib import Path
from tabulate import tabulate
import pandas as pd

ip_folder_location = "D:/Users/Prakash/Downloads/Delete Later/NSE Sectoral Indices/"
index_name_code_mapping = []
index_code_stock_code_mapping_dict = {}
stock_code_set = set()

def read_index_name_code_mapping():
    global index_name_code_mapping
    index_name_code_mapping = pd.read_csv('./Excels/NSE_Index_Name_Code_Mapping.csv', dtype=str)
    # index_name_code_mapping_dict = index_name_code_mapping.to_dict()
    print(index_name_code_mapping.head())


def read_index_components_details():
    global index_code_stock_code_mapping_dict
    global stock_code_set

    for p in Path(ip_folder_location).glob('*.csv'):
        # print(f"{p.name}:\n{p.read_text()}\n")
        # print(f"{p.name}")
        # print(f"{p.name[9:p.name.__len__()-8].strip('_').upper()}\n{p.read_text()}\n")
        sector_name = p.name[9:p.name.__len__() - 8].strip('_').upper()
        sector_code_name = index_name_code_mapping.loc[index_name_code_mapping['Name'] == sector_name, 'Code'].values.item()
        index_components = pd.read_table(StringIO(p.read_text()), sep=",")
        index_components_list = index_components['Symbol'].tolist()
        index_code_stock_code_mapping_dict[sector_code_name] = index_components_list
        stock_code_set.update(index_components_list)

def main():
    global stock_code_set
    print("Hello World!")
    read_index_name_code_mapping()
    read_index_components_details()
    stock_code_set = sorted(stock_code_set)
    # print(list(dict.fromkeys(stock_code_set)))
    print(tabulate(index_code_stock_code_mapping_dict))
    print("done")


if __name__ == "__main__":
    main()
