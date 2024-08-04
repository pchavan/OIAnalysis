#! python
#USAGE csvUpdate.py fo07MAY2024bhav.csv 29-May-2024 30-May-2024
import sys
import pandas as pd  
import requests
import zipfile
import os
# https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_20240701_F_0000.csv.zip
# nse_url_fno_bhavcopy = "https://nsearchives.nseindia.com/content/historical/DERIVATIVES"
nse_url_fno_bhavcopy = "https://nsearchives.nseindia.com/content/"
nse_url_fno_prefix = "fo/"
nse_url_fno_file_prefix = "BhavCopy_NSE_FO_0_0_0_"
nse_url_fno_file_suffix = "_F_0000.csv.zip"
output_dir = "NSEOI"
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
    'accept-language': 'en,gu;q=0.9,hi;q=0.8',
    'accept-encoding': 'gzip, deflate, br'}

url_nse = 'https://www.nseindia.com'
url_nse_derivatives = 'https://www.nseindia.com/all-reports-derivatives'

cwd = os.getcwd()
sess = requests.Session()
cookies = dict()

def set_cookie():
    request = sess.get(url_nse, headers=headers, timeout=5)
    cookies = dict(request.cookies)

datePart = sys.argv[1].upper() #22MAY2024 -> 20240522
# fileToDownload = nse_url_fno_prefix+datePart+nse_url_fno_suffix
fileToDownload = nse_url_fno_file_prefix+datePart+nse_url_fno_file_suffix

# month = datePart[2:5]
# year = datePart[5:]
# url = nse_url_fno_bhavcopy+"/"+year+"/"+month+"/"+fileToDownload
url = nse_url_fno_bhavcopy+nse_url_fno_prefix+fileToDownload
# csvFile = sys.argv[2]
# outputFile = fileToDownload[:-4] + "_modified_for_bnf_expiry.csv"
set_cookie()
# replace = sys.argv[3]
outputFile = fileToDownload[:-8] + "_modified_expiry.csv"

response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
with open(cwd + os.sep + output_dir + os.sep + fileToDownload, mode="wb") as file:
    file.write(response.content)
    file.flush()

with zipfile.ZipFile(cwd + os.sep + output_dir  + os.sep +  fileToDownload, 'r') as zip_ref:
    zip_ref.extractall(cwd + os.sep + output_dir)

csvFile = cwd + os.sep + output_dir + os.sep + fileToDownload[:-4]
# making data frame from the csv file  
dataframe = pd.read_csv(csvFile, dtype=str)
    
# nf_expiry_df = pd.DataFrame(dataframe.loc[(dataframe['SYMBOL'] == "NIFTY")&(dataframe["OPTION_TYP"]=="XX")]["EXPIRY_DT"]).reset_index(drop=True)
# bnf_expiry_df = pd.DataFrame(dataframe.loc[(dataframe['SYMBOL'] == "BANKNIFTY")&(dataframe["OPTION_TYP"]=="XX")]["EXPIRY_DT"]).reset_index(drop=True)
nf_expiry_df = pd.DataFrame(dataframe.loc[(dataframe['TckrSymb'] == "NIFTY")&(dataframe["OptnTp"].isnull())]["XpryDt"]).reset_index(drop=True)
bnf_expiry_df = pd.DataFrame(dataframe.loc[(dataframe['TckrSymb'] == "BANKNIFTY")&(dataframe["OptnTp"].isnull())]["XpryDt"]).reset_index(drop=True)
nf_expiry_df=nf_expiry_df.sort_values(by="XpryDt")
bnf_expiry_df=bnf_expiry_df.sort_values(by="XpryDt")
# for x in range(0,3):
#     dataframe.replace(to_replace=bnf_expiry_df["EXPIRY_DT"][x],
#                       value=nf_expiry_df["EXPIRY_DT"][x], inplace=True)
for x in range(0,3): #NF
    for y in range (0, 3): #BNF
        # Match expiry yyyy-mm from BNF and NF before replacing.
        # Replace values ONLY for XpryDT and FininstrmActlXpryDt columns
        if nf_expiry_df["XpryDt"][x][:7] == bnf_expiry_df["XpryDt"][y][:7]:
            # bnf_expiry_df["XpryDt"][y]=nf_expiry_df["XpryDt"][x]
            # dataframe["XpryDt"] = dataframe["XpryDt"].map({bnf_expiry_df["XpryDt"][y]:nf_expiry_df["XpryDt"][x]})
            dataframe.XpryDt.replace(to_replace=bnf_expiry_df["XpryDt"][y], value=nf_expiry_df["XpryDt"][x], inplace=True)
            dataframe.FininstrmActlXpryDt.replace(to_replace=bnf_expiry_df["XpryDt"][y], value=nf_expiry_df["XpryDt"][x],
                                     inplace=True)
            # dataframe["FininstrmActlXpryDt"] = dataframe["FininstrmActlXpryDt"].map({bnf_expiry_df["FininstrmActlXpryDt"][y]:nf_expiry_df["FininstrmActlXpryDt"][x]})
            # dataframe.replace(to_replace=bnf_expiry_df["XpryDt"][x],
            #                   value=nf_expiry_df["XpryDt"][x], inplace=True)

# writing  the dataframe to another csv file
dataframe.to_csv(cwd + os.sep + output_dir + os.sep + outputFile,
                 index = False)
# print("In csv file " + csvFile + " find " + find + " and replace with " + replace)
