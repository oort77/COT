# COT analysis -- streamlit v2a

### NEEDS REWRITE: put DDATE to h5 file refs instead of to environment variable

# Import libraries
from datetime import datetime
# import pytz
import pandas as pd
import numpy as np
import h5py
import zipfile, urllib.request, shutil, requests
import os
import altair
import streamlit as st

# Streamlit page configuration
st.set_page_config(page_title='COT', page_icon='./icon.ico',
                   layout='centered', initial_sidebar_state='auto')

pd.set_option('mode.chained_assignment', None)

# Init
path = "/home/gm/notebooks/COT/"
curr_year = datetime.today().year
start_year = 2010
pos_format = 'size'  # vs. '%'
pos_show = 'net'  # vs. 'long/short'
flag_download_all = False  # Set to True to download all data again
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
}

# H5 init
data_store = pd.HDFStore('data.h5')   # <-- 1
ddate = data_store.get_storer('curr_data').attrs['ddate']   # <-- 1
# Init data
#data = pd.DataFrame() # NEED FOR THE FIRST DOWNLOAD BEFORE data.h5 exists <-- 0
data = data_store['curr_data']  # <-- 1
####### data_store.close()

# Zipped COT report import function
def get_COT(url, file_name):
#     with urllib.request.urlopen(url) as response, open(file_name,
#                                                        'wb') as out_file:
    with requests.get(url, stream=True, headers=headers) as response, open(file_name,
                                                       'wb') as out_file:
#        st.write(response)
        shutil.copyfileobj(response.raw, out_file)
    with zipfile.ZipFile(file_name) as zf:
        zf.extractall()

# Import COT reports by year and convert to xls
def COT_handling(year):
    year = str(year)
    filename = year + '.zip'
#     get_COT('https://www.cftc.gov/files/dea/history/fut_fin_xls_' + filename,
#             year + '.zip')
    url = 'https://www.cftc.gov/files/dea/history/fut_fin_xls_' + year +'.zip'
#    st.write(url)
    get_COT(url, filename)
#             year + '.zip')
    # Rename
    new_filename = year + '.xls'
    os.rename(path + 'FinFutYY.xls', path + new_filename)


def append_xls_to_dataframe(df, xls_filename):
    if df.empty:
        df = pd.read_excel(xls_filename)
    else:
        df = df.append(pd.read_excel(xls_filename))
    return (df)

#====================REWRITE==========================================
def write_h5(df, is_current=True):
    
    for i in range(start_year, curr_year):
        df = append_xls_to_dataframe(df, str(i) + ".xls")

    if is_current == False:  # Writing previous years data only
        data_store['prev_years_data'] = df
        data_store.get_storer(
            'prev_years_data').attrs["ddate"] = datetime.today().date()
        st.error("Wrote 'prev_years_data' dataframe to h5 data store")
    else:
        # data contains all info from all years
        df = append_xls_to_dataframe(df, str(curr_year) + ".xls")
        data_store['curr_data'] = df
        #        ********** hdf5
        data_store.get_storer(
            'curr_data').attrs['ddate'] = datetime.today().date()
        st.error("Wrote 'curr_data' dataframe to h5 data store")


# Download ALL COT files -- if needed

# Set flag_download_all to True above) and change init to:
# data = pd.DataFrame()
if flag_download_all == True:
    for i in range(start_year, curr_year):
        COT_handling(i)

    write_h5(data, is_current=False)

    COT_handling(curr_year)
    write_h5(data, is_current=True)

# Year change

if curr_year > ddate.year:
    # Download prev year report and put to data_store['prev_years_data']
    COT_handling(curr_year - 1)
    append_xls_to_dataframe(data, str(curr_year - 1) + ".xls")
    write_h5(data, is_current=False)

# Check if there is a new COT report (sure by Saturday) and download the latest

if ddate != str(datetime.today().date()):
    if datetime.today().weekday() == 3 or (
            datetime.today().date() - ddate).days >= 7:
        # Updata with new COT report
        COT_handling(curr_year)
        data = append_xls_to_dataframe(data_store['prev_years_data'],
                                       str(curr_year) + ".xls")
        write_h5(data, is_current=True)

start_year = int(st.sidebar.text_input('Show data from year:',
                                       value='2010'))  #, max_chars=4))
if start_year < 2010:
    st.error("Data is available starting from year 2010")

data.set_index('Report_Date_as_MM_DD_YYYY', inplace=True)
data.index = pd.to_datetime(data.index)
data.index.rename('Date', inplace=True)
data['Year'] = data.index.year

# Get last report date
last_report_date = data[data['Year'] == curr_year].index.max().strftime(
    "%Y-%m-%d")

# Select data to show by start_year
data_y = data[data['Year'] >= start_year]

# Choose instrument
instruments = list(pd.unique(data_y['Market_and_Exchange_Names']))
instruments.sort()
# Set default instrument ro E-mini S&P500
ES_index = instruments.index(
    'E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE')
instr = st.sidebar.selectbox('Choose an instrument:',
                             instruments,
                             index=ES_index)

# Define 'instr' dataframe
df = data_y[data_y['Market_and_Exchange_Names'] == instr]

# Select traders categories
ct = [False] * 5
cat_list = [
    'Asset managers', 'Leveraged funds', 'Other reportables',
    'Non-reportables', 'Dealers'
]
# Show checkboxes with traders categories
for n in range(len(cat_list)):
    ct[n] = st.sidebar.checkbox(cat_list[n], value=True)

# Calculate net positions
avg_period = 1
df['Asset managers'] = (
    df['Asset_Mgr_Positions_Long_All'] -
    df['Asset_Mgr_Positions_Short_All']).rolling(avg_period).mean()
df['Leveraged funds'] = (
    df['Lev_Money_Positions_Long_All'] -
    df['Lev_Money_Positions_Short_All']).rolling(avg_period).mean()
df['Non-reportables'] = (
    df['NonRept_Positions_Long_All'] -
    df['NonRept_Positions_Short_All']).rolling(avg_period).mean()
df['Dealers'] = (df['Dealer_Positions_Long_All'] -
                 df['Dealer_Positions_Short_All']).rolling(avg_period).mean()
df['Other reportables'] = (
    df['Other_Rept_Positions_Long_All'] -
    df['Other_Rept_Positions_Short_All']).rolling(avg_period).mean()

# Categories to plot
categories = []
for i in range(len(ct)):
    if ct[i]:
        categories.append(cat_list[i])
df0 = df[categories]

# Plot data
st.write(f'### COMMITMENT OF TRADERS - NET POSITIONS IN FUTURES')
st.write(f'{instr.split(" -")[0]} FROM {start_year} TO {curr_year}')
# st.line_chart(df0)
st.line_chart(df0, width=640, height=440, use_container_width=False)
st.sidebar.markdown("---")
# st.sidebar.write(f"Last download: {os.getenv('DDATE')}  \n Last report: {last_report_date}")
st.sidebar.write(
    f"Last update: {ddate}  \n Last report: {last_report_date}"
)
data_store.close()