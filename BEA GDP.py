# -*- coding: utf-8 -*-
'''
Created on Fri Jul 28 09:41:49 2023

@author: wlangston
'''
import requests
import pandas as pd
#allows access to google sheets
import gspread
import os
cwd = os.getcwd()

base_url = 'https://apps.bea.gov/api/data/'
bea_api_key = '9D4C60D7-52EB-46B4-A814-5EDA4E638294'

# START OF PEER STATE GDP SECTION

region_list = ['United States', 'Connecticut', 'Massachusetts', 'New jersey', 'New York', 'Rhode Island']

#Parameter dictionary for accessing BEA API and JSON data
params_peer = {
    'UserID': bea_api_key,
    'method': 'GetData',
    'datasetname': 'Regional',
    'TableName': 'SQGDP9',
    'LineCode': '1',
    'Year': 'LAST10',
    'GeoFips': 'STATE',
    'ResultFormat': 'json'
}

#Import data from BEA API Using parameters above
#Check for good response
try:
    response_peer = requests.get(base_url,params=params_peer, timeout = 10)
    error = response_peer.json()['BEAAPI']['Results']['Error']['APIErrorCode']
    print(f'Peer State Data, API Error Code: {error}')
except requests.exceptions.Timeout:
    print('Peer State Data, The request timed out')
except KeyError:
    pass

if response_peer.status_code != 200:
    print(f'Peer State Data, API failed, status code: {response_peer.status_code}')

#Create dataframe of desired data points
data_peer = response_peer.json()['BEAAPI']['Results']['Data']
peer_gdp = pd.DataFrame(data_peer)
peer_gdp = peer_gdp[peer_gdp['GeoName'].isin(region_list)]

#Convert to more useful time values
peer_gdp['Time'] = pd.to_datetime(peer_gdp['TimePeriod'])

#Reshape dataframe for tableau and export csv
peer_gdp = peer_gdp.pivot(index = 'Time', columns = 'GeoName', values = 'DataValue')

# #Calculate 1yr and 5yr %change for index charts
# for col in peer_gdp:
#     peer_gdp[col] = peer_gdp[col].str.replace(',','').astype(float)
#     peer_gdp[f'{col}_1yr'] = peer_gdp[col].pct_change(periods = 4)  
#     peer_gdp[f'{col}_5yr'] = peer_gdp[col].pct_change(periods = 20)


#Export
peer_gdp.to_csv('peer_gdp.csv')

#sheets_api_key = '57df72284b2e6a72162ca8a4f78a8a161f22243a'
#Reads json file approving access to google drive, edit file location for sharing
gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

#Updates spreadsheeet with state_gdp.csv that was exported.  Easier to update like this and we want the csv anyway.
spreadsheet1 = gc.open('Peer State GDP')
with open('peer_gdp.csv', 'r', encoding = 'UTF-8') as file_obj:
    content = file_obj.read()
    gc.import_csv(spreadsheet1.id, data=content)

#Share spreadsheet so that the main email has access, not just editing client/API
spreadsheet1.share('advancecteconomicdashboard@gmail.com', perm_type = 'user', role = 'writer')

# END OF PEER STATE GDP SECTION



# START OF CT GDP BY INDUSTRY SECTION

params_ct = {
    'UserID': bea_api_key,
    'method': 'GetData',
    'datasetname': 'Regional',
    'TableName': 'SQGDP9',
    'LineCode': 'All',
    'Year': 'LAST10',
    'GeoFips': 'CT',
    'ResultFormat': 'json'
}
#Import data from BEA API Using parameters above
#Check for good response
try:
    response_ct = requests.get(base_url,params=params_ct, timeout = 10)
    error = response_ct.json()['BEAAPI']['Results']['Error']['APIErrorCode']
    print(f'CT Industry Data, API Error Code: {error}')
except requests.exceptions.Timeout:
    print('CT Industry Data, The request timed out')
except KeyError:
    pass

if response_ct.status_code != 200:
    print(f'CT Industry Data, API failed, status code: {response_ct.status_code}')

#Create dataframe of desired data points
data_ct = response_ct.json()['BEAAPI']['Results']['Data']
ct_industry = pd.DataFrame(data_ct)

#Convert to more useful time values
ct_industry['Time'] = pd.to_datetime(ct_industry['TimePeriod'])

#Reshape dataframe for tableau and export csv
ct_industry = ct_industry.pivot(index = 'Time', columns = 'Description', values = 'DataValue')

#Export
ct_industry.to_csv('ct_industry_gdp.csv')

#sheets_api_key = '57df72284b2e6a72162ca8a4f78a8a161f22243a'
#Reads json file approving access to google drive, edit file location for sharing
gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

#Updates spreadsheeet with state_gdp.csv that was exported.  Easier to update like this and we want the csv anyway.
spreadsheet2 = gc.open('CT Industry GDP')
with open('ct_industry_gdp.csv', 'r', encoding = 'UTF-8') as file_obj:
    content = file_obj.read()
    gc.import_csv(spreadsheet2.id, data=content)

#Share spreadsheet so that the main email has access, not just editing client/API
spreadsheet2.share('advancecteconomicdashboard@gmail.com', perm_type = 'user', role = 'writer')



# END OF CT GDP BY INDUSTRY SECTION



#START OF ANNUAL CT GDP BY INDUSTRY SECTION
params_annual = {
    'UserID': bea_api_key,
    'method': 'GetData',
    'datasetname': 'Regional',
    'TableName': 'SAGDP9N',
    'LineCode': 'All',
    'GeoFips': 'CT',
    'ResultFormat': 'json'
}
#Import data from BEA API Using parameters above
#Check for good response
try:
    response_annual = requests.get(base_url,params=params_annual, timeout = 10)
    error = response_annual.json()['BEAAPI']['Results']['Error']['APIErrorCode']
    print(f'CT Annual Industry Data, API Error Code: {error}')
except requests.exceptions.Timeout:
    print('CT Annual Industry Data, The request timed out')
except KeyError:
    pass

if response_annual.status_code != 200:
    print(f'CT Annual Industry Data, API failed, status code: {response_ct.status_code}')

#Create dataframe of desired data points
data_annual = response_annual.json()['BEAAPI']['Results']['Data']
ct_annual = pd.DataFrame(data_annual)

#Convert to more useful time values
ct_annual['Time'] = pd.to_datetime(ct_annual['TimePeriod'])
recent_annual = ct_annual['Time'].max()

#Keep only most recent year
ct_annual = ct_annual[ct_annual['Time'] == recent_annual].drop(columns = ['Code', 'GeoFips', 'GeoName','CL_UNIT', 'UNIT_MULT', 'NoteRef'])
ct_annual.rename(columns = {'DataValue' : 'GDP'}, inplace = True)

#Calculate industry percentages of total gdp
#Convert values to number type
ct_annual['GDP'] = ct_annual['GDP'].str.replace(',','')
ct_annual['GDP'] = pd.to_numeric(ct_annual['GDP'].str.replace('(NA)',''), errors='coerce')

total_gdp = float(ct_annual[ct_annual['Description'] == 'All industry total ']['GDP'].values[0])

ct_annual['percent_gdp'] = ct_annual['GDP'].div(total_gdp)

#Reshape dataframe for tableau and export csv, may not be necessary
#ct_annual = ct_annual.pivot(index = 'Time', columns = 'Description', values = 'GDP')

#Export
ct_annual.to_csv('ct_annual_gdp.csv')

# sheets_api_key = '57df72284b2e6a72162ca8a4f78a8a161f22243a'
#Reads json file approving access to google drive, edit file location for sharing
gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

#Updates spreadsheeet with state_gdp.csv that was exported.  Easier to update like this and we want the csv anyway.
spreadsheet3 = gc.open('CT Annual GDP')
with open('ct_annual_gdp.csv', 'r', encoding = 'UTF-8') as file_obj:
    content = file_obj.read()
    gc.import_csv(spreadsheet3.id, data=content)

#Share spreadsheet so that the main email has access, not just editing client/API
spreadsheet3.share('advancecteconomicdashboard@gmail.com', perm_type = 'user', role = 'writer')

#END OF ANNUAL CT GDP BY INDUSTRY SECTION

