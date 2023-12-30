# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 09:06:52 2023

@author: wlangston
"""
import pandas as pd
import requests
import json
import gspread
import csv


headers = {'Content-type' : 'application/json'}
seriesid = ['LNS11300000', 'LNS12300000', 'LNS14000000', 'LASST090000000000003', 'LASST090000000000004', 'LASST090000000000005', 'LASST090000000000006', 'LASST090000000000007', 'LASST090000000000008']
start_year = '2016'
end_year = '2023'
bls_api = 'e4079648c4934efda43df6f591b08c5e'
series_dict = {'LNS11300000' : 'U.S. Labor Force Participation Rate',
               'LNS12300000' : 'U.S. Employment-Population Ratio',
               'LNS14000000' : 'U.S. Unemployment Rate',
               'LASST090000000000003' : 'CT Unemployment Rate',
               'LASST090000000000004' : 'CT Unemployment',
               'LASST090000000000005' : 'CT Employment',
               'LASST090000000000006' : 'CT Labor Force',
               'LASST090000000000007' : 'CT Employment-Population Ratio',
               'LASST090000000000008' : 'CT Labor Force Participation Rate'
               }

data = json.dumps({'seriesid' : seriesid, 'startyear' : start_year, 'endyear' : end_year, 'registrationkey' : bls_api})
response = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(response.text)

parsed_data = []
for series in json_data['Results']['series']:
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes = ''
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
            parsed_data.append([seriesId,year,period,value,footnotes[0:-1]])

df = pd.DataFrame(parsed_data, columns=['seriesID', 'year', 'period', 'value', 'footnotes'])

pivoted_df = df.pivot(index=['year', 'period'], columns='seriesID', values='value').reset_index()

pivoted_df.rename(columns=series_dict, inplace=True)

pivoted_df['Time'] = pivoted_df['period'].str[1:] + '/' + pivoted_df['year']

pivoted_df = pivoted_df[['Time', 'CT Employment', 'CT Labor Force', 'CT Unemployment', 'CT Unemployment Rate', 
                         'CT Labor Force Participation Rate', 'CT Employment-Population Ratio', 'U.S. Unemployment Rate', 
                         'U.S. Labor Force Participation Rate', 'U.S. Employment-Population Ratio']]

pivoted_df['CT Unemployment Rate'] = pivoted_df['CT Unemployment Rate'].astype(float) / 100
pivoted_df['CT Labor Force Participation Rate'] = pivoted_df['CT Labor Force Participation Rate'].astype(float) / 100
pivoted_df['CT Employment-Population Ratio'] = pivoted_df['CT Employment-Population Ratio'].astype(float) / 100
pivoted_df['U.S. Unemployment Rate'] = pivoted_df['U.S. Unemployment Rate'].astype(float) / 100
pivoted_df['U.S. Labor Force Participation Rate'] = pivoted_df['U.S. Labor Force Participation Rate'].astype(float) / 100
pivoted_df['U.S. Employment-Population Ratio'] = pivoted_df['U.S. Employment-Population Ratio'].astype(float) / 100


pivoted_df.to_csv('LAUS.csv')

#sheets_api_key = '57df72284b2e6a72162ca8a4f78a8a161f22243a'
#Reads json file approving access to google drive, edit file location for sharing
gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

#Updates spreadsheeet with csv that was exported.  Easier to update like this and we want the csv anyway.
spreadsheet1 = gc.open('LAUS')
with open('LAUS.csv', 'r', encoding = 'UTF-8') as file_obj:
    content = file_obj.read()
    gc.import_csv(spreadsheet1.id, data=content)

#Share spreadsheet so that the main email has access, not just editing client/API
spreadsheet1.share('advancecteconomicdashboard@gmail.com', perm_type = 'user', role = 'writer')



# ##THIS IS A METHOD OF UPDATING THE A TAB IN THE SPREADSHEET CALLED SHEET1##
# ##TABLEAU REJECTS DATA CONNECTIONS EASILY AND DOES NOT LIKE TO REPLACE##
# ##IF A SHEET NAME OTHER THAN LAUS BECOMES NECESSARY THIS ALLOWS FOR THAT##

# #sheets_api_key = '57df72284b2e6a72162ca8a4f78a8a161f22243a'
# #Reads json file approving access to google drive, edit file location for sharing
# gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

# spreadsheet1 = gc.open('LAUS')
# spreadsheet1.values_update('Sheet1',
#                             params = {'valueInputOption' : 'USER_ENTERED'},
#                             body = {'values' : list(csv.reader(open('LAUS.csv')))}
#                             )

# #Share spreadsheet so that the main email has access, not just editing client/API
# spreadsheet1.share('advancecteconomicdashboard@gmail.com', perm_type = 'user', role = 'writer')




