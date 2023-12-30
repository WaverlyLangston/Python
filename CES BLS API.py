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
seriesid = ['SMS09000000000000001','SMS09000000500000001','SMS09000000600000001',
            'SMS09000000700000001','SMS09000000800000001','SMS09000001000000001',
            'SMS09000001500000001','SMS09000002000000001','SMS09000003000000001',
            'SMS09000003100000001','SMS09000003200000001','SMS09000004000000001',
            'SMS09000004100000001','SMS09000004200000001','SMS09000004300000001',
            'SMS09000005000000001','SMS09000005500000001','SMS09000005552000001',
            'SMS09000005553000001','SMS09000006000000001','SMS09000006054000001',
            'SMS09000006055000001','SMS09000006056000001','SMS09000006500000001',
            'SMS09000006561000001','SMS09000006562000001','SMS09000007000000001',
            'SMS09000007071000001','SMS09000007072000001','SMS09000008000000001',
            'SMS09000009000000001','SMS09000009091000001','SMS09000009092000001',
            'SMS09000009093000001'
            ]

seriesname = ['Total_Nonfarm','Total_Private','Goods_Producing','Service_Providing',
              'Private_Service_Providing','Mining_and_Logging','Mine_Log_Construc',
              'Construction','Manufacturing','Durable_Goods','Non-Durable_Goods',
              'Trade_Transport_Utility','Wholesale_Trade','Retail_Trade',
              'Transport_Warehouse_Utility','Information','Financial_Activities',
              'Finance_and_Insurance','Realestate_Ren_Lease','Prof_Business_Service',
              'Profl_Sci_Tech_Serv','Mgmt_of_Co_Enter','AdminSupport_Waste_Remed',
              'Education_and_Health_Services','Educational_Services','Healthcare_Social_Assist',
              'Leisure_and_Hospitality','Arts_Enter_Rec','Accom_Food_Service',
              'Other_Services','Government','Federal_Government','State_Government','Local_Government'
              ]

seriesdict = dict(zip(seriesid, seriesname))

start_year = '2016'
end_year = '2023'
bls_api = 'e4079648c4934efda43df6f591b08c5e'


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

pivoted_df['Time'] = pivoted_df['period'].str[1:] + '/' + pivoted_df['year']

tableau_data = pivoted_df[['Time', 'SMS09000000000000001','SMS09000000500000001','SMS09000001500000001',
                           'SMS09000003000000001','SMS09000004000000001','SMS09000005000000001',
                           'SMS09000005500000001','SMS09000006000000001','SMS09000006500000001',
                           'SMS09000007000000001','SMS09000008000000001','SMS09000009000000001'
                           ]]

tableau_dict = {'SMS09000000000000001' : 'Total Nonfarm',
                'SMS09000000500000001' : 'Total Private',
                'SMS09000001500000001' : 'Mining, Logging and Construction',
                'SMS09000003000000001' : 'Manufacturing',
                'SMS09000004000000001' : 'Trade, Transportation, and Utilities',
                'SMS09000005000000001' : 'Information',
                'SMS09000005500000001' : 'Financial Activities',
                'SMS09000006000000001' : 'Professional and Business Services',
                'SMS09000006500000001' : 'Private Education and Health Services',
                'SMS09000007000000001' : 'Leisure and Hospitality',
                'SMS09000008000000001' : 'Other Services',
                'SMS09000009000000001' : 'Government'
                }

tableau_data.rename(columns=tableau_dict, inplace=True)


tableau_data.to_csv('CES.csv')

# #sheets_api_key = '57df72284b2e6a72162ca8a4f78a8a161f22243a'
# #Reads json file approving access to google drive, edit file location for sharing
# gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

# #Updates spreadsheeet with csv that was exported.  Easier to update like this and we want the csv anyway.
# spreadsheet1 = gc.open('LAUS')
# with open('LAUS.csv', 'r', encoding = 'UTF-8') as file_obj:
#     content = file_obj.read()
#     gc.import_csv(spreadsheet1.id, data=content)

# #Share spreadsheet so that the main email has access, not just editing client/API
# spreadsheet1.share('advancecteconomicdashboard@gmail.com', perm_type = 'user', role = 'writer')



##THIS IS A METHOD OF UPDATING THE A TAB IN THE SPREADSHEET CALLED SHEET1##
##TABLEAU REJECTS DATA CONNECTIONS EASILY AND DOES NOT LIKE TO REPLACE##
##IF A SHEET NAME OTHER THAN LAUS BECOMES NECESSARY THIS ALLOWS FOR THAT##

#sheets_api_key = '57df72284b2e6a72162ca8a4f78a8a161f22243a'
#Reads json file approving access to google drive, edit file location for sharing
gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

spreadsheet1 = gc.open('Jobs Data')
spreadsheet1.values_update('Sheet1',
                            params = {'valueInputOption' : 'USER_ENTERED'},
                            body = {'values' : list(csv.reader(open('CES.csv')))}
                            )

#Share spreadsheet so that the main email has access, not just editing client/API
spreadsheet1.share('advancecteconomicdashboard@gmail.com', perm_type = 'user', role = 'writer')




