#Import several JOLTS metrics for CT and U.S. from BLS API, clean and prepare for use in tableau dashboard, export, and update google sheet 


import pandas as pd
import requests
import json
import gspread
import csv

headers = {'Content-type' : 'application/json'}
seriesid = ['JTS000000090000000HIL', 'JTS000000090000000HIR', 'JTS000000090000000JOL', 
            'JTS000000090000000JOR', 'JTS000000090000000LDL', 'JTS000000090000000LDR',
            'JTS000000090000000QUL', 'JTS000000090000000QUR', 'JTS000000090000000TSL',
            'JTS000000090000000TSR', 'JTS000000090000000UOR', 'JTS000000000000000HIR',
            'JTS000000000000000JOR', 'JTS000000000000000LDR', 'JTS000000000000000OSR',
            'JTS000000000000000QUR', 'JTS000000000000000TSR', 'JTS000000000000000UOR'
            ]

start_year = '2016'
end_year = '2023'

#BLS API key removed for public upload.  Keys can be acquired for free from website.
bls_api = ''
series_dict = {'JTS000000090000000HIL' : 'CT Hires Level',
               'JTS000000090000000HIR' : 'CT Hires Rate',
               'JTS000000090000000JOL' : 'CT Job Openings Level',
               'JTS000000090000000JOR' : 'CT Job Openings Rate',
               'JTS000000090000000LDL' : 'CT Layoffs and Discharges Level',
               'JTS000000090000000LDR' : 'CT Layoffs and Discharges Rate',
               'JTS000000090000000QUL' : 'CT Quits Level',
               'JTS000000090000000QUR' : 'CT Quits Rate',
               'JTS000000090000000TSL' : 'CT Total Separations Level',
               'JTS000000090000000TSR' : 'CT Total Separations Rate',
               'JTS000000090000000UOR' : 'CT Unemployed per Job Opening Ratio',
               'JTS000000000000000HIR' : 'U.S. Hires Rate',
               'JTS000000000000000JOR' : 'U.S. Job Openings Rate',
               'JTS000000000000000LDR' : 'U.S. Layoffs and Discharges Rate',
               'JTS000000000000000OSR' : 'U.S. Other Sep Rate',
               'JTS000000000000000QUR' : 'U.S. Quits Rate',
               'JTS000000000000000TSR' : 'U.S. Total Separations Rate',
               'JTS000000000000000UOR' : 'U.S. Unemployed per Job Opening Ratio'
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

pivoted_df = pivoted_df[['Time','CT Hires Level','CT Job Openings Level','CT Layoffs and Discharges Level',
                         'CT Quits Level','CT Total Separations Level','CT Unemployed per Job Opening Ratio',
                         'CT Hires Rate','CT Job Openings Rate','CT Layoffs and Discharges Rate',
                         'CT Quits Rate','CT Total Separations Rate','U.S. Unemployed per Job Opening Ratio',
                         'U.S. Hires Rate','U.S. Job Openings Rate','U.S. Layoffs and Discharges Rate',
                         'U.S. Quits Rate','U.S. Total Separations Rate']]

pivoted_df['CT Hires Rate'] = pivoted_df['CT Hires Rate'].astype(float) / 100
pivoted_df['CT Job Openings Rate'] = pivoted_df['CT Job Openings Rate'].astype(float) / 100
pivoted_df['CT Layoffs and Discharges Rate'] = pivoted_df['CT Layoffs and Discharges Rate'].astype(float) / 100
pivoted_df['CT Quits Rate'] = pivoted_df['CT Quits Rate'].astype(float) / 100
pivoted_df['CT Total Separations Rate'] = pivoted_df['CT Total Separations Rate'].astype(float) / 100
pivoted_df['CT Unemployed per Job Opening Ratio'] = pivoted_df['CT Unemployed per Job Opening Ratio'].astype(float) / 100
pivoted_df['U.S. Hires Rate'] = pivoted_df['U.S. Hires Rate'].astype(float) / 100
pivoted_df['U.S. Job Openings Rate'] = pivoted_df['U.S. Job Openings Rate'].astype(float) / 100
pivoted_df['U.S. Layoffs and Discharges Rate'] = pivoted_df['U.S. Layoffs and Discharges Rate'].astype(float) / 100
pivoted_df['U.S. Quits Rate'] = pivoted_df['U.S. Quits Rate'].astype(float) / 100
pivoted_df['U.S. Total Separations Rate'] = pivoted_df['U.S. Total Separations Rate'].astype(float) / 100
pivoted_df['U.S. Unemployed per Job Opening Ratio'] = pivoted_df['U.S. Unemployed per Job Opening Ratio'].astype(float) / 100

pivoted_df['CT Hires Level'] = pivoted_df['CT Hires Level'].astype(float) * 1000
pivoted_df['CT Job Openings Level'] = pivoted_df['CT Job Openings Level'].astype(float) * 1000
pivoted_df['CT Layoffs and Discharges Level'] = pivoted_df['CT Layoffs and Discharges Level'].astype(float) * 1000
pivoted_df['CT Quits Level'] = pivoted_df['CT Quits Level'].astype(float) * 1000
pivoted_df['CT Total Separations Level'] = pivoted_df['CT Total Separations Level'].astype(float) * 1000


pivoted_df.to_csv('JOLTS.csv')

#Google sheets api removed from public upload
#sheets_api_key = ''

#Reads json file approving access to google drive, edit file location for sharing
gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')


spreadsheet1 = gc.open('JOLTS Data')
spreadsheet1.values_update('Sheet1',
                           params = {'valueInputOption' : 'USER_ENTERED'},
                           body = {'values' : list(csv.reader(open('JOLTS.csv')))}
                           )

#Share spreadsheet so that the main email has access, not just editing client/API
#Email hidden for public upload
spreadsheet1.share('EmailHidden', perm_type = 'user', role = 'writer')
