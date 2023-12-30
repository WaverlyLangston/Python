#Download business formation statistics from census, clean and maniuplate for use in a tableau visualization, export and update a google sheet

import pandas as pd
import plotly.io as pio
pio.renderers.default='browser'
import Census_Population_Estimates_Time_Series as pop
import gspread
import csv


#import census bfs file
data = pd.read_csv('https://www.census.gov/econ/bfs/csv/bfs_monthly.csv')

#month dictionary for times for tableau
month_dict = { 'jan':'01',
              'feb' : '02',
              'mar' : '03',
              'apr' : '04',
              'may' : '05',
              'jun' : '06',
              'jul' : '07',
              'aug' : '08',
              'sep' : '09',
              'oct' : '10',
              'nov' : '11',
              'dec':'12'}

#Filter and format for CT and US data for business applications and high propensity applications
ct_us = ['CT', 'US']
biz_type = ['BA_BA', 'BA_HBA']
tableau_data = data[(data['sa'] == 'A') & (data['geo'].isin(ct_us)) & data['series'].isin(biz_type) & (data['naics_sector'] == 'TOTAL')].drop(columns=['sa','naics_sector'])
tableau_data['datapoint'] = tableau_data['series'] +'_'+ tableau_data['geo']
tableau_data = tableau_data.drop(columns=['series', 'geo'])
tableau_data = tableau_data.melt(id_vars=['datapoint', 'year'])
tableau_data['time'] = tableau_data['variable'].map(month_dict)
tableau_data['time'] = tableau_data['time'].astype(str) + '/' + tableau_data['year'].astype(str)
tableau_data['time'] = tableau_data['time'].str.strip()
tableau_data['time'] = pd.to_datetime(tableau_data['time'])
tableau_data = tableau_data.dropna()
tableau_data = tableau_data.pivot(index=['time', 'variable', 'year'],columns='datapoint', values='value').astype(float)
tableau_data = tableau_data.rename(columns = {'variable':'month', 'BA_BA_CT':'CT Business Applications', 'BA_BA_US':'U.S. Business Applications', 'BA_HBA_CT':'CT High Propensity Business Applications', 'BA_HBA_US':'U.S. High Propensity Business Applications'})
tableau_data = tableau_data.reset_index()
tableau_data['year']=tableau_data['year'].astype(str)

#import and reshape population data, saved local file
population = pop.population()
population.set_index('Geography', inplace = True)
population = population.transpose()
population = population.reset_index()
population = population.rename(columns = {'index':'year'})

#add population data for Connecticut and United States to tableau data file
tableau_data = pd.merge(tableau_data, population[['year', 'Connecticut', 'United States']], on='year', how='left')
tableau_data = tableau_data.set_index('time')
tableau_data = tableau_data.rename(columns = {'Connecticut' : 'CT Population', 'United States' : 'U.S. Population'})
tableau_data['CT Population'] = tableau_data['CT Population'].astype(float)
tableau_data['U.S. Population'] = tableau_data['U.S. Population'].astype(float)

#calculate per capita values
tableau_data['CT Business Applications Per Capita'] = tableau_data['CT Business Applications']/tableau_data['CT Population']
tableau_data['U.S. Business Applications Per Capita'] = tableau_data['U.S. Business Applications']/tableau_data['U.S. Population']
tableau_data['CT High Propensity Business Applications Per Capita'] = tableau_data['CT High Propensity Business Applications']/tableau_data['CT Population']
tableau_data['U.S. High Propensity Business Applications Per Capita'] = tableau_data['U.S. High Propensity Business Applications']/tableau_data['U.S. Population']

#calculate 12-month moving averages for per capita values
tableau_data['CT Business Applications Per Capita 12-Month Moving Average'] = tableau_data['CT Business Applications Per Capita'].rolling(12).mean()
tableau_data['U.S. Business Applications Per Capita 12-Month Moving Average'] = tableau_data['U.S. Business Applications Per Capita'].rolling(12).mean()
tableau_data['CT High Propensity Business Applications Per Capita 12-Month Moving Average'] = tableau_data['CT High Propensity Business Applications Per Capita'].rolling(12).mean()
tableau_data['U.S. High Propensity Business Applications Per Capita 12-Month Moving Average'] = tableau_data['U.S. High Propensity Business Applications Per Capita'].rolling(12).mean()
                                                                                          
tableau_data.to_csv('business_formation_statistics.csv')


# #this is the old way of updating google drive, sheet name changes#
# #Google sheets api removed for public upload
# #sheets_api_key = ''
# #Reads json file approving access to google drive, edit file location for sharing
# gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

# #Updates spreadsheeet with csv that was exported.  Easier to update like this and we want the csv anyway.
# spreadsheet1 = gc.open('Business Formation Statistics')
# with open('business_formation_statistics.csv', 'r', encoding = 'UTF-8') as file_obj:
#     content = file_obj.read()
#     gc.import_csv(spreadsheet1.id, data=content)

# #Share spreadsheet so that the main email has access, not just editing client/API
# #Email hidden for public upload
# spreadsheet1.share('EmailHidden', perm_type = 'user', role = 'writer')


##THIS IS A METHOD OF UPDATING THE A TAB IN THE SPREADSHEET CALLED SHEET1##
##TABLEAU REJECTS DATA CONNECTIONS EASILY AND DOES NOT LIKE TO REPLACE##
##IF A SHEET NAME OTHER THAN LAUS BECOMES NECESSARY THIS ALLOWS FOR THAT##

#Google sheets api removed for public upload
#sheets_api_key = ''

#Reads json file approving access to google drive, edit file location for sharing
gc = gspread.service_account(filename='dashboard_data_google_drive_access_info.json')

spreadsheet1 = gc.open('Business Formation Statistics')
spreadsheet1.values_update('Sheet1',
                            params = {'valueInputOption' : 'USER_ENTERED'},
                            body = {'values' : list(csv.reader(open('business_formation_statistics.csv')))}
                            )

#Share spreadsheet so that the main email has access, not just editing client/API
#Email hidden for public upload.
spreadsheet1.share('EmailHidden', perm_type = 'user', role = 'writer')

