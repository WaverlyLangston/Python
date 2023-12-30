# Import several years of population data from census pop estimates for use in per capita calculations of other scripts

def population():
    import pandas as pd
    import gc
        
    #Import 2000 - 2010 Population Estimate Data
    data00s = pd.read_csv('https://www2.census.gov/programs-surveys/popest/tables/2000-2010/intercensal/state/st-est00int-01.csv',header=[3],)
    data00s.rename(columns={'Unnamed: 0':'Geography', 'Unnamed: 1':'April 2000', 'Unnamed: 12':'April 2010','Unnamed: 13':'2010'}, inplace = True)
    data00s['Geography'] = data00s['Geography'].apply(lambda x : str(x).replace('.', ''))
    column_names = data00s.columns
    for i in column_names:
        data00s[i] = data00s[i].apply(lambda x: str(x).replace(',',''))
    #data00s = data00s.drop([1,2,3,4,56,58,59,60,61,62,63,64,65])
    
    #Import 2010 - 2019 Population Estimate Data
    data10s = pd.read_excel("https://www2.census.gov/programs-surveys/popest/tables/2010-2020/state/totals/nst-est2020.xlsx", header=[3])
    data10s.rename(columns={'Unnamed: 0':'Geography', 'Census':'2010 Census', 'Estimates Base': '2010 Estimates Base', 'April 1': 'April 2020 Pop Est', 'July 1':'July 2020 Pop Est'}, inplace = True)
    data10s['Geography'] = data10s['Geography'].apply(lambda x : str(x).replace('.', ''))
    #data10s = data10s.drop([1,2,3,4,56,58,59,60,61,62,63])
    
    #Import 2020 - 2022 Population Estimate Data
    data20s = pd.read_excel('https://www2.census.gov/programs-surveys/popest/tables/2020-2022/state/totals/NST-EST2022-POP.xlsx', header=[3])
    data20s.rename(columns={'Unnamed: 0':'Geography', 'Unnamed: 1': '2020 Estimates Base'}, inplace = True)
    data20s['Geography'] = data20s['Geography'].apply(lambda x : str(x).replace('.', ''))
    #data20s = data20s.drop([1,2,3,4,56,58,59,60,61,62,63])
    
    #merge into one population dataframe
    population_data = data00s.drop(['April 2000', 'April 2010', '2010'], axis=1)
    population_data = population_data.drop([56,58,59,60,61,62,63,64,65])
    population_data = population_data.merge(data10s, on='Geography')
    population_data = population_data.drop(['2010 Census', '2010 Estimates Base', 'April 2020 Pop Est', 'July 2020 Pop Est'], axis=1)
    population_data = population_data.merge(data20s, on='Geography')
    population_data = population_data.drop(['2020 Estimates Base'], axis=1)
    
    # # #add 2023 data by duplicating 2022 values
    population_data.columns = population_data.columns.map(str)
    population_data['2023'] = population_data['2022']
     
    #delete old and clean
    del data00s
    del data10s
    del data20s
    gc.collect()

    return population_data
