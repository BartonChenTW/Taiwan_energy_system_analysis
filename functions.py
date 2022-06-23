######################################################
# this script convert json data from DATA.GOV.TW
# for electricity generation data of Taiwan
# link: https://data.gov.tw/dataset/37331
# the origin file is in json format with 10 min interval
# for each indivitual generator.
# The scrip will convert to 1hr interval and group with
# type of generation, e.g. nuclear, gas, coal...



def json2csv(file_name='', file_folder='Taiwan electricity data\\'):
    '''read json file (from Taipower, 10min interval) to csv files with 10min interval and 1hr interavl.
    '''
    import json

    ## read json file
    
    file_path = file_folder + file_name

    with open( f'{file_path}.json', encoding='utf-8-sig') as json_file:
        di_all = json.load(json_file)

    # format of 'di_all'
    # data.keys() = dict_keys(['records'])
    # data['records'].keys() = dict_keys(['CATALOG', 'START_DATE', 'END_DATE', 'UNIT_OF_MEASUREMENT', 'INTERVAL', 'NET_P'])
    # time-series data stored in 'NET_P'
    # interval = 10 min


    ## convert NET_P into csv

    di_NET_P = di_all['records']['NET_P']
    # type = list of dictionary
    # format of 'di_NET_P': FUEL_TYPE, UNIT_NAME, DATE, NET_P


    ## convert csv to DataFrame

    import pandas as pd
    df = pd.DataFrame()

    for di in di_NET_P:
        index = di['DATE']
        column_name = di['FUEL_TYPE'] + '-' + di['UNIT_NAME']
        df.at[index, column_name] = di['NET_P'] 

    df.index = pd.to_datetime(df.index)     # change index format to 'datetime' (required for 'groupby' by time)


    ## set DataFrame and save to csv

    # set index (time) to a seperate column
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'time'})


    ## only read data for sepcific months
    # df = df[ df['time'].dt.month == 4 ]   

    # export csv with 10min interval
    print( f'save data in csv (with original interval) at "{file_path}.csv"' )

    df.to_csv(file_path + '.csv', encoding='utf-8-sig', index=False)


    ### convert the data to interval 1 hr

    # convert data (except 'time) 
    df.iloc[:, 1:] = df.iloc[:,1:].astype(float)

    df_hourly = df.groupby( pd.Grouper(key='time', freq='1h')).mean().round(1)
    df_hourly.reset_index(inplace=True)
    df_hourly = df_hourly.rename(columns = {'index':'time'})

    # export csv with 1hr interval
    print( f'save data in csv (1-hr interval) at "{file_path}_h.csv"' )
    df_hourly.to_csv(file_path + '_h.csv', encoding='utf-8-sig', index=False)




def merge_csv(list_filename='', nan2zero=True):
    '''to merge multipoe csv files into a single csv file'''

    import pandas as pd

    df = pd.DataFrame()
    if list_filename == '':
        list_filename = ['TWdata_2020OCT_3m_h', 'TWdata_2021JAN_3m_h', 'TWdata_2021APR_1m_h', 'TWdata_2021MAY_3m_h', 'TWdata_2021AUG_3m_h']

    for filename in list_filename:
        df2 = pd.read_csv('Taiwan electricity data\\' + filename + '.csv')
        df = pd.concat([df, df2])

    if nan2zero:
        df = df.fillna(0)   # change 'NAN' into '0'

    df.reset_index(inplace=True, drop=True)
    df.to_csv('Taiwan electricity data\\' + 'merged_file' + '.csv', encoding='utf-8-sig', index=False)



def merge_column(df_input, seperator='#', export_file=True):
    ''' combine (sum up) columns based on the column name with seperator.
    filename: without '.csv'

    For example:
        for seperator '#', it merages the columns likes: '核能-核三#1', '核能-核三#2' into '核能-核三'
        for seperator '-', it merages the columns likes: '太陽能-民雄', '太陽能-永安鹽灘' into '太陽能'
    '''

    import pandas as pd

    df = df_input

    ## creat a new dataframe, copy column 'time'
    df2 = pd.DataFrame()
    df2['time'] = df['time']

    columns = list(df.columns)
    columns.remove('time')

    list_category = []

    for col in columns:
        
        # read the type of generator; column name format: 'type'-'name of generator'; for example: '核能-核三#1'
        category = col.split(seperator)[0]
        # print(f'type of generator: {category}')

        if category in list_category:  continue
        else:  list_category.append(category)

        # sum up the column with the same type
        df2[category] = 0
        for col_check in columns:
            category_check = col_check.split(seperator)[0]

            ## skip if category doesn't match
            if category_check != category: continue

            df2[category] += df[col_check]


    if export_file:
        df2.reset_index(inplace=True, drop=True)
        export_file_name = 'Taiwan electricity data\\' + 'merged_column' + '.csv'
        df2.to_csv(export_file_name, encoding='utf-8-sig', index=False)
        print(f'data saved at "{export_file_name}"')

    return df2 


def get_statistic(df):
    import pandas as pd

    df_summary = pd.DataFrame()

    if '總發電量' in df.columns: sum_gen = df['總發電量'].sum()/1000/1000

    for col in df.columns:
        if col == '時間': continue
        
        df_summary.at['最小值 (MW)',col] = df[col].min()
        df_summary.at['最大值 (MW)',col] = df[col].max()
        df_summary.at['平均值 (MW)',col] = df[col].mean()
        df_summary.at['年發電量 (TWh)',col] = df[col].sum()/1000/1000
        if '總發電量' in df.columns: df_summary.at['發電佔比 (%)',col] = df_summary.at['年發電量 (TWh)',col] / sum_gen * 100

    return df_summary




def get_solar_data(filename, label, target_CF=0, folder = 'Taiwan RE data\\' ):
    '''a function to get solar data from RE'''

    import pandas as pd

    # read the raw data from Renealbe Ninja
    df_solar = pd.read_csv( folder + filename + '.csv',skiprows=[0,1,2])

    # remove not used column
    df_solar = df_solar.drop(['time', 'irradiance_direct', 'irradiance_diffuse'],axis=1)

    # change name
    df_solar = df_solar.rename(columns = {"local_time":"時間", "electricity":f"太陽能-{label}(RE ninja)", "temperature":f"溫度-{label}"})

    # set capacity factor target
    if target_CF > 0:
        df_solar[f'太陽能-{label}'] = df_solar[f'太陽能-{label}(RE ninja)'] / df_solar[f'太陽能-{label}(RE ninja)'].mean() * target_CF


    ### move last 8 hours to the beginning due to 'local time' difference

    # set datetime
    df_solar['時間'] = pd.to_datetime(df_solar['時間'])

    # change data in 2021 into 2020
    df_solar['時間'].loc[ df_solar['時間'] >= '2021'] = df_solar['時間'].loc[ df_solar['時間'] >= '2021'].apply(lambda x: x.replace(year=2020))

    # sort the data by new time
    df_solar = df_solar.sort_values(by='時間').reset_index(drop=True)

    return df_solar



def REninja_pv(re_type:str, lat:float, lon:float, year=0, date_from:str='', date_to:str='', raw=False, local_time=True,
               dataset='merra2', capacity=1, system_loss=0.1, 
               tracking=0, tilt=35, azim=180, format='json', 
               height=120, turbine='Vestas V164 9500'):
    '''to get solar PV data from RE ninja. 
    
    Parameter
    -----
    re_type: either 'pv' or 'wind'
    date_from: e.g. '2020-12-31'
    date_to: e.g. '2021-12-31'    
    year: auto download the data for that year; download the data with 'local time' if 'local_time' = True
    '''

    if re_type not in ['pv', 'wind']: 
        print('re_type not regonised. stop funciton.')
        return

    print(f'Loading data from RE ninja; latitude: {lat}, longitude: {lon}, year: {year}')

    import pandas as pd
    import requests
    import json
    
    token = 'e8164c92aebc65d0bb991b63e6bc74fdc77e7690'
    api_base = 'https://www.renewables.ninja/api/'

    s = requests.session()
    # Send token header with each request
    s.headers = {'Authorization': 'Token ' + token}



    if year >0:
        if local_time:
            date_from = f'{year-1}-12-31'
            date_to = f'{year}-12-31'
        else:
            date_from = f'{year}-01-01'
            date_to = f'{year}-12-31'           

    if re_type == 'pv':
        url = api_base + 'data/pv'

        args = {
        'lat': lat,
        'lon': lon,
        'date_from': date_from,
        'date_to': date_to,
        'raw':raw,
        'local_time': local_time,
        'dataset': dataset,
        'capacity': capacity,
        'system_loss': system_loss,
        'tracking': tracking,
        'tilt': tilt,
        'azim': azim,
        'format': format
        }
    elif re_type == 'wind':
        url = api_base + 'data/wind'

        args = {
            'lat': lat,
            'lon': lon,
            'date_from': date_from,
            'date_to': date_to,
            'raw':raw,
            'local_time': local_time,            
            'capacity': capacity,
            'height': height,
            'turbine': turbine,
            'format': 'json'
        }

    r = s.get(url, params=args)

    # Parse JSON to get a pandas.DataFrame of data and dict of metadata
    parsed_response = json.loads(r.text)

    data = pd.read_json(json.dumps(parsed_response['data']), orient='index')
    metadata = parsed_response['metadata']

    ## filter year data with local time
    if local_time:
        data = data.loc[ data['local_time'].dt.year == year ]
        data['local_time'] = data['local_time'].dt.tz_localize(None)

    data.reset_index(drop=True, inplace=True)
    
    return data