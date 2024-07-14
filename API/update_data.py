import requests
import numpy as np
import pandas as pd
from fetch_data import get_max_date, get_all_series, get_base_weights_and_index_for_update, get_max_base_year
import datetime
import sqlite3

month_to_string = {
    1: "01",
    2: "02",
    3: "03",
    4: "04",
    5: "05",
    6: "06",
    7: "07",
    8: "08",
    9: "09",
    10: "10",
    11: "11",
    12: "12"
}


api_key = '12e590849a204e98801be46e6d77ca12'
url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

def create_table_from_dataframe(cursor, df, table_name):
    # columns = df.columns
    dtypes = df.dtypes
    sql_dtypes = []

    for column, dtype in dtypes.items():
        if dtype == 'int64':
            sql_dtypes.append(f'"{column}" INTEGER')
        elif dtype == 'float64':
            sql_dtypes.append(f'"{column}" REAL')
        elif dtype == 'object':
            sql_dtypes.append(f'"{column}" TEXT')
        elif dtype.name == 'datetime64[ns]':
            sql_dtypes.append(f'"{column}" TEXT')  # SQLite does not have a dedicated date type, so store as TEXT
        else:
            raise ValueError(f"Unhandled dtype: {dtype} for column {column}")

    col_defs = ", ".join(sql_dtypes)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs});"    
    cursor.execute(create_table_query)

def gen_api_payload(start_year, end_year, series):
    
    # Request payload
    payload = {
        "seriesid": series,
        "startyear": start_year,
        "endyear": end_year,
        "registrationkey": api_key
    }
    
    return payload

def get_dataframe(response):
    
    # Check the response status
    if response.status_code == 200:
        data = response.json()
        series_data = data['Results']['series']

        # Initialize a list to store the parsed data
        parsed_data = []

        # Parse the response
        for series in series_data:
            series_id = series['seriesID']
            for item in series['data']:
                year = item['year']
                period = item['period']
                value = item['value']
                # Convert period to month
                if period.startswith('M'):
                    month = period.replace('M', '')
                else:
                    month = 'NA'  # Handle other period types if necessary
                # Append to parsed data list
                parsed_data.append([year, month, series_id, value])

        # Create a pandas DataFrame
        df = pd.DataFrame(parsed_data, columns=['year', 'month', 'series_id', 'value'])

    else:
        print(f"Error: {response.status_code}")
        
    return df

def get_series_data(series_df, addDate):

    series_of_interest = np.array_split(np.array(series_df['series_id']), 2)
    tmp_df = pd.DataFrame(columns=['year', 'month', 'series_id','value'])

    for array in series_of_interest:
    
        series = list(array)
        yr_start = addDate.year
        yr_end = addDate.year

        while(True):

            payload = gen_api_payload(str(yr_start), str(yr_end), series)
            response = requests.post(url, json=payload)
            df = get_dataframe(response)
            tmp_df = pd.concat([tmp_df, df])
            
            break
    
    tmp_df = tmp_df.loc[(tmp_df['month']==month_to_string[addDate.month])].copy()

    return tmp_df

def get_inflation_df_append(addDate):

    series_relation_df = get_all_series('inflation_database.db')
    tmp_df = get_series_data(series_relation_df, addDate)
    tmp_df = pd.merge(tmp_df, series_relation_df, how='left', on=['series_id'])
    tmp_df['date'] = pd.to_datetime(tmp_df[['year', 'month']].assign(day=1)).dt.strftime('%Y-%m-%d')
    final_columns = ['year','month','series_id','series_desc','level','priority','value','date']
    inflation_index_df = tmp_df[final_columns]
    inflation_index_df['year'] = inflation_index_df['year'].astype(int)
    inflation_index_df['month'] = inflation_index_df['month'].astype(int)
    inflation_index_df['value'] = inflation_index_df['value'].astype(float)
    inflation_index_df['value'] = np.round(inflation_index_df['value'], 3)

    return inflation_index_df

def get_weights_df_append(addDate):

    addYear = int(addDate.year)
    addMonth = int(addDate.month)
    weights_helper_df = get_base_weights_and_index_for_update(db_name='inflation_database.db', addYear=addYear, addMonth=addMonth)
    weights_helper_df['weight'] = np.round(weights_helper_df['base_weight'] * (weights_helper_df['add_item_index'] / weights_helper_df['base_item_index']) / (weights_helper_df['add_overall_index'] / weights_helper_df['base_overall_index']), 3)
    weights_helper_df['date'] = pd.to_datetime(weights_helper_df[['year', 'month']].assign(day=1)).dt.strftime('%Y-%m-%d')
    final_columns = ['year','month','series_id','series_desc','level','priority','weight','date']
    weights_final_df_append = weights_helper_df[final_columns]

    return weights_final_df_append

def update_db(add_date_string):

    return_msg = {}
    conn = sqlite3.connect('inflation_database.db')
    cursor = conn.cursor()

    max_date_weights_string = get_max_date('inflation_database.db', 'weights_table')
    max_date_index_string = get_max_date('inflation_database.db','inflation_index')
    max_base_year = get_max_base_year('inflation_database.db')
    max_date = datetime.datetime.strptime(max_date_weights_string, "%Y-%m-%d")
    add_date = datetime.datetime.strptime(add_date_string, "%Y-%m-%d")
    max_date_inflation_table = datetime.datetime.strptime(max_date_index_string, "%Y-%m-%d")

    if(max_date >= add_date):
        return_msg['all_tables'] = "This month or a more recent month already exists in weights_table. Max date was " + max_date_weights_string
        return return_msg
        
    if(max_date_inflation_table >= add_date):
        return_msg['inflation_index'] = "Data for this month already in inflation_index table. Skipping that append..."
    else:
        inflation_index_df_append = get_inflation_df_append(addDate=add_date)
        create_table_from_dataframe(cursor, inflation_index_df_append, 'inflation_index')
        inflation_index_df_append.to_sql('inflation_index', conn, if_exists='append', index=False)
        return_msg['inflation_index'] = "inflation_index has been updated"

    if((int(add_date.year)-1 > max_base_year)):
        return_msg['weights_table'] = "Max base year is: " + str(max_base_year) + ". Cannot update table until base_weights table has been updated for " + str(int(add_date.year)-1)
        return return_msg
    else:
        weights_df_append = get_weights_df_append(addDate=add_date)
        create_table_from_dataframe(cursor, weights_df_append, 'weights_table')
        weights_df_append.to_sql('weights_table', conn, if_exists='append', index=False)
        return_msg['weights_table'] = "weights_table has been updated"

    return return_msg
    

