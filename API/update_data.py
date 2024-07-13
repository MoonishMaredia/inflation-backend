import requests
import numpy as np
import pandas as pd
import re
from fetch_data import get_max_date_weights, get_all_series
import datetime

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
    print("Here are the series of interest in array form", series_of_interest)
    tmp_df = pd.DataFrame(columns=['year', 'month', 'series_id','value'])

    for array in series_of_interest:
    
        series = list(array)
        yr_start = addDate.year
        yr_end = addDate.year

    while(True):

        payload = gen_api_payload(str(yr_start), str(yr_end), series)
        print("Here's a payload to the api:", payload)
        response = requests.post(url, json=payload)
        df = get_dataframe(response)
        tmp_df = pd.concat([tmp_df, df])
        
        break
    
    print("Here's the tmp df for the full year:", tmp_df)
    print("Will filter on the following:", addDate.month, month_to_string[addDate.month])
    tmp_df = tmp_df.loc[(tmp_df.loc['month']==month_to_string[addDate.month])].copy()
    print("Here's the tmp df generated via api after filering to our month", tmp_df)

    return tmp_df

def get_inflation_df_append(addDate):

    print("Pulling series table")
    series_relation_df = get_all_series('inflation_database.db')
    print("Here's the series table", series_relation_df.head())
    print("Getting series data")
    tmp_df = get_series_data(series_relation_df, addDate)
    print("Joining necessary data onto tmp_df")
    tmp_df = pd.merge(tmp_df, series_relation_df, how='left', on=['series_id'])
    print("Here's the df post join", tmp_df.head(), tmp_df.dtypes)
    tmp_df['date'] = pd.to_datetime(tmp_df[['year', 'month']].assign(day=1)).dt.strftime('%Y-%m-%d')
    final_columns = ['year','month','series_id','series_desc','level','priority','value','date']
    inflation_index_df = tmp_df[final_columns]
    print("Here's the final df to append after re-ordering and adding all cols", inflation_index_df.head())

    return inflation_index_df


def update_db(add_date_string, weights_table_updated=False):

    max_date_string = get_max_date_weights('inflation_database.db')
    print(max_date_string)
    max_date = datetime.datetime.strptime(max_date_string, "%Y-%m-%d")
    print(max_date)
    add_date = datetime.datetime.strptime(add_date_string, "%Y-%m-%d")
    print(add_date)

    if(max_date.month <= add_date.month):
        return "This month or a more recent month already exists in database. Max data was {max_date}"
    
    if((max_date.year > add_date.year) and (weights_table_updated!=True)):
        return "We cannot update all tables until weights table has been updated"
    
    print("I am here")
    inflation_index_df_append = get_inflation_df_append(addDate=add_date)
    

