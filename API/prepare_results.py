import numpy as np


def prepare_series_data(series_type, raw_df):

    #ensure the df is sorted by series and date
    raw_df = raw_df.sort_values(by=['priority','date'])
    result = {}
    #isolate just the x-axis
    x_axis = list(np.unique(np.array(raw_df['date'])))
    data = {}
    for series_id, group in raw_df.groupby('series_id'):
        data[series_id] = {
            'value': group['value'].tolist(),
            'series_desc': group['series_desc'].iloc[0]
        }
    #Case 1: user wants overall index level
    if(series_type=="Level"):
        result["x-axis"] = x_axis
        result["data"] = data

    #Case 2: monthly rate
    elif(series_type=="Monthly Rate"):
        for key in data.keys():
            data[key]['value'] = [round(((data[key]['value'][i] / data[key]['value'][i-1])-1)*100.0,2) for i in range(1, len(data[key]['value']))]
        result["x-axis"] = x_axis[1:].copy()
        result["data"] = data
    
    #Case 3: annual rate
    elif(series_type=="Annual Rate"):
        for key in data.keys():
            data[key]['value'] = [round(((data[key]['value'][i] / data[key]['value'][i-12])-1)*100.0, 2) for i in range(12, len(data[key]['value']))]
        result["x-axis"] = x_axis[12:].copy()
        result["data"] = data 

    return result


def prepare_compare_data(raw_df):

    raw_df['scalar'] = raw_df['end_index_val'] / raw_df['beg_index_val']
    raw_df['end_weight'] = raw_df['beg_weight'] * raw_df['scalar'] 
    raw_df['contributing_difference'] = raw_df['end_weight'] - raw_df['beg_weight']

    print(raw_df.head())

    return None