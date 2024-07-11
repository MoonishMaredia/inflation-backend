from fetch_data import generate_series_query, generate_compare_query, get_data
from prepare_results import prepare_series_data, prepare_compare_data


def get_final_data(request_type, input_args):

    if(request_type=="time-series"):
        print("Arguments:", input_args)
        query = generate_series_query(input_args.seriesType, input_args.yearStart, input_args.yearEnd, 
                                      input_args.monthStart, input_args.monthEnd, input_args.seriesIds)
        raw_data_df = get_data(db_name="inflation_database.db", query=query)
        results = prepare_series_data(input_args.seriesType, raw_data_df)
        
    if(request_type=="compare"):
        query = generate_compare_query(input_args.yearStart, input_args.yearEnd, 
                                      input_args.monthStart, input_args.monthEnd)
        raw_data_df = get_data(db_name="inflation_database.db", query=query)
        results = prepare_compare_data(raw_data_df)

    return results