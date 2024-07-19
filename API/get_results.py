from fetch_data import generate_series_query, generate_compare_query, get_data
from prepare_results import prepare_series_data, prepare_compare_data
import logging

logger = logging.getLogger(__name__)

async def get_final_data(db, request_type, input_args):
    try:
        if request_type == "time-series":
            logger.info(f"Generating time series data for: {input_args}")
            query = generate_series_query(input_args.seriesType, input_args.yearStart, input_args.yearEnd, 
                                          input_args.monthStart, input_args.monthEnd, input_args.seriesIds)
            raw_data_df = await get_data(db, query)
            results = prepare_series_data(input_args.seriesType, raw_data_df)
        
        elif request_type == "compare":
            logger.info(f"Generating compare data for: {input_args}")
            query = generate_compare_query(input_args.yearStart, input_args.yearEnd, 
                                          input_args.monthStart, input_args.monthEnd)
            raw_data_df = await get_data(db, query)
            results = prepare_compare_data(raw_data_df)
        else:
            raise ValueError(f"Invalid request type: {request_type}")

        return results
    
    except Exception as e:
        logger.error(f"Error in get_final_data: {str(e)}")
        raise