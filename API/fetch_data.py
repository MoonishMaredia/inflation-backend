import sqlite3
import pandas as pd

month_mapping = {
    "01": "31",
    "02": "28",
    "03": "31",
    "04": "30",
    "05": "31",
    "06": "30",
    "07": "31",
    "08": "31",
    "09": "30",
    "10": "31",
    "11": "30",
    "12": "31"
}


def generate_series_query(yearStart, yearEnd, monthStart, monthEnd, seriesIds):

    begDate = yearStart + "-" + monthStart + "-" + "01"
    endDate = yearEnd + "-" + monthEnd + "-" + month_mapping[monthEnd]
    seriesOfInterest = seriesIds.join(", ")

    query = f"SELECT FROM inflation_index where /
        date >= {begDate} and date <= {endDate} /
        and series_id in ({seriesOfInterest})"

    
    return query



def get_series_data(db_name, table_name):

    return None

