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


def generate_series_query(series_type, yearStart, yearEnd, monthStart, monthEnd, seriesIds):

    #format dates for SQL query
    if(series_type=="Level"):
        begDate = yearStart + "-" + monthStart + "-" + "01"
    elif(series_type=="Monthly Rate"):
        begDate = yearStart + "-" + str(int(monthStart)-1) + "-" + "01" #get data from a month previous to calculate m-o-m change
    elif(series_type=="Annual Rate"):
        begDate = str(int(yearStart)-1) + "-" + monthStart + "-" + "01" #get data from a year previous to calculate y-o-y change

    #end date    
    endDate = yearEnd + "-" + monthEnd + "-" + month_mapping[monthEnd]

    #join series list into string for query
    seriesOfInterest = ', '.join([f"'{series_id}'" for series_id in seriesIds])

    #construct query
    query = f"""
    SELECT * FROM inflation_index 
    where date >= '{begDate}' 
    and date <= '{endDate}'
    and series_id in ({seriesOfInterest})
    order by priority, date
    """

    return query


def generate_compare_query(yearStart, yearEnd, monthStart, monthEnd):

    begDate = yearStart + "-" + monthStart + "-" + "01"
    endDate = yearEnd + "-" + monthEnd + "-" + "01"

    query = f"""
    SELECT 
        A.series_id, 
        A.series_desc,
        A.level,
        A.priority,
        A.beg_index_val,
        B.end_index_val,
        C.beg_weight,
        D.parent_category_series_id,
        D.parent_category_series_desc
    FROM 
        (SELECT 
            date, 
            series_id, 
            series_desc, 
            level, 
            priority, 
            value AS beg_index_val
        FROM 
            inflation_index 
        WHERE 
            date = '{begDate}'
        ) AS A
    LEFT JOIN 
        (SELECT
            date,
            series_id,
            value AS end_index_val
        FROM 
            inflation_index
        WHERE 
            date = '{endDate}'
        ) AS B 
        ON A.series_id = B.series_id
    LEFT JOIN 
        (SELECT 
            date, 
            series_id,
            weight AS beg_weight
        FROM 
            weights_table
        WHERE 
            date = '{begDate}'
        ) AS C 
        ON A.series_id = C.series_id
    LEFT JOIN 
        (SELECT 
            series_id, 
            parent_category_series_id,
            parent_category_series_desc
        FROM 
            series_relation
        ) AS D 
        ON A.series_id = D.series_id
    """
    return query


def get_max_date(db_name):
    query = "SELECT max(date) as max_date FROM inflation_index "
    result_df = get_data(db_name, query)
    date_string = result_df['max_date'].iloc[0]
    return date_string



def get_data(db_name, query):

    # Connect to the SQLite database
    conn = sqlite3.connect(db_name)

    #fetch data
    return_df = pd.read_sql_query(query, conn)

    #close connection
    conn.close()

    return return_df

