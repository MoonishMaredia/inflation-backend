import pandas as pd
import sqlite3
import os

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

if __name__=="__main__":

    # initialize db
    index_df = pd.read_csv('./datafiles/index_table_final.csv', index_col=False, usecols=['year', 'month', 'series_id', 'series_desc', 'level', 'order', 'value'])
    index_df.rename(columns={"order":"priority"}, inplace=True)
    index_df['date'] = pd.to_datetime(index_df[['year', 'month']].assign(day=1))

    #initialize connection
    conn = sqlite3.connect('inflation_database.db')
    cursor = conn.cursor()
    
    #execute create table query 
    create_table_from_dataframe(cursor, index_df, 'inflation_index')

    #insert data into query
    index_df.to_sql('inflation_index', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()

